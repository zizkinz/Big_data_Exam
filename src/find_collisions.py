from pathlib import Path
import shutil

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from filter import maritime_distance_expr

COLLISION_DISTANCE_M = 150.0
COLLISION_DISTANCE_NM = COLLISION_DISTANCE_M / 1852.0
TIME_WINDOW_SEC = 10 * 60
DEFAULT_COLLISIONS_DIR = "/app/data/collisions"
LAT_TOLERANCE = 0.01
LON_TOLERANCE = 0.01


def load_filtered_day(spark: SparkSession, file_path: str) -> DataFrame:
    return (
        spark.read.option("header", True).csv(file_path)
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("sog", F.col("sog").cast("double"))
        .withColumn("distance_to_center_nm", F.col("distance_to_center_nm").cast("double"))
    )


def prepare_candidate_points(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("timestamp").isNotNull())
          .filter(F.col("mmsi").isNotNull())
          .filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
          .select("timestamp", "mmsi", "name", "latitude", "longitude", "sog", "navigational_status")
          .dropDuplicates(["timestamp", "mmsi", "latitude", "longitude"])
    )


from pyspark.sql.window import Window
from pyspark.sql import functions as F

def find_collision_candidates(df: DataFrame) -> DataFrame:
    base_points = prepare_candidate_points(df)

    # 1. Create the sliding 20-minute windows (sliding every 10 minutes)
    windows_df = (
        base_points
        .withColumn("epoch_10m", F.floor(F.col("timestamp").cast("long") / TIME_WINDOW_SEC))
        .withColumn("window_id", F.explode(F.array(F.col("epoch_10m"), F.col("epoch_10m") - 1)))
    )

    # Side A keeps original names
    df_a = windows_df

    # Side B gets explicitly prefixed names to prevent PySpark join ambiguity
    df_b = windows_df.select(
        F.col("window_id").alias("b_window_id"),
        F.col("mmsi").alias("b_mmsi"),
        F.col("name").alias("b_name"),
        F.col("timestamp").alias("b_timestamp"),
        F.col("latitude").alias("b_latitude"),
        F.col("longitude").alias("b_longitude"),
        F.col("sog").alias("b_sog"),
        F.col("navigational_status").alias("b_status")
    )

    # 2. Optimized Equi-Join using unique column names
    joined = df_a.join(
        df_b,
        on=(
                (F.col("window_id") == F.col("b_window_id")) &
                (F.col("mmsi") < F.col("b_mmsi")) &
                # The Magic: Drop ships that are far apart before the join finishes
                (F.abs(F.col("latitude") - F.col("b_latitude")) <= LAT_TOLERANCE) &
                (F.abs(F.col("longitude") - F.col("b_longitude")) <= LON_TOLERANCE)
        ),
        how="inner",
    )

    # 3. Deduplicate: This now works perfectly because all string columns are unique
    unique_pairs = joined.dropDuplicates([
        "mmsi", "b_mmsi", "timestamp", "b_timestamp"
    ])

    # 4. Strict time filter and distance calculation
    with_distance = (
        unique_pairs.withColumn(
            "time_diff_sec",
            F.abs(F.col("timestamp").cast("long") - F.col("b_timestamp").cast("long"))
        )
        .filter(F.col("time_diff_sec") <= TIME_WINDOW_SEC) # Ensure strictly <= 10 mins
        .withColumn(
            "distance_nm",
            maritime_distance_expr(
                F.col("latitude"),
                F.col("longitude"),
                F.col("b_latitude"),
                F.col("b_longitude"),
            ),
        )
        .withColumn("distance_m", F.col("distance_nm") * F.lit(1852.0))
    )

    # 5. Filter by collision distance
    candidates = with_distance.filter(F.col("distance_nm") <= F.lit(COLLISION_DISTANCE_NM))

    # 6. Keep only the absolute closest point per vessel pair
    pair_window = Window.partitionBy(
        F.col("mmsi"), F.col("b_mmsi")
    ).orderBy(
        F.col("distance_m").asc(),
        F.col("time_diff_sec").asc(),
        F.col("timestamp").asc()
    )

    return (
        candidates.withColumn("pair_rank", F.row_number().over(pair_window))
        .filter(F.col("pair_rank") == 1)
        .select(
            F.col("mmsi").alias("mmsi_1"),
            F.col("b_mmsi").alias("mmsi_2"),
            F.col("name").alias("name_1"),
            F.col("b_name").alias("name_2"),
            F.col("timestamp").alias("timestamp_1"),
            F.col("b_timestamp").alias("timestamp_2"),
            F.col("time_diff_sec"),
            F.round(F.col("distance_m"), 3).alias("distance_m"),
            F.col("latitude").alias("latitude_1"),
            F.col("longitude").alias("longitude_1"),
            F.col("b_latitude").alias("latitude_2"),
            F.col("b_longitude").alias("longitude_2"),
            F.col("sog").alias("sog_1"),
            F.col("b_sog").alias("sog_2"),
            F.col("navigational_status").alias("status_1"),
            F.col("b_status").alias("status_2"),
        )
        .orderBy("timestamp_1", "mmsi_1", "mmsi_2")
    )


def write_single_csv(df: DataFrame, final_csv_path: str) -> None:
    final_path = Path(final_csv_path)
    final_path.parent.mkdir(parents=True, exist_ok=True)

    temp_dir = final_path.with_suffix(".tmpdir")
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    if final_path.exists():
        final_path.unlink()

    (
        df.coalesce(1)
          .write
          .mode("overwrite")
          .option("header", True)
          .csv(str(temp_dir))
    )

    part_files = list(temp_dir.glob("part-*.csv"))
    if not part_files:
        raise FileNotFoundError(f"No Spark CSV part file found in temporary directory: {temp_dir}")

    shutil.move(str(part_files[0]), str(final_path))
    shutil.rmtree(temp_dir)


def process_one_filtered_file(
    spark: SparkSession,
    input_file: str,
    output_dir: str = DEFAULT_COLLISIONS_DIR
) -> str:
    df = load_filtered_day(spark, input_file)
    collisions = find_collision_candidates(df)
    output_path = Path(output_dir) / Path(input_file).name
    write_single_csv(collisions, str(output_path))
    return str(output_path)