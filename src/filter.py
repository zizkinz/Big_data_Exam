import math
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CENTER_LAT = 55.225000
CENTER_LON = 14.245000
RADIUS_NM = 50.0

MOVING_SOG_MIN = 2
MOVING_STEP_NM_MIN = 0.02
MAX_SOG_KNOTS = 40.0
MAX_STEP_NM = 5.0

SEQUENCE_INVALID = {
    "123456789", "987654321", "111111111", "222222222", "333333333",
    "444444444", "555555555", "666666666", "777777777", "888888888", "999999999"
}


def create_spark(app_name: str = "ais-filter", n_cores: int = 4) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(f"local[{n_cores}]")
        .config("spark.sql.shuffle.partitions", str(max(200, n_cores * 4)))
        .config("spark.default.parallelism", str(max(8, n_cores * 4)))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def maritime_distance_expr(lat1_col, lon1_col, lat2_col, lon2_col):
    radius_nm = 3440.065

    lat1 = F.radians(lat1_col)
    lon1 = F.radians(lon1_col)
    lat2 = F.radians(lat2_col)
    lon2 = F.radians(lon2_col)

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = (
        F.pow(F.sin(dlat / 2), 2) +
        F.cos(lat1) * F.cos(lat2) * F.pow(F.sin(dlon / 2), 2)
    )
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))

    return radius_nm * c


def clean_columns(df: DataFrame) -> DataFrame:
    rename_map = {
        "# Timestamp": "timestamp",
        "Type of mobile": "type_of_mobile",
        "MMSI": "mmsi",
        "Latitude": "latitude",
        "Longitude": "longitude",
        "Navigational status": "navigational_status",
        "ROT": "rot",
        "SOG": "sog",
        "COG": "cog",
        "Heading": "heading",
        "IMO": "imo",
        "Callsign": "callsign",
        "Name": "name",
        "Ship type": "ship_type",
        "Cargo type": "cargo_type",
        "Width": "width",
        "Length": "length",
        "Type of position fixing device": "position_fixing_device",
        "Draught": "draught",
        "Destination": "destination",
        "ETA": "eta",
        "Data source type": "data_source_type",
    }

    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    return df


def read_and_standardize_csv(spark: SparkSession, file_path: str) -> DataFrame:
    df = spark.read.option("header", True).csv(file_path)
    df = clean_columns(df)

    return (
        df.withColumn("timestamp", F.to_timestamp("timestamp", "dd/MM/yyyy HH:mm:ss"))
          .withColumn("latitude", F.col("latitude").cast("double"))
          .withColumn("longitude", F.col("longitude").cast("double"))
          .withColumn("sog", F.col("sog").cast("double"))
          .withColumn("cog", F.col("cog").cast("double"))
          .withColumn("heading", F.col("heading").cast("double"))
          .withColumn("rot", F.col("rot").cast("double"))
          .withColumn("mmsi", F.trim(F.col("mmsi")))
          .withColumn("name", F.trim(F.col("name")))
          .withColumn("navigational_status", F.trim(F.col("navigational_status")))
    )


def filter_invalid_rows(df: DataFrame) -> DataFrame:

    rescue_pattern = r"(?i)(rescue|sar|pilot|coast guard|guard|lifeboat|redning|raddning|pobl)"

    return df.filter(
        F.col("timestamp").isNotNull() &
        F.col("latitude").isNotNull() &
        F.col("longitude").isNotNull() &
        (F.col("latitude") != 0.0) &
        (F.col("longitude") != 0.0) &
        (F.abs(F.col("latitude")) <= 90.0) &
        (F.abs(F.col("longitude")) <= 180.0) &
        F.col("mmsi").rlike(r"^\d{9}$") &
        (~F.col("mmsi").isin(*SEQUENCE_INVALID)) &
        (F.col("name").isNull() | ~F.col("name").rlike(rescue_pattern))
    )


def filter_geographic_area(
    df: DataFrame,
    center_lat: float = CENTER_LAT,
    center_lon: float = CENTER_LON,
    radius_nm: float = RADIUS_NM
) -> DataFrame:
    lat_buffer = radius_nm / 60.0
    lon_buffer = radius_nm / (60.0 * math.cos(math.radians(center_lat)))

    df = df.filter(
        (F.col("latitude").between(center_lat - lat_buffer, center_lat + lat_buffer)) &
        (F.col("longitude").between(center_lon - lon_buffer, center_lon + lon_buffer))
    )

    return (
        df.withColumn(
            "distance_to_center_nm",
            maritime_distance_expr(
                F.col("latitude"),
                F.col("longitude"),
                F.lit(center_lat),
                F.lit(center_lon)
            )
        )
        .filter(F.col("distance_to_center_nm") <= radius_nm)
    )


def filter_stationary_and_noisy(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("mmsi").orderBy("timestamp")



    df = (
        df.withColumn("prev_timestamp", F.lag("timestamp").over(w))
          .withColumn("prev_latitude", F.lag("latitude").over(w))
          .withColumn("prev_longitude", F.lag("longitude").over(w))
          .withColumn("time_diff_sec", F.col("timestamp").cast("long") - F.col("prev_timestamp").cast("long"))
          .withColumn(
              "step_distance_nm",
              F.when(
                  F.col("prev_latitude").isNotNull(),
                  maritime_distance_expr(
                      F.col("prev_latitude"),
                      F.col("prev_longitude"),
                      F.col("latitude"),
                      F.col("longitude")
                  )
              )
          )
          .withColumn(
              "implied_speed_knots",
              F.when(
                  F.col("time_diff_sec") > 0,
                  F.col("step_distance_nm") / (F.col("time_diff_sec") / 3600.0)
              )
          )
    )

    stationary_statuses = [
        "At anchor",
        "Moored",
        "Aground",
        "Not under command",
        "Restricted manoeuverability",
        "Engaged in fishing"
    ]

    moving_expr = (
        (F.coalesce(F.col("sog"), F.lit(0.0)) >= MOVING_SOG_MIN) |
        (F.coalesce(F.col("step_distance_nm"), F.lit(0.0)) >= MOVING_STEP_NM_MIN)
    )

    plausible_expr = (
        F.col("prev_timestamp").isNull() |
        (
            (F.col("time_diff_sec") > 0) &
            (F.coalesce(F.col("step_distance_nm"), F.lit(0.0)) <= MAX_STEP_NM) &
            (F.coalesce(F.col("implied_speed_knots"), F.col("sog"), F.lit(0.0)) <= MAX_SOG_KNOTS)
        )
    )

    return (
        df.filter(~F.col("navigational_status").isin(*stationary_statuses))
          .filter(moving_expr)
          .filter(plausible_expr)
    )


def select_output_columns(df: DataFrame) -> DataFrame:
    keep_cols = [
        "timestamp", "mmsi", "name", "imo", "callsign", "type_of_mobile",
        "latitude", "longitude", "sog", "cog", "heading", "rot",
        "navigational_status", "ship_type", "cargo_type", "draught",
        "destination", "eta", "data_source_type", "distance_to_center_nm"
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]
    return df.select(*keep_cols)


def filter_one_file(spark: SparkSession, file_path: str) -> DataFrame:
    df = read_and_standardize_csv(spark, file_path)
    df = filter_invalid_rows(df)
    df = filter_geographic_area(df)
    df = filter_stationary_and_noisy(df)
    df = select_output_columns(df)
    return df.dropDuplicates(["timestamp", "mmsi", "latitude", "longitude"])


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


def process_one_file_to_same_name(spark: SparkSession, input_file: str, output_dir: str) -> str:
    # 1. Determine the exact output path first
    output_path = Path(output_dir) / Path(input_file).name

    # 2. Check if it already exists to bypass Spark processing
    if output_path.exists():
        print(f"Skipped filtering: {output_path} already exists.")
        return str(output_path)

    # 3. If it doesn't exist, run the heavy processing and write the file
    df = filter_one_file(spark, input_file)
    write_single_csv(df, str(output_path))

    return str(output_path)
