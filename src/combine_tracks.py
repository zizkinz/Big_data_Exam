import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def write_single_csv(df, final_csv_path: str) -> None:
    # Combine the temporary Spark files into a single one day output file
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


def combine_and_sort_tracks(spark: SparkSession, input_dir: str, output_csv_path: str) -> str:
    """
    Reads all daily collision track CSVs, sorts them globally by severity (mean_pair_proportion)
    and writes them to a single final CSV
    """
    input_path = str(Path(input_dir) / "*.csv")

    # Read all CSVs in the directory
    raw_df = spark.read.option("header", True).csv(input_path)

    # Cast to double for proper mathematical sorting, and sort
    sorted_df = (
        raw_df
        .withColumn("mean_pair_proportion", F.col("mean_pair_proportion").cast("double"))
        .orderBy(
            F.col("mean_pair_proportion").desc_nulls_last(),
            F.col("collision_timestamp"),
            F.col("pair_id"),
            F.col("vessel_in_pair"),
            F.col("timestamp")
        )
    )

    write_single_csv(sorted_df, output_csv_path)
    return output_csv_path