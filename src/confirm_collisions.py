from pathlib import Path
import argparse
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from filter import create_spark, maritime_distance_expr

# Interpolation step in seconds used when evaluating vessel motion between observed points
INTERPOLATION_STEP_SEC = 1
# Bounding time intervals before and after alleged collision (10 minutes as defined in the assignment)
TRACK_WINDOW_SEC = 10 * 60
# Final confirmation uses a stricter distance threshold than the earlier candidate stage
EXACT_COLLISION_TOLERANCE_M = 100
EXACT_COLLISION_TOLERANCE_NM = EXACT_COLLISION_TOLERANCE_M / 1852.0

DEFAULT_FILTERED_DIR = "/app/data/filtered"
DEFAULT_COLLISIONS_DIR = "/app/data/collisions"
DEFAULT_OUTPUT_DIR = "/app/data/collision_tracks"


def load_filtered_day(spark: SparkSession, file_path: str):
    # Load the filtered day AIS file and cast the columns needed for
    # interpolation and distance calculations.
    return (
        spark.read.option("header", True).csv(file_path)
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("ts", F.col("timestamp").cast("long"))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("sog", F.col("sog").cast("double"))
        .select("mmsi", "name", "timestamp", "ts", "latitude", "longitude", "sog", "navigational_status")
        .filter(F.col("mmsi").isNotNull() & F.col("ts").isNotNull())
        .filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
    )


def load_collision_candidates(spark: SparkSession, file_path: str):
    # Read the candidate collision pairs and build a time window around each pair
    return (
        spark.read.option("header", True).csv(file_path)
        .withColumn("timestamp_1", F.to_timestamp("timestamp_1"))
        .withColumn("timestamp_2", F.to_timestamp("timestamp_2"))
        .withColumn("ts_1", F.col("timestamp_1").cast("long"))
        .withColumn("ts_2", F.col("timestamp_2").cast("long"))
        .withColumn("center_ts", ((F.col("ts_1") + F.col("ts_2")) / 2).cast("long"))
        .withColumn("window_start", F.least(F.col("ts_1"), F.col("ts_2")) - F.lit(TRACK_WINDOW_SEC))
        .withColumn("window_end", F.greatest(F.col("ts_1"), F.col("ts_2")) + F.lit(TRACK_WINDOW_SEC))
        .withColumn("pair_id", F.concat_ws("_", F.col("mmsi_1"), F.col("mmsi_2"), F.col("center_ts")))
        .select(
            "pair_id", "mmsi_1", "mmsi_2", "name_1", "name_2",
            "timestamp_1", "timestamp_2", "ts_1", "ts_2", "center_ts", "window_start", "window_end"
        )
    )

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

def build_segments(points_df, ts_col_prefix: str):
    # For each point find the next observed point for the same vessel pair
    start_ts = f"{ts_col_prefix}_start_ts"
    start_lat = f"{ts_col_prefix}_start_lat"
    start_lon = f"{ts_col_prefix}_start_lon"
    end_ts = f"{ts_col_prefix}_end_ts"
    end_lat = f"{ts_col_prefix}_end_lat"
    end_lon = f"{ts_col_prefix}_end_lon"
    ts_name = f"ts_{ts_col_prefix}"
    lat_name = f"lat_{ts_col_prefix}"
    lon_name = f"lon_{ts_col_prefix}"

    return (
        points_df.alias("l")
        .join(
            points_df.alias("r"),
            on=(
                (F.col("l.pair_id") == F.col("r.pair_id")) &
                (F.col(f"l.{ts_name}") < F.col(f"r.{ts_name}"))
            ),
            how="inner",
        )
        .groupBy(
            F.col("l.pair_id").alias("pair_id"),
            F.col("l.mmsi_1").alias("mmsi_1"),
            F.col("l.mmsi_2").alias("mmsi_2"),
            F.col("l.name_1").alias("name_1"),
            F.col("l.name_2").alias("name_2"),
            F.col(f"l.{ts_name}").alias(start_ts),
            F.col(f"l.{lat_name}").alias(start_lat),
            F.col(f"l.{lon_name}").alias(start_lon),
        )
        .agg(
            F.min(
                F.struct(
                    F.col(f"r.{ts_name}"),
                    F.col(f"r.{lat_name}"),
                    F.col(f"r.{lon_name}")
                )
            ).alias("next_pt")
        )
        .select(
            "pair_id", "mmsi_1", "mmsi_2", "name_1", "name_2",
            start_ts, start_lat, start_lon,
            F.col(f"next_pt.{ts_name}").alias(end_ts),
            F.col(f"next_pt.{lat_name}").alias(end_lat),
            F.col(f"next_pt.{lon_name}").alias(end_lon),
        )
        .filter(F.col(end_ts).isNotNull())
        .filter(F.col(end_ts) > F.col(start_ts))
    )


def find_confirmed_collision_times(filtered_df, candidates_df):
    # Pull all filtered AIS points for vessel A and vessel B within each
    # candidate window, then compare their tracks at one-second steps.
    a_points = (
        candidates_df.alias("p")
        .join(
            filtered_df.alias("f"),
            on=(
                (F.col("f.mmsi") == F.col("p.mmsi_1")) &
                (F.col("f.ts") >= F.col("p.window_start")) &
                (F.col("f.ts") <= F.col("p.window_end"))
            ),
            how="inner",
        )
        .select(
            "pair_id", "mmsi_1", "mmsi_2", "name_1", "name_2",
            F.col("f.ts").alias("ts_a"),
            F.col("f.latitude").alias("lat_a"),
            F.col("f.longitude").alias("lon_a"),
        )
        .dropDuplicates(["pair_id", "ts_a", "lat_a", "lon_a"])
    )

    b_points = (
        candidates_df.alias("p")
        .join(
            filtered_df.alias("f"),
            on=(
                (F.col("f.mmsi") == F.col("p.mmsi_2")) &
                (F.col("f.ts") >= F.col("p.window_start")) &
                (F.col("f.ts") <= F.col("p.window_end"))
            ),
            how="inner",
        )
        .select(
            "pair_id", "mmsi_1", "mmsi_2", "name_1", "name_2",
            F.col("f.ts").alias("ts_b"),
            F.col("f.latitude").alias("lat_b"),
            F.col("f.longitude").alias("lon_b"),
        )
        .dropDuplicates(["pair_id", "ts_b", "lat_b", "lon_b"])
    )

    # Convert the data points into linear segments
    a_segments = build_segments(a_points, "a")
    b_segments = build_segments(b_points, "b")

    # Keep only segment pairs whose time ranges overlap
    overlapping = (
        a_segments.alias("a")
        .join(
            b_segments.alias("b"),
            on=(
                    (F.col("a.pair_id") == F.col("b.pair_id")) &
                    (F.greatest(F.col("a.a_start_ts"), F.col("b.b_start_ts")) < F.least(F.col("a.a_end_ts"),
                                                                                        F.col("b.b_end_ts")))
            ),
            how="inner",
        )
        .withColumn("overlap_start", F.greatest(F.col("a.a_start_ts"), F.col("b.b_start_ts")))
        .withColumn("overlap_end", F.least(F.col("a.a_end_ts"), F.col("b.b_end_ts")))
        .filter(F.col("overlap_start") <= F.col("overlap_end"))
        .withColumn(
            "interp_ts",
            F.sequence(F.col("overlap_start"), F.col("overlap_end"))
        )
        .withColumn("interp_ts", F.explode("interp_ts"))
    )

    # Linearly interpolate each vessel position at every second in the overlap
    interpolated = (
        overlapping
        .withColumn(
            "frac_a",
            (F.col("interp_ts") - F.col("a.a_start_ts")) / (F.col("a.a_end_ts") - F.col("a.a_start_ts"))
        )
        .withColumn(
            "frac_b",
            (F.col("interp_ts") - F.col("b.b_start_ts")) / (F.col("b.b_end_ts") - F.col("b.b_start_ts"))
        )
        .withColumn(
            "lat_a_interp",
            F.col("a.a_start_lat") + F.col("frac_a") * (F.col("a.a_end_lat") - F.col("a.a_start_lat"))
        )
        .withColumn(
            "lon_a_interp",
            F.col("a.a_start_lon") + F.col("frac_a") * (F.col("a.a_end_lon") - F.col("a.a_start_lon"))
        )
        .withColumn(
            "lat_b_interp",
            F.col("b.b_start_lat") + F.col("frac_b") * (F.col("b.b_end_lat") - F.col("b.b_start_lat"))
        )
        .withColumn(
            "lon_b_interp",
            F.col("b.b_start_lon") + F.col("frac_b") * (F.col("b.b_end_lon") - F.col("b.b_start_lon"))
        )
        .withColumn(
            "distance_nm",
            maritime_distance_expr(
                F.col("lat_a_interp"), F.col("lon_a_interp"),
                F.col("lat_b_interp"), F.col("lon_b_interp")
            )
        )
        .withColumn("distance_m", F.col("distance_nm") * F.lit(1852.0))
        .filter(F.col("distance_nm") <= F.lit(EXACT_COLLISION_TOLERANCE_NM))
    )

    # Keep the closest interpolated encounter for each candidate pair and use
    # that timestamp as the confirmed collision time
    confirmed = (
        interpolated.groupBy(F.col("a.pair_id").alias("pair_id"))
        .agg(
            F.min(
                F.struct(
                    F.col("distance_m"),
                    F.col("interp_ts"),
                    F.col("a.mmsi_1"),
                    F.col("a.mmsi_2"),
                    F.col("a.name_1"),
                    F.col("a.name_2")
                )
            ).alias("hit")
        )
        .select(
            "pair_id",
            F.col("hit.mmsi_1").alias("mmsi_1"),
            F.col("hit.mmsi_2").alias("mmsi_2"),
            F.col("hit.name_1").alias("name_1"),
            F.col("hit.name_2").alias("name_2"),
            F.col("hit.interp_ts").alias("collision_ts"),
            F.round(F.col("hit.distance_m"), 3).alias("collision_distance_m")
        )
        .withColumn("window_start", F.col("collision_ts") - F.lit(TRACK_WINDOW_SEC))
        .withColumn("window_end", F.col("collision_ts") + F.lit(TRACK_WINDOW_SEC))
    )

    return confirmed


def extract_collision_tracks(filtered_df, confirmed_df):
    # Build a 10-minute track window around the confirmed collision for each
    # vessel in the pair
    vessel_1_tracks = (
        confirmed_df.alias("c")
        .join(
            filtered_df.alias("f"),
            on=(
                (F.col("f.mmsi") == F.col("c.mmsi_1")) &
                (F.col("f.ts") >= F.col("c.window_start")) &
                (F.col("f.ts") <= F.col("c.window_end"))
            ),
            how="inner",
        )
        .select(
            "c.pair_id",
            F.from_unixtime(F.col("c.collision_ts")).cast("timestamp").alias("collision_timestamp"),
            F.col("c.mmsi_1").alias("pair_mmsi_1"),
            F.col("c.mmsi_2").alias("pair_mmsi_2"),
            F.col("f.mmsi").alias("mmsi"),
            F.col("f.name").alias("name"),
            F.col("f.timestamp").alias("timestamp"),
            F.col("f.latitude").alias("latitude"),
            F.col("f.longitude").alias("longitude"),
            F.col("f.sog").alias("sog"),
            F.col("f.navigational_status").alias("navigational_status"),
            F.lit(1).alias("vessel_in_pair")
        )
    )

    vessel_2_tracks = (
        confirmed_df.alias("c")
        .join(
            filtered_df.alias("f"),
            on=(
                (F.col("f.mmsi") == F.col("c.mmsi_2")) &
                (F.col("f.ts") >= F.col("c.window_start")) &
                (F.col("f.ts") <= F.col("c.window_end"))
            ),
            how="inner",
        )
        .select(
            "c.pair_id",
            F.from_unixtime(F.col("c.collision_ts")).cast("timestamp").alias("collision_timestamp"),
            F.col("c.mmsi_1").alias("pair_mmsi_1"),
            F.col("c.mmsi_2").alias("pair_mmsi_2"),
            F.col("f.mmsi").alias("mmsi"),
            F.col("f.name").alias("name"),
            F.col("f.timestamp").alias("timestamp"),
            F.col("f.latitude").alias("latitude"),
            F.col("f.longitude").alias("longitude"),
            F.col("f.sog").alias("sog"),
            F.col("f.navigational_status").alias("navigational_status"),
            F.lit(2).alias("vessel_in_pair")
        )
    )

    return (
        vessel_1_tracks.unionByName(vessel_2_tracks)
        .dropDuplicates(["pair_id", "mmsi", "timestamp", "latitude", "longitude"])
        .orderBy("collision_timestamp", "pair_id", "vessel_in_pair", "timestamp")
    )

def add_sog_proportion(tracks_df):
    # Compare average speed before and after the collision time for each vessel
    sog_base = tracks_df.select("mmsi", "pair_id", "timestamp", "collision_timestamp", "sog")

    sog_stats = (
        sog_base
        .withColumn("sog_before", F.when(F.col("timestamp") < F.col("collision_timestamp"), F.col("sog")))
        .withColumn("sog_after", F.when(F.col("timestamp") > F.col("collision_timestamp"), F.col("sog")))
        .groupBy("mmsi", "pair_id")
        .agg(
            F.avg("sog_before").alias("mean_sog_before"),
            F.avg("sog_after").alias("mean_sog_after")
        )
        .withColumn("mean_sog_before", F.coalesce(F.col("mean_sog_before"), F.lit(0.01)))
        .withColumn("mean_sog_after", F.coalesce(F.col("mean_sog_after"), F.lit(0.01)))
        .withColumn("sog_proportion", F.col("mean_sog_before") / F.col("mean_sog_after"))
        .select("mmsi", "pair_id", "sog_proportion")
    )

    # Keep only pairs where the average speed after the collision for both vessels
    # has decreased by at least 10%
    filtered_stats = (
        sog_stats
        .groupBy("pair_id")
        .agg(
            F.min("sog_proportion").alias("min_pair_proportion"),
            F.avg("sog_proportion").alias("mean_pair_proportion")
        )
        .filter(F.col("min_pair_proportion") >= 1.1)
        .select("pair_id", "mean_pair_proportion")
    )

    return (
        tracks_df
        .join(filtered_stats, on="pair_id", how="inner")
    )





def process_date(spark: SparkSession, date_str: str, filtered_dir: str, collisions_dir: str, output_dir: str):
    # Preserve the original filename in the filtered output directory so that
    # later pipeline stages can match day files by date without extra mapping
    output_path = str(Path(output_dir) / f"aisdk-{date_str}.csv")

    if Path(output_path).exists():
        print(f"Skipped collision tracks extraction: {output_path} already exists.")
        return output_path

    filtered_file = str(Path(filtered_dir) / f"aisdk-{date_str}.csv")
    collision_file = str(Path(collisions_dir) / f"aisdk-{date_str}.csv")

    filtered_df = load_filtered_day(spark, filtered_file)
    candidates_df = load_collision_candidates(spark, collision_file)
    confirmed_df = find_confirmed_collision_times(filtered_df, candidates_df)
    tracks_df = extract_collision_tracks(filtered_df, confirmed_df)

    # localCheckpoint truncates the lineage so the next aggregation does not
    # repeatedly recompute the long interpolation pipeline
    tracks_df = tracks_df.localCheckpoint(eager=True)

    tracks_df = add_sog_proportion(tracks_df)

    write_single_csv(tracks_df, output_path)

    spark.catalog.clearCache()
    return output_path





def parse_args():
    # Parse the input date and optional directory overrides for batch runs.
    parser = argparse.ArgumentParser(description="Extract AIS trajectory points around confirmed interpolated collisions")
    parser.add_argument("--date", required=True, help="Date string like 2021-12-13")
    parser.add_argument("--filtered-dir", default=DEFAULT_FILTERED_DIR)
    parser.add_argument("--collisions-dir", default=DEFAULT_COLLISIONS_DIR)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--n-cores", type=int, default=4)
    return parser.parse_args()


def main():
    # Create Spark, process the requested day and stop Spark
    args = parse_args()
    spark = create_spark(app_name="ais-collision-tracks", n_cores=args.n_cores)
    try:
        out_path = process_date(spark, args.date, args.filtered_dir, args.collisions_dir, args.output_dir)
        print(f"Collision tracks written to: {out_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()