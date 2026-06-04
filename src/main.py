import argparse
import glob
import os
import gc
from pathlib import Path

from filter import create_spark, process_one_file_to_same_name
from find_collisions import process_one_filtered_file
from confirm_collisions import process_date as process_collision_tracks_for_date
from combine_tracks import combine_and_sort_tracks
from visualize_top_collision import generate_final_report


# Default project paths inside the container
# Raw AIS files are read from /data/raw
# and the final merged result is written to /output/all_collision_tracks.csv
DEFAULT_RAW_INPUT = "/data/raw/aisdk-2021-12"
DEFAULT_FILTERED_OUTPUT = "/app/data/filtered"
DEFAULT_COLLISIONS_OUTPUT = "/app/data/collisions"
DEFAULT_COLLISION_TRACKS_OUTPUT = "/app/data/collision_tracks"
DEFAULT_FINAL_MASTER_OUTPUT = "/app/output/all_collision_tracks.csv"


def parse_args():
    """Parse command-line arguments for batch processing"""
    parser = argparse.ArgumentParser(
        description="Filter daily AIS files, find collision candidates, and extract collision tracks"
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_RAW_INPUT,
        help="Input directory containing raw daily CSV files"
    )
    parser.add_argument(
        "--pattern",
        default="aisdk-2021-12*.csv",
        help="Filename pattern for daily CSV files"
    )
    parser.add_argument(
        "--n-cores",
        type=int,
        # Leave one CPU core for the main.py orchestrator
        default=max(1, (os.cpu_count() or 2) - 1)
    )
    return parser.parse_args()


def discover_files(input_dir: str, pattern: str):
    """Find all daily AIS files that match the requested pattern"""
    search_path = os.path.join(input_dir, pattern)
    files = sorted(glob.glob(search_path))
    if not files:
        raise FileNotFoundError(f"No files found under {input_dir} with pattern {pattern}")
    return files


def main():
    args = parse_args()
    input_files = discover_files(args.input, args.pattern)

    # Create output folders once before processing starts
    Path(DEFAULT_FILTERED_OUTPUT).mkdir(parents=True, exist_ok=True)
    Path(DEFAULT_COLLISIONS_OUTPUT).mkdir(parents=True, exist_ok=True)
    Path(DEFAULT_COLLISION_TRACKS_OUTPUT).mkdir(parents=True, exist_ok=True)
    Path(DEFAULT_FINAL_MASTER_OUTPUT).parent.mkdir(parents=True, exist_ok=True)

    # Process one day at a time to lower Spark memory usage and to flush unwanted residual memory
    for idx, input_file in enumerate(input_files, start=1):
        print(f"\n--- Starting processing for file [{idx}/{len(input_files)}]: {input_file} ---")

        # Start a dedicated Spark session for this day.\
        spark = create_spark(
            app_name=f"ais-processing-day-{idx}",
            n_cores=args.n_cores
        )

        try:
            # Filter the raw AIS file
            filtered_path = process_one_file_to_same_name(
                spark, input_file, DEFAULT_FILTERED_OUTPUT
            )
            print(f"[{idx}/{len(input_files)}] Wrote filtered file: {filtered_path}")

            # Detect candidate collision events from the filtered day file
            collision_path = process_one_filtered_file(
                spark, filtered_path, DEFAULT_COLLISIONS_OUTPUT
            )
            print(f"[{idx}/{len(input_files)}] Wrote collision candidates: {collision_path}")


            # Extract vessel tracks around each confirmed collision candidate.
            date_str = Path(input_file).stem.replace("aisdk-", "")
            collision_tracks_path = process_collision_tracks_for_date(
                spark,
                date_str,
                DEFAULT_FILTERED_OUTPUT,
                DEFAULT_COLLISIONS_OUTPUT,
                DEFAULT_COLLISION_TRACKS_OUTPUT
            )
            print(f"[{idx}/{len(input_files)}] Wrote collision tracks: {collision_tracks_path}")

        finally:
            # Kill the Spark to completely flush heap memory
            spark.stop()

            # Force Python cleanup before the next day starts
            gc.collect()

    # After all day jobs are complete, merge the per-day collision-track outputs
    # into one output file for final ranking
    print("\nAll days processed. Combining and sorting final master report")

    # Use one last Spark session for the final merge
    spark_final = create_spark(app_name="ais-combine-results", n_cores=args.n_cores)
    try:
        final_output_path = combine_and_sort_tracks(
            spark_final,
            DEFAULT_COLLISION_TRACKS_OUTPUT,
            DEFAULT_FINAL_MASTER_OUTPUT
        )
        print(f"SUCCESS: Final sorted master report written to: {final_output_path}")
    finally:
        spark_final.stop()
        gc.collect()

    # Generate the final top collision track map
    generate_final_report()

if __name__ == "__main__":
    main()