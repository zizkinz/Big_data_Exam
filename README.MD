## Project Structure

``` text
Big_data_Exam/
├─ data/
│  ├─ raw/                     # Original AIS data
│  ├─ filtered/                # Non-stationary vessel data
│  ├─ collisions_local/        # Candidate collision pairs from 20-min windows
│  ├─ collisions_confirmed/    # Collisions confirmed after path interpolation
│  └─ output/
│     ├─ all_collisions.csv    # Final ranked list of collision events
│     └─ top_collision.png     # Visualization of the top collision event
├─ src/
│  ├─ main.py                  # Orchestrator: runs the full pipeline in order
│  ├─ filter.py                # Daily data cleaning and quality filtering
│  ├─ find_collisions.py       # Local cross-joins within 20-min windows (150 m)
│  ├─ confirm_collisions.py    # Path interpolation + 100 m threshold + SOG rule
│  └─ combine_tracks.py        # Aggregates confirmed collisions and ranks them
├─ src/visualization/
│  └─ visualize_top_collision.py  # Draws a map for the top-ranked collision
├─ config/
│  └─ config.yaml              # Paths, Spark config, and pipeline parameters
├─ requirements.txt            # Python dependencies (PySpark, plotting libs, etc.)
└─ README.md                   # This file
```

------------------------------------------------------------------------

## Requirements

-   Python 3.x

-   PySpark

-   Standard scientific stack: `pandas`, `numpy`

-   Geospatial / plotting libraries used in `visualize_top_collision.py`, e.g.:

    -   `matplotlib`

-   A working Spark environment:

    -   Local (`--master local[*]`) or
    -   Cluster, as configured in `config.yaml`

------------------------------------------------------------------------

## Pipeline Overview

The full workflow is orchestrated by `main.py`. It runs a sequence of PySpark jobs that:

1.  Filter raw AIS data day by day to remove clearly invalid or unusable records.
2.  Identify candidate vessel encounters where two ships come within 150 m of each other inside overlapping 20-minute windows via cross-joins.
3.  Confirm which of these encounters are likely collisions by interpolating vessel trajectories, applying a stricter 100 m distance threshold, and analyzing speed changes.
4.  Aggregate all confirmed collision tracks into a single file and rank them by severity (average speed decrease).
5.  Visualize the top-ranked collision event on a map.

At a high level:

``` bash
spark-submit src/main.py --config config/config.yaml
```

will perform the entire pipeline, from filtering to visualization

------------------------------------------------------------------------

## Code Explanation

### `main.py` – Orchestrator

`main.py` is the main script in the project. It:

-   Initializes Spark with the configuration specified in `config/config.yaml`
-   Orchestrates the sequential execution of the following steps:
    1.  Daily filtering (`filter.py`)
    2.  Collision candidate search (`find_collisions.py`)
    3.  Collision confirmation and speed analysis (`confirm_collisions.py`)
    4.  Aggregation and ranking of collision tracks (`combine_tracks.py`)
    5.  Optional visualization of the top collision (`visualize_top_collision.py`)

The script wires the I/O paths between steps (e.g., `filtered/` → `collisions_local/` → `collisions_confirmed/` → `output/`).

------------------------------------------------------------------------

### `filter.py` – Daily AIS Data Filtering

`filter.py` takes raw AIS data, partitions it by day, and applies a set of quality rules to remove incorrect records. Typical rules (as implemented in the code) include, for example:

-   Dropping rows with missing or obviously invalid coordinates and/or MMSI values.
-   Filtering out impossible speeds, headings, or timestamps.
-   Removing vessels having stationary status.

The output is a cleaned dataset per day in `data/filtered/`, which serves as the input for collision detection.

------------------------------------------------------------------------

### `find_collisions.py` – Local Cross-Joins on 20-Minute Windows (150 m)

`find_collisions.py` operates on the filtered AIS data. Conceptually, it:

1.  Builds overlapping 20-minute time windows for each vessel’s trajectory.
2.  Within each window, performs local cross-joins between vessel positions so that only vessels that are temporally close are considered together.
3.  Computes pairwise distances between vessels in the same window.
4.  Flags “collision candidates” when two vessels come closer than **150 meters** at some point inside the 20-minute window (according to the distance calculation implemented in the code).

The result is a set of candidate vessel pairs with time windows and approximate locations where a potential collision might have occurred. These are stored in `data/collisions_local/`.

------------------------------------------------------------------------

### `confirm_collisions.py` – Path Interpolation, 100 m Threshold, and SOG Rule

`confirm_collisions.py` takes the candidate encounters and applies a stricter, more physically meaningful analysis:

1.  **Path interpolation (20-minute window):**
    -   For each candidate vessel pair, trajectories over the relevant 20-minute window are interpolated.
    -   This yields a more fine-grained representation of each vessel’s path during the encounter.
2.  **Collision confirmation (100 m threshold):**
    -   For every second in the interpolated window, the distance between the two vessels tarjectories is computed.
    -   A “collision event” is mapped if at least one point in the window has the distance strictly below **100 meters** (as vessels themselves can be longer than a 100 meters, so a collision might result in a non-zero distance between the two corresponding transponders).
3.  **Speed-over-ground (SOG) analysis before and after collision:**
    -   For each confirmed collision event, the mean SOG is separately calculated for both vessels before and after the collision time.
    -   If the decrease in mean SOG after the collision is less than **10%** for a vessel pair, or if both vessels have increased their speed, the event is discarded as a likely non-collision.
4.  **Handling transponder drop-offs:**
    -   If a vessel’s AIS transponder stops transmitting shortly after the collision, the mean SOG after collision for that vessel is set to **0.01** by convention.
    -   This allows such cases to be treated as strong collision candidates (or severe events) without dividing by zero or having missing speed values.

Only events that pass both the distance criterion (100 m) and the speed-change criterion (≥10% drop in mean SOG) are kept. These confirmed collision tracks are written to `data/collisions_confirmed/`.

------------------------------------------------------------------------

### `combine_tracks.py` – Aggregation and Ranking of Collision Tracks

`combine_tracks.py` collects all confirmed collision events and:

1.  Merges them into a single dataset.
2.  Computes summary statistics for each collision event, including the **average speed decrease after the collision** (across both vessels, according to the rules in `confirm_collisions.py`).
3.  Sorts the events in **descending order** of average speed decrease, so that the most severe collisions (largest speed drop) appear first.

The aggregated and ranked output is written to `data/output/all_collisions.csv` (or the path specified in `config.yaml`).

------------------------------------------------------------------------

### `visualize_top_collision.py` – Map Visualization of the Top Event

`visualize_top_collision.py` takes the top-ranked collision event from the aggregated list and produces a visual map:

-   Reads the top event from `data/output/all_collisions.csv`.
-   Extracts the trajectories of the two vessels involved.
-   Plots the paths on a map, including:
    -   Vessel tracks over the collision window.
    -   The approximate collision point where the interpolated distance was the lowest.

The output is a picture (`data/output/top_collision.png`) depicting the collision event.

------------------------------------------------------------------------

## Running the Whole Pipeline Step by Step

Although `main.py` can run the pipeline end-to-end, each step can also be executed separately for debugging or experimentation.

Examples (adapt paths and arguments as needed):

``` bash
# 1. Filter raw data
spark-submit src/filter.py --config config/config.yaml

# 2. Find collision candidates (150 m, 20-min windows)
spark-submit src/find_collisions.py --config config/config.yaml

# 3. Confirm collisions (100 m + SOG rules)
spark-submit src/confirm_collisions.py --config config/config.yaml

# 4. Combine and rank tracks
spark-submit src/combine_tracks.py --config config/config.yaml

# 5. Visualize the top collision
python src/visualization/visualize_top_collision.py --config config/config.yaml
```

Running `main.py` should orchestrate these steps automatically with the correct sequence and dependencies.

------------------------------------------------------------------------

## Notes and Assumptions

-   All distance thresholds (150 m for candidates, 100 m for confirmation) and the 10% SOG decrease threshold are domain choices encoded in the code and can be adjusted via configuration or parameters if needed.
-   The choice of 20-minute overlapping windows balances temporal resolution with computational cost; smaller windows increase temporal precision but also increase the number of local cross-joins.
-   The convention of setting post-collision mean SOG to 0.01 when the transponder goes off is intended to flag potentially severe events while maintaining numeric stability in calculations.

## Top Ranked Collision Event

The pipeline identifies one event as the most severe collision in the dataset, with an average proportional speed decrease of approximately **300.99** times across the involved vessels (`mean_pair_proportion ≈ 300.99`).\
This event involves the vessels **KARIN HOEJ** (MMSI `219021240`) and **MV SCOT CARRIER** (MMSI `232018267`) near coordinates **55.223067° N, 14.243730° E** at timestamp **2021‑12‑13T02:27:29Z**. [<file:25>]

The trajectories reconstructed by the pipeline show MV SCOT CARRIER (blue track) following a curved path, initially sailing southward and then gradually turning and straightening as it proceeds eastward along a relatively steady course.\
KARIN HOEJ (red track), in contrast, approaches on a more direct, nearly straight line from the northeast towards the impact area, intersecting the path of MV SCOT CARRIER at the collision point. [<file:25>]

The collision map below visualizes the interpolated paths and the impact point used in the analysis.\
The start of each trajectory is marked with a filled circle, while the collision point is shown as a red cross at the location where the interpolated separation distance dropped below 100 meters and the subsequent speed behavior satisfied the ≥10% SOG decrease rule (the transponder of KARIN HOEJ went offline right after the collision).

![Collision Trajectory Map](collision_map.jpg)


docker compose up --build -d
docker compose exec ais-collision bash
spark-submit src/main.py
