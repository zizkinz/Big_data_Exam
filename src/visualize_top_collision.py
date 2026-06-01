import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Paths align with the orchestrator
DEFAULT_FINAL_MASTER_OUTPUT = "/app/output/all_collision_tracks.csv"
MAP_OUTPUT_FILE = "/app/output/collision_map.png"


def generate_final_report():
    csv_path = Path(DEFAULT_FINAL_MASTER_OUTPUT)
    if not csv_path.exists():
        print(f"Warning: Master output not found at {csv_path}. Skipping visualization.")
        return

    print("\nLoading master collision report for visualization...")
    df = pd.read_csv(csv_path)

    if df.empty:
        print("No collision tracks found in the data.")
        return

    # 1. Isolate the top collision pair
    top_pair_id = df.iloc[0]['pair_id']
    incident_df = df[df['pair_id'] == top_pair_id].copy()

    # Convert timestamps to datetime objects
    incident_df['timestamp'] = pd.to_datetime(incident_df['timestamp'])
    collision_timestamp = pd.to_datetime(incident_df.iloc[0]['collision_timestamp'])

    # 2. Separate the two vessels
    v1_mmsi = incident_df.iloc[0]['pair_mmsi_1']
    v2_mmsi = incident_df.iloc[0]['pair_mmsi_2']

    v1_df = incident_df[incident_df['mmsi'] == v1_mmsi].sort_values('timestamp')
    v2_df = incident_df[incident_df['mmsi'] == v2_mmsi].sort_values('timestamp')

    v1_name = str(v1_df['name'].iloc[0]) if not v1_df['name'].isna().all() else f"Vessel {v1_mmsi}"
    v2_name = str(v2_df['name'].iloc[0]) if not v2_df['name'].isna().all() else f"Vessel {v2_mmsi}"

    # Find the closest actual reported AIS point to the calculated collision time
    v1_impact_pt = v1_df.iloc[(v1_df['timestamp'] - collision_timestamp).abs().argmin()]
    v2_impact_pt = v2_df.iloc[(v2_df['timestamp'] - collision_timestamp).abs().argmin()]

    # --- PRINT REQUIRED CONSOLE OUTPUT ---
    print("\n" + "=" * 50)
    print("🚨 CATASTROPHIC COLLISION IDENTIFIED 🚨")
    print("=" * 50)
    print(f"Timestamp of Impact : {collision_timestamp} UTC")
    print("-" * 50)
    print(f"VESSEL 1:")
    print(f"  Name : {v1_name}")
    print(f"  MMSI : {v1_mmsi}")
    print(f"  Impact Coordinates: {v1_impact_pt['latitude']:.5f}, {v1_impact_pt['longitude']:.5f}")
    print(f"  Speed at Impact   : {v1_impact_pt['sog']} knots")
    print("-" * 50)
    print(f"VESSEL 2:")
    print(f"  Name : {v2_name}")
    print(f"  MMSI : {v2_mmsi}")
    print(f"  Impact Coordinates: {v2_impact_pt['latitude']:.5f}, {v2_impact_pt['longitude']:.5f}")
    print(f"  Speed at Impact   : {v2_impact_pt['sog']} knots")
    print("=" * 50 + "\n")

    # --- GENERATE MATPLOTLIB MAP ---
    print("Generating Matplotlib Map...")

    plt.figure(figsize=(10, 8))
    plt.style.use('seaborn-v0_8-darkgrid')  # Uses seaborn styling from your requirements

    # Plot tracks (Longitude is X, Latitude is Y)
    plt.plot(v1_df['longitude'], v1_df['latitude'], color='red', linewidth=2, label=f"Track: {v1_name}")
    plt.plot(v2_df['longitude'], v2_df['latitude'], color='blue', linewidth=2, label=f"Track: {v2_name}")

    # Mark Start points
    plt.scatter(v1_df.iloc[0]['longitude'], v1_df.iloc[0]['latitude'], color='darkred', marker='o', s=100,
                label=f"{v1_name} Start")
    plt.scatter(v2_df.iloc[0]['longitude'], v2_df.iloc[0]['latitude'], color='darkblue', marker='o', s=100,
                label=f"{v2_name} Start")

    # Mark Impact points
    plt.scatter(v1_impact_pt['longitude'], v1_impact_pt['latitude'], color='red', marker='X', s=200, edgecolor='black',
                zorder=5, label="Impact Point")
    plt.scatter(v2_impact_pt['longitude'], v2_impact_pt['latitude'], color='blue', marker='X', s=200, edgecolor='black',
                zorder=5)

    # Annotations & Formatting
    plt.title(f"Collision Trajectory Map\n{v1_name} vs {v2_name}", fontsize=14, fontweight='bold')
    plt.xlabel("Longitude", fontsize=12)
    plt.ylabel("Latitude", fontsize=12)
    plt.legend(loc='best')
    plt.tight_layout()

    # Save output
    out_path = Path(MAP_OUTPUT_FILE)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=300)
    plt.close()

    print(f"✅ Success! Static map saved to: {out_path}")


if __name__ == "__main__":
    generate_final_report()