"""Print a quick data quality profile of one TLC trip file.

Usage:
    python scripts/profile_tlc.py data/landing/yellow_tripdata_2025-01.parquet
"""

import sys
from pathlib import Path

import pandas as pd


def profile(path: Path) -> None:
    month = path.stem.split("_")[-1]  # e.g. "2025-01"
    df = pd.read_parquet(path)

    print(f"\n{'=' * 60}\nFile: {path.name}\n{'=' * 60}")
    print(f"Rows: {len(df):,}   Columns: {len(df.columns)}")

    print("\nColumns and types:")
    print(df.dtypes.to_string())

    print("\nNull counts (only columns with nulls):")
    nulls = df.isna().sum()
    print(nulls[nulls > 0].to_string() if (nulls > 0).any() else "none")

    pickup = df["tpep_pickup_datetime"]
    dropoff = df["tpep_dropoff_datetime"]
    print(f"\nPickup range: {pickup.min()}  ->  {pickup.max()}")

    checks = {
        f"pickup outside {month}": (pickup.dt.strftime("%Y-%m") != month).sum(),
        "dropoff before pickup": (dropoff < pickup).sum(),
        "fare_amount <= 0": (df["fare_amount"] <= 0).sum(),
        "trip_distance <= 0": (df["trip_distance"] <= 0).sum(),
        "passenger_count == 0": (df["passenger_count"] == 0).sum(),
        "total_amount < 0": (df["total_amount"] < 0).sum(),
    }
    print("\nSuspicious rows:")
    for name, count in checks.items():
        print(f"  {name:<28} {count:>10,}  ({count / len(df):.2%})")

    duplicates = df.duplicated().sum()
    print(f"  {'exact duplicate rows':<28} {duplicates:>10,}  ({duplicates / len(df):.2%})")


if __name__ == "__main__":
    for arg in sys.argv[1:]:
        profile(Path(arg))