"""Download NYC TLC Yellow Taxi trip files and the taxi zone lookup.

Usage:
    python ingestion/download_tlc.py 2025-01 2025-02 2025-03

Safe to rerun: files that already exist are skipped.
"""

import sys
import time
from pathlib import Path

import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
TRIP_URL = BASE_URL + "/trip-data/yellow_tripdata_{month}.parquet"
ZONE_URL = BASE_URL + "/misc/taxi_zone_lookup.csv"

LANDING_DIR = Path("data/landing")
DEFAULT_MONTHS = ["2025-01", "2025-02", "2025-03"]


def download(url: str, dest: Path, retries: int = 3) -> None:
    """Stream a file to disk, skipping it if it already exists."""
    if dest.exists() and dest.stat().st_size > 0:
        print(f"  skip (already exists): {dest.name}")
        return

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")

    for attempt in range(1, retries + 1):
        try:
            print(f"  downloading {dest.name} (attempt {attempt})...")
            with requests.get(url, stream=True, timeout=60) as response:
                response.raise_for_status()
                with open(tmp, "wb") as f:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
            tmp.replace(dest)  # only a complete file gets the real name
            print(f"  saved {dest} ({dest.stat().st_size / 1_000_000:.1f} MB)")
            return
        except requests.RequestException as exc:
            print(f"  failed: {exc}")
            if attempt == retries:
                raise
            time.sleep(2 ** attempt)  # wait 2s, 4s, ... before retrying


def main(months: list[str]) -> None:
    print("Zone lookup:")
    download(ZONE_URL, LANDING_DIR / "taxi_zone_lookup.csv")

    for month in months:
        print(f"Trips {month}:")
        download(
            TRIP_URL.format(month=month),
            LANDING_DIR / f"yellow_tripdata_{month}.parquet",
        )


if __name__ == "__main__":
    main(sys.argv[1:] or DEFAULT_MONTHS)