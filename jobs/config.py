"""Paths shared by all jobs.

LAKE_ROOT defaults to a local folder. Later it can point to S3
(for example s3a://my-bucket/lake) through an environment variable.
"""

import os
from pathlib import Path

LANDING_DIR = Path("data/landing").resolve()  # raw downloads, always local
LAKE_ROOT = os.getenv("LAKE_ROOT", str(Path("data/lake").resolve()))


def lake_path(layer: str, table: str) -> str:
    """Location of one table, for example lake_path('bronze', 'yellow_trips')."""
    return f"{LAKE_ROOT}/{layer}/{table}"