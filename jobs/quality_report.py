"""Build a data quality report from Silver and quarantine.

Run from the project root:
    docker compose exec spark python -m jobs.quality_report
    docker compose exec spark python -m jobs.quality_report --strict

--strict exits with an error if any month's rejection rate exceeds the
threshold, so a scheduler (Airflow, later) can stop a bad load.
"""

import sys
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from jobs.config import lake_path
from jobs.spark_session import get_spark

MAX_REJECT_RATE = 0.05  # 5% of a month's rows
REPORT_PATH = Path("docs/quality_report.md")


def collect_stats(spark: SparkSession) -> list[dict]:
    silver_root = Path(lake_path("silver", "yellow_trips"))
    quarantine_root = Path(lake_path("quarantine", "yellow_trips"))

    stats = []
    for part in sorted(silver_root.glob("source_month=*")):
        month = part.name.split("=")[1]
        silver_rows = spark.read.parquet(str(part)).count()

        quarantine_rows, reasons = 0, {}
        quarantine_part = quarantine_root / part.name
        if quarantine_part.exists():
            quarantined = spark.read.parquet(str(quarantine_part))
            quarantine_rows = quarantined.count()
            reasons = {
                row["reason"]: row["count"]
                for row in quarantined.select(
                    F.explode(F.split("reject_reasons", r"\|")).alias("reason")
                )
                .groupBy("reason")
                .count()
                .collect()
            }

        total = silver_rows + quarantine_rows
        stats.append(
            {
                "month": month,
                "total": total,
                "silver": silver_rows,
                "quarantine": quarantine_rows,
                "rate": quarantine_rows / total if total else 0.0,
                "reasons": reasons,
            }
        )
    return stats


def render(stats: list[dict]) -> str:
    lines = [
        "# Data Quality Report",
        "",
        f"Generated: {date.today()}  |  Threshold: {MAX_REJECT_RATE:.0%} rejected per month",
        "",
        "## Per month",
        "",
        "| Month | Rows in | Silver | Quarantined | Reject rate | Status |",
        "|---|---:|---:|---:|---:|---|",
    ]
    for s in stats:
        status = "OK" if s["rate"] <= MAX_REJECT_RATE else "OVER THRESHOLD"
        lines.append(
            f"| {s['month']} | {s['total']:,} | {s['silver']:,} | "
            f"{s['quarantine']:,} | {s['rate']:.2%} | {status} |"
        )

    all_rules = sorted({rule for s in stats for rule in s["reasons"]})
    lines += [
        "",
        "## Rejections by rule",
        "",
        "A row that breaks several rules is counted under each one.",
        "",
        "| Rule | " + " | ".join(s["month"] for s in stats) + " |",
        "|---|" + "---:|" * len(stats),
    ]
    for rule in all_rules:
        counts = " | ".join(f"{s['reasons'].get(rule, 0):,}" for s in stats)
        lines.append(f"| {rule} | {counts} |")

    if not all_rules:
        lines.append("| (no rejections) |" + " |" * len(stats))
    return "\n".join(lines) + "\n"


def main() -> None:
    spark = get_spark("quality-report")
    spark.sparkContext.setLogLevel("WARN")

    stats = collect_stats(spark)
    if not stats:
        raise SystemExit("Silver is empty. Run the silver job first.")

    report = render(stats)
    REPORT_PATH.write_text(report, encoding="utf-8")
    print(report)
    print(f"Written to {REPORT_PATH}")
    spark.stop()

    breaches = [s["month"] for s in stats if s["rate"] > MAX_REJECT_RATE]
    if breaches and "--strict" in sys.argv:
        raise SystemExit(f"Quality gate failed: {breaches} exceed {MAX_REJECT_RATE:.0%} rejection")


if __name__ == "__main__":
    main()