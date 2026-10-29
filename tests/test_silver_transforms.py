from datetime import datetime
from decimal import Decimal

import pytest

from jobs.transforms import (
    SILVER_SCHEMA,
    add_trip_id,
    cast_types,
    clean_zones,
    flag_duplicates,
    flag_quality_issues,
    standardize_columns,
    unexpected_columns,
)
from tests.helpers import make_trips

MONTH = "2025-01"

# Each override breaks exactly ONE rule, so each rule is tested in isolation.
RULE_CASES = {
    "NULL_TIMESTAMP": {"pickup_ts": None},
    "NULL_LOCATION": {"pu_location_id": None},
    "NULL_AMOUNT": {"total_amount": None},
    "DROPOFF_BEFORE_PICKUP": {"dropoff_ts": datetime(2025, 1, 15, 9, 0, 0)},
    "TRIP_TOO_LONG": {"dropoff_ts": datetime(2025, 1, 16, 11, 30, 0)},  # 25.5 hours
    "PICKUP_OUTSIDE_MONTH": {
        "pickup_ts": datetime(2024, 12, 15, 10, 0, 0),
        "dropoff_ts": datetime(2024, 12, 15, 10, 30, 0),
    },
    "NON_POSITIVE_FARE": {"fare_amount": Decimal("0.00")},
    "NEGATIVE_TOTAL": {"total_amount": Decimal("-1.00")},
    "NON_POSITIVE_DISTANCE": {"trip_distance": 0.0},
    "DISTANCE_OUTLIER": {"trip_distance": 250.0},
    "INVALID_PASSENGER_COUNT": {"passenger_count": 9},
}


def reasons(df) -> list[str]:
    return [row["reject_reasons"] for row in df.select("reject_reasons").collect()]


# ---------- quality rules ----------


def test_clean_trip_has_no_reject_reasons(spark):
    assert reasons(flag_quality_issues(make_trips(spark, {}), MONTH)) == [""]


@pytest.mark.parametrize("code, override", RULE_CASES.items())
def test_each_rule_fires_on_its_own(spark, code, override):
    flagged = flag_quality_issues(make_trips(spark, override), MONTH)
    assert reasons(flagged) == [code]


def test_unknown_passenger_counts_are_kept(spark):
    df = make_trips(spark, {"passenger_count": None}, {"passenger_count": 0})
    assert reasons(flag_quality_issues(df, MONTH)) == ["", ""]


def test_a_row_can_break_several_rules(spark):
    df = make_trips(spark, {"fare_amount": Decimal("-5.00"), "trip_distance": 0.0})
    codes = reasons(flag_quality_issues(df, MONTH))[0].split("|")
    assert set(codes) == {"NON_POSITIVE_FARE", "NON_POSITIVE_DISTANCE"}


def test_unknown_zone_ids_are_rejected(spark):
    zones = [161, 236]
    df = make_trips(spark, {"pu_location_id": 999}, {"do_location_id": 999}, {})
    assert reasons(flag_quality_issues(df, MONTH, zones)) == [
        "INVALID_PICKUP_ZONE",
        "INVALID_DROPOFF_ZONE",
        "",
    ]


def test_missing_zone_is_reported_once_not_as_invalid_zone(spark):
    df = make_trips(spark, {"pu_location_id": None})
    assert reasons(flag_quality_issues(df, MONTH, [161, 236])) == ["NULL_LOCATION"]


# ---------- standardizing and typing ----------


def test_standardize_columns_renames_and_lowercases(spark):
    raw = spark.createDataFrame(
        [(1, "2025-01-15 10:00:00", 1.25, 161, "x")],
        ["VendorID", "tpep_pickup_datetime", "Airport_fee", "PULocationID", "Extra_Col"],
    )
    assert set(standardize_columns(raw).columns) == {
        "vendor_id",
        "pickup_ts",
        "airport_fee",
        "pu_location_id",
        "extra_col",
    }


def test_cast_types_enforces_the_contract(spark):
    raw = spark.createDataFrame(
        [("1", "abc", "2025-01-15 10:00:00", "junk")],
        ["VendorID", "passenger_count", "tpep_pickup_datetime", "junk"],
    )
    typed = cast_types(standardize_columns(raw))
    row = typed.first()

    assert typed.columns == list(SILVER_SCHEMA)  # exact columns, exact order
    assert "junk" not in typed.columns  # unknown columns are dropped
    assert dict(typed.dtypes)["fare_amount"] == "decimal(10,2)"
    assert row["vendor_id"] == 1
    assert row["passenger_count"] is None  # "abc" becomes NULL instead of crashing the job
    assert row["pickup_ts"] is not None
    assert row["total_amount"] is None  # missing source column becomes NULL


def test_unexpected_columns_lists_what_the_contract_does_not_know(spark):
    df = spark.createDataFrame([(1, "x", "f")], ["vendor_id", "junk", "source_file"])
    assert unexpected_columns(df) == ["junk"]


# ---------- trip_id and duplicates ----------


def test_trip_id_is_deterministic_and_64_characters(spark):
    ids = [r["trip_id"] for r in add_trip_id(make_trips(spark, {}, {})).collect()]
    assert ids[0] == ids[1]
    assert len(ids[0]) == 64


def test_trip_id_changes_when_any_column_changes(spark):
    df = add_trip_id(make_trips(spark, {}, {"fare_amount": Decimal("16.00")}))
    ids = [r["trip_id"] for r in df.collect()]
    assert ids[0] != ids[1]


def test_trip_id_distinguishes_null_from_zero(spark):
    df = add_trip_id(make_trips(spark, {"passenger_count": None}, {"passenger_count": 0}))
    ids = [r["trip_id"] for r in df.collect()]
    assert ids[0] != ids[1]


def test_flag_duplicates_keeps_exactly_one_copy(spark):
    rows = [
        ("a", ""),
        ("b", ""),
        ("b", ""),
        ("c", "NON_POSITIVE_FARE"),
        ("c", "NON_POSITIVE_FARE"),
    ]
    df = spark.createDataFrame(rows, ["trip_id", "reject_reasons"])

    by_id: dict[str, list[str]] = {}
    for row in flag_duplicates(df).collect():
        by_id.setdefault(row["trip_id"], []).append(row["reject_reasons"])

    assert by_id["a"] == [""]
    assert sorted(by_id["b"]) == ["", "DUPLICATE_TRIP"]
    assert sorted(by_id["c"]) == ["NON_POSITIVE_FARE", "NON_POSITIVE_FARE|DUPLICATE_TRIP"]


# ---------- zone lookup ----------


def test_clean_zones_standardizes_trims_and_types(spark):
    raw = spark.createDataFrame(
        [
            ("1", " EWR ", "Newark Airport", "EWR", "zones.csv", "t"),
            ("abc", "Queens", "Bad Row", "Boro Zone", "zones.csv", "t"),
        ],
        ["LocationID", "Borough", "Zone", "service_zone", "source_file", "ingested_at"],
    )
    rows = {r["zone"]: r for r in clean_zones(raw).collect()}

    assert rows["Newark Airport"]["location_id"] == 1
    assert rows["Newark Airport"]["borough"] == "EWR"
    assert rows["Bad Row"]["location_id"] is None