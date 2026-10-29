from datetime import date, datetime
from decimal import Decimal

import pytest

from jobs.gold_transforms import (
    UNKNOWN_KEY,
    build_agg_daily,
    build_dim_date,
    build_fact_trips,
    build_lookup_dim,
)
from tests.helpers import make_silver


def fact_row(spark, override: dict):
    return build_fact_trips(make_silver(spark, override)).first()


# ---------- dimensions ----------


def test_dim_date_has_one_row_per_day_with_iso_weekdays(spark):
    rows = {r["date"]: r for r in build_dim_date(spark, date(2025, 1, 1), date(2025, 1, 7)).collect()}

    assert len(rows) == 7
    assert rows[date(2025, 1, 1)]["date_key"] == 20250101

    saturday, sunday, monday = rows[date(2025, 1, 4)], rows[date(2025, 1, 5)], rows[date(2025, 1, 6)]
    assert (saturday["day_of_week"], saturday["is_weekend"]) == (6, True)
    assert (sunday["day_of_week"], sunday["is_weekend"]) == (7, True)
    assert (monday["day_of_week"], monday["is_weekend"]) == (1, False)


def test_lookup_dimension_includes_the_unknown_member(spark):
    dim = build_lookup_dim(spark, {1: "A", 2: "B"}, "code_key", "code_name")
    rows = {r["code_key"]: r["code_name"] for r in dim.collect()}

    assert rows == {1: "A", 2: "B", UNKNOWN_KEY: "Missing or unmapped code"}


# ---------- fact table ----------


def test_known_payment_type_keeps_its_key(spark):
    assert fact_row(spark, {"payment_type": 1})["payment_type_key"] == 1


def test_unmapped_codes_point_to_the_unknown_member(spark):
    row = fact_row(spark, {"payment_type": 42, "ratecode_id": 77})
    assert row["payment_type_key"] == UNKNOWN_KEY
    assert row["rate_code_key"] == UNKNOWN_KEY


def test_dimension_keys_and_pickup_hour(spark):
    row = fact_row(spark, {})
    assert row["pickup_date_key"] == 20250115
    assert row["pickup_hour"] == 10
    assert row["pu_zone_key"] == 161
    assert row["do_zone_key"] == 236


def test_tip_pct_is_only_calculated_for_card_trips(spark):
    card = fact_row(spark, {"payment_type": 1, "tip_amount": Decimal("3.00")})
    cash = fact_row(spark, {"payment_type": 2, "tip_amount": Decimal("0.00")})

    assert card["tip_pct"] == pytest.approx(0.20)  # 3.00 tip on a 15.00 fare
    assert cash["tip_pct"] is None  # cash tips are not recorded, so no percentage


def test_duration_and_speed(spark):
    row = fact_row(spark, {})  # 2.5 miles in 30 minutes
    assert row["trip_duration_min"] == pytest.approx(30.0)
    assert row["avg_speed_mph"] == pytest.approx(5.0)


def test_speed_is_null_for_trips_under_one_minute(spark):
    row = fact_row(spark, {"dropoff_ts": datetime(2025, 1, 15, 10, 0, 30)})  # 30 seconds
    assert row["trip_duration_min"] == pytest.approx(0.5)
    assert row["avg_speed_mph"] is None


# ---------- aggregates ----------


def test_agg_daily_totals(spark):
    silver = make_silver(
        spark,
        {"payment_type": 1},  # card trip: total 22.50, tip 20% of fare
        {"payment_type": 2, "tip_amount": Decimal("0.00"), "total_amount": Decimal("19.50")},
    )
    row = build_agg_daily(build_fact_trips(silver)).first()

    assert row["trips"] == 2
    assert row["total_revenue"] == Decimal("42.00")
    assert row["avg_card_tip_pct"] == 20.0  # the cash trip is ignored, not counted as 0%
    assert row["pickup_date"] == date(2025, 1, 15)