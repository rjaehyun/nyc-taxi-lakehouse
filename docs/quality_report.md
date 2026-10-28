# Data Quality Report

Generated: 2026-10-28  |  Threshold: 5% rejected per month

## Per month

| Month | Rows in | Silver | Quarantined | Reject rate | Status |
|---|---:|---:|---:|---:|---|
| 2025-01 | 3,475,226 | 3,253,603 | 221,623 | 6.38% | OVER THRESHOLD |
| 2025-02 | 3,577,543 | 3,310,554 | 266,989 | 7.46% | OVER THRESHOLD |
| 2025-03 | 4,145,257 | 3,848,404 | 296,853 | 7.16% | OVER THRESHOLD |

## Rejections by rule

A row that breaks several rules is counted under each one.

| Rule | 2025-01 | 2025-02 | 2025-03 |
|---|---:|---:|---:|
| DISTANCE_OUTLIER | 122 | 151 | 179 |
| DROPOFF_BEFORE_PICKUP | 124 | 93 | 81 |
| INVALID_PASSENGER_COUNT | 3 | 3 | 2 |
| NEGATIVE_TOTAL | 63,037 | 55,179 | 68,686 |
| NON_POSITIVE_DISTANCE | 90,893 | 99,771 | 103,722 |
| NON_POSITIVE_FARE | 145,516 | 184,096 | 210,378 |
| PICKUP_OUTSIDE_MONTH | 22 | 31 | 33 |
| TRIP_TOO_LONG | 20 | 16 | 11 |
