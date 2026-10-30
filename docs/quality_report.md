# Data Quality Report

Generated: 2026-10-30  |  Threshold: 10% rejected per month

## Per month

| Month | Rows in | Silver | Quarantined | Reject rate | Status |
|---|---:|---:|---:|---:|---|
| 2025-01 | 3,475,226 | 3,330,520 | 144,706 | 4.16% | OK |
| 2025-02 | 3,577,543 | 3,394,232 | 183,311 | 5.12% | OK |
| 2025-03 | 4,145,257 | 3,935,853 | 209,404 | 5.05% | OK |

## Rejections by rule

A row that breaks several rules is counted under each one.

| Rule | 2025-01 | 2025-02 | 2025-03 |
|---|---:|---:|---:|
| DISTANCE_OUTLIER | 122 | 151 | 179 |
| DROPOFF_BEFORE_PICKUP | 124 | 93 | 81 |
| INVALID_PASSENGER_COUNT | 3 | 3 | 2 |
| NEGATIVE_TOTAL | 63,037 | 55,179 | 68,686 |
| NON_POSITIVE_FARE | 144,118 | 182,655 | 208,724 |
| PICKUP_OUTSIDE_MONTH | 22 | 31 | 33 |
| TRIP_TOO_LONG | 20 | 16 | 11 |
