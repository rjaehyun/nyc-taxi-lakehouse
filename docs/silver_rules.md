# Silver Layer Rules

## Contract
- Columns snake_case, types enforced, missing source columns become NULL
- Money: decimal(10,2). Timestamps: UTC session, wall-clock values unshifted.
- Lineage kept: source_file, ingested_at, source_month. Derived: pickup_date.

## Quality rules
| Code | Condition | Reasoning | % of rows (Jan) |
|---|---|---|---|
| NULL_TIMESTAMP | pickup or dropoff missing | Trip can't be placed in time | ... |
| NULL_LOCATION | pickup or dropoff zone missing | Can't be joined to a zone | ... |
| NULL_AMOUNT | fare, total, or distance missing | Can't be analyzed | ... |
| DROPOFF_BEFORE_PICKUP | dropoff earlier than pickup | Impossible | ... |
| TRIP_TOO_LONG | longer than 24 hours | Almost certainly a meter error | ... |
| PICKUP_OUTSIDE_MONTH | pickup not in the file's month | Stray records | ... |
| NON_POSITIVE_FARE | fare <= 0 | ... | ... |
| NEGATIVE_TOTAL | total < 0 | Refunds or disputes, excluded from analysis | ... |
| NON_POSITIVE_DISTANCE | distance <= 0 | ... | ... |
| DISTANCE_OUTLIER | over 200 miles | ... | ... |
| INVALID_PASSENGER_COUNT | below 0 or above 8 | NULL and 0 kept as unknown | ... |

## Decisions I made and why
- Passenger count NULL/0: kept as unknown, because ...
- Thresholds (24h, 200 miles, 8 passengers): chosen because ...

## Reconciliation
Each month: rows in = silver + quarantine (enforced by the job).

## Added

| Code | Condition | Reasoning | % of rows (Jan) |
|---|---|---|---|
| INVALID_PICKUP_ZONE | pickup zone id not in zone lookup | Can't be joined to a zone | ... |
| INVALID_DROPOFF_ZONE | dropoff zone id not in zone lookup | Same | ... |
| DUPLICATE_TRIP | same trip_id as an earlier row | Same trip recorded twice | ... |

## trip_id
SHA-256 of all 20 business columns (NULL marked explicitly).
Assumption: two rows identical in every column are one trip recorded twice.
Risk: two genuinely different trips identical in every column would be merged.
That is very unlikely given timestamps to the second plus zones and amounts.

## Zone IDs 264 and 265
They exist in the lookup ("Unknown" and outside NYC), so they pass validation
and are kept. Gold can group them as "Unknown".

## Quality gate
quality_report --strict fails if any month rejects more than 5% of its rows.