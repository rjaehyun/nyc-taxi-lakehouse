# Data Quality Findings (Raw TLC Yellow Taxi Data)

Files profiled: 2025-01, 2025-02, 2025-03

## Volume
- Rows per month: ...
- Columns: ...

## Issues found
| Issue | Count (Jan) | % of rows | Planned handling in Silver |
|---|---|---|---|
| Pickup outside the file's month | ... | ... | Quarantine |
| Dropoff before pickup | ... | ... | Quarantine |
| fare_amount <= 0 | ... | ... | Quarantine |
| trip_distance <= 0 | ... | ... | Quarantine |
| passenger_count == 0 or null | ... | ... | Decide: keep as unknown or quarantine |
| Exact duplicates | ... | ... | Drop |

## Observations
- Anything surprising: columns that differ between months, odd outliers, nulls.