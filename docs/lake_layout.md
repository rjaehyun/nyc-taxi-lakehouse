# Lake Layout

```
data/lake/
├── bronze/   raw data + lineage metadata, nothing cleaned
├── silver/   cleaned, typed, validated  (Day 4-5)
├── gold/     star schema and aggregates (Day 6)
└── quarantine/  rejected rows with reasons (Day 4)
```

## Bronze contract

| Table | Source | Grain | Partitioned by |
|---|---|---|---|
| yellow_trips | TLC monthly Parquet | one row per trip, as published | source_month |
| taxi_zones | TLC zone lookup CSV | one row per zone | none |

Added columns: `source_file`, `ingested_at`, and `source_month` (trips only).

## Rules
- Values and column names are exactly as published.
- Reruns replace only the month being loaded, so they never create duplicates.
- Row counts are reconciled after each write.

## Observations from my run
- Part files per month partition: ...
- Size on disk per month vs the source Parquet file: ...
- Anything that differed between months: ...