# Spark Exploration Notes (Day 2)

## Environment
- Spark version, Java version (from the notebook: spark.version)
- Run via docker compose, local[*] mode

## Row counts: Spark vs pandas
| File | pandas (Day 1) | Spark | Match? |
|---|---|---|---|
| 2025-01 | ... | ... | ... |

## Schema drift between months
- Identical across months? If not, what changed?

## Type observations
- How are the pickup/dropoff timestamps typed in Spark?
- Any columns with surprising types (for example, counts stored as double)?

## Lazy evaluation
- What did you see in the Spark UI after calling count()?

## Decisions for Bronze and Silver
- Types I'll cast explicitly in Silver:
- Columns I may need to rename or drop: