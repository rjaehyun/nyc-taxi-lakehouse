# Development Notes

## Problems I hit and how I fixed them

### 1. ANSI mode in Spark 4 turns bad casts into errors
Spark 4 defaults to ANSI mode, where an invalid cast raises an error. Our quality
rules expect a bad cast to become NULL so the row can be quarantined.
**Fix:** `spark.sql.ansi.enabled=false` in the session builder, and a unit test
that checks "abc" becomes NULL.

### 2. Partition columns starting with an underscore are hidden by Spark
**Fix:** named the partition column `source_month`, not `_source_month`.

### 3. Join condition referencing a second read of the same file
Calling a loader function inside a join condition created a separate DataFrame,
and Spark could not resolve the column ("zone_key missing").
**Fix:** read the dimension once into a variable and reference it by name.

### 4. Partition overwrite does not clear a partition when the new data is empty
Spark writes nothing for an empty DataFrame, so a stale partition could survive.
**Fix:** delete the partition folder before writing.

### 5. Timezones in tests
Python converts naive datetimes using its own timezone. **Fix:** pin TZ=UTC in the
test setup.