# PySpark DataFrame Cheat Sheet

## ACME Fintech Coding Interview Reference

**How to use:** Run each section in order in a Databricks notebook. Every DataFrame is created before it is referenced. No external data or files are required.

---

## Setup & Imports

```python
from pyspark.sql import SparkSession                # Entry point for DataFrame and SQL operations
from pyspark.sql import functions as F              # Built-in functions (col, when, sum, etc.)
from pyspark.sql.window import Window               # For defining window specifications
from pyspark.sql.types import (                     # Data type classes for schema definition
    StructType, StructField, StringType, IntegerType,
    LongType, DoubleType, FloatType, DateType,
    TimestampType, BooleanType, ArrayType, MapType
)

# Create a SparkSession — the main entry point to all Spark functionality
# .builder starts the configuration chain
# .appName sets the name visible in the Spark UI
# .getOrCreate() returns an existing session or creates a new one
# NOTE: In Databricks, 'spark' already exists — this line is safe to run anyway
spark = SparkSession.builder \
    .appName("ACMEInsurance") \
    .getOrCreate()
```

---

## 1. Creating & Populating DataFrames

### From a list of tuples (most common in interviews)

```python
# Define sample data as a list of tuples — each tuple is one row
data = [
    ("POL001", "C100", "FL", 1250.00, "active",  "2024-01-15", "2025-01-15"),
    ("POL002", "C101", "TX", 980.50,  "active",  "2024-03-01", "2025-03-01"),
    ("POL003", "C102", "FL", 1500.00, "expired", "2023-06-15", "2024-06-15"),
    ("POL004", "C103", "LA", 2100.75, "active",  "2024-07-01", "2025-07-01"),
    ("POL005", "C100", "FL", 1300.00, "active",  "2024-08-01", "2025-08-01"),
]

# createDataFrame takes the data and a list of column names
# Spark infers the data types automatically from the Python types
df = spark.createDataFrame(
    data,
    ["policy_id", "customer_id", "state", "premium", "status",
     "effective_date", "expiration_date"]
)

# Cast the string date columns to proper DateType for date operations later
df = df.withColumn("effective_date", F.col("effective_date").cast(DateType())) \
       .withColumn("expiration_date", F.col("expiration_date").cast(DateType()))

df.show()
```

### With an explicit schema

```python
# StructType defines the overall schema as a list of StructField objects
# Each StructField takes: column name, data type, and nullable (True/False)
# False for nullable means the column cannot contain null values
schema = StructType([
    StructField("policy_id",      StringType(),  False),   # Non-nullable string
    StructField("customer_id",    StringType(),  False),   # Non-nullable string
    StructField("state",          StringType(),  True),    # Nullable string
    StructField("premium",        DoubleType(),  True),    # Nullable double
    StructField("status",         StringType(),  True),    # Nullable string
    StructField("effective_date", StringType(),  True),    # Nullable string (cast later)
    StructField("expiration_date",StringType(),  True),    # Nullable string (cast later)
])

# Pass the schema as the second argument instead of column name strings
# This gives you exact control over types and nullability
df_schema = spark.createDataFrame(data, schema)
df_schema.printSchema()
```

### From a Pandas DataFrame

```python
import pandas as pd

# Create a regular Pandas DataFrame
pandas_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})

# Convert it to a PySpark DataFrame — Spark infers schema from Pandas dtypes
df_from_pandas = spark.createDataFrame(pandas_df)
df_from_pandas.show()
```

---

## 2. Inspecting the DataFrame

```python
df.show()                        # Display the first 20 rows in a formatted table
df.show(3, truncate=False)       # Show 3 rows with full column width (no truncation)

df.printSchema()                 # Print the schema as a tree showing column names and types
print(df.dtypes)                 # Print a list of (column_name, type_string) tuples
print(df.columns)                # Print a plain list of column name strings
print(df.count())                # Print the total row count (triggers a full scan — expensive)

df.describe().show()             # Show summary statistics: count, mean, stddev, min, max
df.summary().show()              # Extended stats including 25th/50th/75th percentiles
```

---

## 3. Selecting Columns

```python
# Select columns by name as strings — simplest syntax
df.select("policy_id", "premium").show()

# Select using F.col() — returns Column objects, needed for chaining operations
df.select(F.col("policy_id"), F.col("premium")).show()

# Rename a column during selection using .alias()
df.select(F.col("policy_id").alias("pol_id"), "premium").show()

# Select all existing columns plus a new computed column
# "*" means all columns, then we add a calculated column alongside them
df.select("*", (F.col("premium") * 1.1).alias("premium_increase")).show()

# Select columns dynamically using a list comprehension
# This example selects only columns whose names contain "date"
df.select([c for c in df.columns if "date" in c]).show()
```

---

## 4. Adding, Renaming & Dropping Columns

```python
# withColumn adds a new column or overwrites an existing one
# Here we create "premium_tax" as 6% of premium
df_modified = df.withColumn("premium_tax", F.col("premium") * 0.06)

# F.lit() creates a column with a constant/literal value for every row
df_modified = df_modified.withColumn("country", F.lit("US"))

# current_timestamp() returns the current date and time for every row
df_modified = df_modified.withColumn("processed_at", F.current_timestamp())

df_modified.show(truncate=False)

# withColumnRenamed changes a column's name without altering its data
df_renamed = df.withColumnRenamed("premium", "annual_premium")
df_renamed.show()

# drop removes one or more columns from the DataFrame
df_dropped = df.drop("customer_id", "status")
df_dropped.show()
```

---

## 5. Filtering Rows

```python
# Keep only rows where state equals "FL"
df.filter(F.col("state") == "FL").show()

# Keep rows where premium is greater than 1000
df.filter(F.col("premium") > 1000).show()

# Keep rows where status is anything other than "expired"
df.filter(F.col("status") != "expired").show()

# AND condition — use & and wrap each condition in parentheses
# Both conditions must be true for the row to pass
df.filter((F.col("state") == "FL") & (F.col("premium") > 1000)).show()

# OR condition — use | for logical OR
# Either condition being true keeps the row
df.filter((F.col("state") == "FL") | (F.col("state") == "LA")).show()

# isin checks if the value is in a specified list — cleaner than multiple ORs
df.filter(F.col("state").isin("FL", "TX", "LA")).show()

# NOT IN — use ~ (tilde) to negate the isin condition
df.filter(~F.col("state").isin("FL", "TX")).show()

# startswith checks if the string value begins with the given prefix
df.filter(F.col("policy_id").startswith("POL")).show()

# contains checks if the string includes the substring anywhere
df.filter(F.col("policy_id").contains("001")).show()

# like uses SQL-style wildcards: % = any characters, _ = single character
df.filter(F.col("policy_id").like("POL%")).show()

# rlike uses a Java-style regular expression for pattern matching
# ^POL[0-9]{3}$ means: starts with POL, then exactly 3 digits, then end
df.filter(F.col("policy_id").rlike("^POL[0-9]{3}$")).show()

# between is inclusive on both ends — keeps values >= 1000 AND <= 2000
df.filter(F.col("premium").between(1000, 2000)).show()

# Date comparison — string dates are automatically cast for comparison
df.filter(F.col("effective_date") >= "2024-01-01").show()

# between works on dates too — inclusive range filter
df.filter(F.col("effective_date").between("2024-01-01", "2024-12-31")).show()
```

---

## 6. Sorting

```python
# Sort ascending by premium (ascending is the default)
df.orderBy("premium").show()

# Sort descending — call .desc() on the column object
df.orderBy(F.col("premium").desc()).show()

# Multi-column sort — first by state ascending, then premium descending within each state
df.orderBy("state", F.col("premium").desc()).show()
```

---

## 7. Aggregations

### Basic groupBy + agg

```python
# groupBy splits the data into groups by the specified column(s)
# agg applies one or more aggregate functions to each group
# alias renames the resulting column
df.groupBy("state").agg(
    F.count("policy_id").alias("total_policies"),          # Count of rows per state
    F.sum("premium").alias("total_premium"),                # Sum of all premiums
    F.avg("premium").alias("avg_premium"),                  # Average premium
    F.min("premium").alias("min_premium"),                  # Smallest premium
    F.max("premium").alias("max_premium"),                  # Largest premium
    F.round(F.stddev("premium"), 2).alias("stddev_premium"),# Standard deviation rounded to 2 decimals
    F.countDistinct("customer_id").alias("unique_customers"),# Count of unique customers
).show()
```

### Conditional aggregation (very common in interviews)

```python
# Use F.when() inside aggregate functions to count or sum conditionally
# This avoids needing a separate filter step before aggregating
df.groupBy("state").agg(
    F.count("*").alias("total"),                            # Total rows per state

    # Count only active policies: when status is active return 1, else 0, then sum
    F.sum(F.when(F.col("status") == "active", 1).otherwise(0)).alias("active_count"),

    # Sum premium only for active policies — non-active rows contribute nothing (null)
    F.sum(F.when(F.col("status") == "active", F.col("premium"))
    ).alias("active_premium"),

    # Count distinct policy_ids but only where premium exceeds 2000
    # when() returns null for non-matching rows, and countDistinct ignores nulls
    F.countDistinct(
        F.when(F.col("premium") > 2000, F.col("policy_id"))
    ).alias("high_premium_policies"),
).show()
```

### Aggregate entire DataFrame (no groupBy)

```python
# Calling agg directly on the DataFrame without groupBy aggregates all rows into one
df.agg(
    F.sum("premium").alias("grand_total"),     # Single total across the entire DataFrame
    F.count("*").alias("row_count"),            # Total number of rows
).show()
```

### Collect aggregate values

```python
# collect_list gathers all values into an array, including duplicates
# collect_set gathers only unique values into an array
df.groupBy("state").agg(
    F.collect_list("policy_id").alias("policy_ids"),       # Array with possible duplicates
    F.collect_set("customer_id").alias("unique_customers"),# Array with unique values only
).show(truncate=False)
```

---

## 8. Joins

### Create the claims DataFrame to join against

```python
# Claims data — note CLM004 references POL999 which does not exist in df
claims = spark.createDataFrame([
    ("CLM001", "POL001", "2024-06-15", 5000.00, "approved"),
    ("CLM002", "POL001", "2024-09-20", 3200.00, "pending"),
    ("CLM003", "POL003", "2024-02-10", 8500.00, "approved"),
    ("CLM004", "POL999", "2024-04-05", 1200.00, "denied"),
    ("CLM005", "POL002", "2024-11-01", 4500.00, "approved"),
], ["claim_id", "policy_id", "claim_date", "claim_amount", "claim_status"]) \
    .withColumn("claim_date", F.col("claim_date").cast(DateType()))

claims.show()
```

### Create a small lookup DataFrame for broadcast join examples

```python
# Small state lookup table — used for broadcast join demonstration
state_lookup = spark.createDataFrame([
    ("FL", "Florida",  "Southeast"),
    ("TX", "Texas",    "Southwest"),
    ("LA", "Louisiana","Southeast"),
    ("CA", "California","West"),
], ["state", "state_name", "region"])

state_lookup.show()
```

### All join types

```python
# INNER join — returns only rows where policy_id exists in BOTH DataFrames
# Rows without a match on either side are dropped
print("--- INNER JOIN ---")
df.join(claims, "policy_id", "inner").show()

# LEFT join — returns ALL rows from df (left side), with matching claim data
# If a policy has no claims, the claim columns will be null
print("--- LEFT JOIN ---")
df.join(claims, "policy_id", "left").show()

# RIGHT join — returns ALL rows from claims (right side), with matching policy data
# If a claim references a policy not in df, the policy columns will be null
print("--- RIGHT JOIN ---")
df.join(claims, "policy_id", "right").show()

# FULL OUTER join — returns ALL rows from BOTH sides
# Nulls appear wherever there's no match on either side
print("--- FULL OUTER JOIN ---")
df.join(claims, "policy_id", "full").show()

# LEFT ANTI join — returns rows from df that have NO matching policy_id in claims
# Only columns from df are returned — useful for finding unmatched records
print("--- LEFT ANTI JOIN (policies with no claims) ---")
df.join(claims, "policy_id", "left_anti").show()

# LEFT SEMI join — returns rows from df that DO have a match in claims
# Like anti-join, only columns from df are returned (no claim columns)
print("--- LEFT SEMI JOIN (policies that have claims) ---")
df.join(claims, "policy_id", "left_semi").show()

# CROSS join — produces the cartesian product: every row in df paired with every row
# Result size = rows_in_df × rows_in_other — use with caution
# Showing just a count here to avoid a huge output
print("--- CROSS JOIN row count ---")
print(df.crossJoin(state_lookup).count())
```

### Multi-column join

```python
# Create a second DataFrame with overlapping column names to demonstrate multi-column join
policy_updates = spark.createDataFrame([
    ("POL001", "2024-01-15", 1350.00),
    ("POL002", "2024-03-01", 1050.00),
    ("POL003", "2023-06-15", 1500.00),
], ["policy_id", "effective_date", "new_premium"]) \
    .withColumn("effective_date", F.col("effective_date").cast(DateType()))

# Join on two conditions simultaneously using & (AND)
# Use the full DataFrame.column syntax to avoid ambiguity between same-named columns
df.join(policy_updates,
    (df.policy_id == policy_updates.policy_id) &
    (df.effective_date == policy_updates.effective_date),
    "inner"
).select(df.policy_id, df.effective_date, df.premium, policy_updates.new_premium).show()
```

### Broadcast join (small table optimization)

```python
# F.broadcast() tells Spark to send the small table to all executors
# Avoids an expensive shuffle — use when one side is small (roughly < 10MB)
df.join(F.broadcast(state_lookup), "state").show()
```

### Handle duplicate column names after join

```python
# Use .alias() to give each DataFrame a short reference name
# Then use "alias.column" syntax to disambiguate columns that exist in both
joined = df.alias("p").join(claims.alias("c"), "policy_id")
joined.select("p.policy_id", "p.state", "p.premium", "c.claim_amount", "c.claim_status").show()
```

---

## 9. Window Functions

### Create the claims history DataFrame used throughout this section

```python
claims_history = spark.createDataFrame([
    ("POL001", "2024-01-15", 5000.00, "wind"),
    ("POL001", "2024-06-20", 3200.00, "water"),
    ("POL001", "2024-09-10", 1500.00, "wind"),
    ("POL002", "2024-03-05", 8500.00, "fire"),
    ("POL002", "2024-07-15", 2200.00, "water"),
    ("POL003", "2024-02-28", 12000.00, "wind"),
    ("POL003", "2024-05-10", 4300.00, "wind"),
    ("POL003", "2024-08-22", 6700.00, "water"),
], ["policy_id", "claim_date", "claim_amount", "claim_type"]) \
    .withColumn("claim_date", F.col("claim_date").cast(DateType()))

claims_history.show()
```

### Define windows

```python
# Partition by a column and order within each partition
# This creates separate "windows" for each policy_id, sorted by claim_date
w = Window.partitionBy("policy_id").orderBy("claim_date")

# Add a frame spec — defines which rows relative to the current row are included
# unboundedPreceding = from the first row in the partition
# currentRow = up to and including the current row
# This is used for running totals and cumulative aggregates
w_running = Window.partitionBy("policy_id") \
    .orderBy("claim_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Partition-only window (no ordering) — used for group-level aggregates
# Every row in the partition sees the same aggregate value
w_partition = Window.partitionBy("policy_id")

# Empty partition — treats the entire DataFrame as one group
# Useful for computing grand totals that appear on every row
w_all = Window.partitionBy()
```

### Ranking functions

```python
# row_number: assigns 1, 2, 3, 4 — always unique, no ties
# If two rows have the same value, one arbitrarily gets a lower number
print("--- ROW_NUMBER ---")
claims_history.withColumn("rn", F.row_number().over(w)).show()

# rank: assigns 1, 2, 2, 4 — ties get the same rank, then it skips
# Two items tied at rank 2 means the next item is rank 4 (not 3)
print("--- RANK ---")
w_by_amount = Window.partitionBy("policy_id").orderBy("claim_amount")
claims_history.withColumn("rnk", F.rank().over(w_by_amount)).show()

# dense_rank: assigns 1, 2, 2, 3 — ties get the same rank, no gaps
# Two items tied at rank 2 means the next item is still rank 3
print("--- DENSE_RANK ---")
claims_history.withColumn("drnk", F.dense_rank().over(w_by_amount)).show()

# ntile: divides rows into N roughly equal-sized groups (buckets)
# ntile(2) splits each policy's claims into 2 halves
print("--- NTILE(2) ---")
claims_history.withColumn("half", F.ntile(2).over(w)).show()

# percent_rank: relative position as a value between 0.0 and 1.0
# 0.0 = first row, 1.0 = last row in the partition
print("--- PERCENT_RANK ---")
claims_history.withColumn("pct_rank", F.percent_rank().over(w)).show()
```

### Common pattern — get top N per group

```python
# Define window: partition by policy, order by claim_amount descending (highest first)
w_top = Window.partitionBy("policy_id").orderBy(F.col("claim_amount").desc())

# Assign row numbers within each policy, then keep only the top 2 claims per policy
# row_number guarantees unique numbering so you get exactly 2 rows per policy
print("--- TOP 2 CLAIMS PER POLICY ---")
claims_history.withColumn("rn", F.row_number().over(w_top)) \
    .filter(F.col("rn") <= 2) \
    .drop("rn") \
    .show()
```

### Common pattern — deduplicate (keep latest)

```python
# Define window: partition by the key, order by claim_date descending (newest first)
w_dedup = Window.partitionBy("policy_id").orderBy(F.col("claim_date").desc())

# Row number 1 within each policy_id is the most recent claim
# Filter to keep only that row, then drop the helper column
print("--- MOST RECENT CLAIM PER POLICY ---")
claims_history.withColumn("rn", F.row_number().over(w_dedup)) \
    .filter(F.col("rn") == 1) \
    .drop("rn") \
    .show()
```

### Lag and Lead

```python
# lag looks BACKWARD — gets the value from the previous row in the window
# lag(column, 1) means 1 row before the current row
print("--- LAG (previous claim amount) ---")
claims_history.withColumn(
    "prev_amount", F.lag("claim_amount", 1).over(w)
).show()

# lead looks FORWARD — gets the value from the next row in the window
# lead(column, 1) means 1 row after the current row
print("--- LEAD (next claim amount) ---")
claims_history.withColumn(
    "next_amount", F.lead("claim_amount", 1).over(w)
).show()

# Third argument is a default value when lag/lead returns null
# This happens for the first row (lag) or last row (lead) in each partition
print("--- LAG with default value of 0 ---")
claims_history.withColumn(
    "prev_amount", F.lag("claim_amount", 1, 0).over(w)
).show()
```

### Running & moving aggregates

```python
# Running total — cumulative sum that grows with each row
print("--- RUNNING TOTAL ---")
claims_history.withColumn(
    "running_total", F.sum("claim_amount").over(w_running)
).show()

# Running average — average of all values from start through current row
print("--- RUNNING AVERAGE ---")
claims_history.withColumn(
    "running_avg", F.round(F.avg("claim_amount").over(w_running), 2)
).show()

# Running count — how many rows from start through current row
print("--- RUNNING COUNT ---")
claims_history.withColumn(
    "running_count", F.count("*").over(w_running)
).show()

# Moving window: current row plus the 1 row before it (2-row window)
# -1 means 1 row before current, currentRow is 0
w_moving_2 = Window.partitionBy("policy_id").orderBy("claim_date") \
    .rowsBetween(-1, Window.currentRow)

# 2-period moving average — average of the current row and 1 prior row
print("--- 2-PERIOD MOVING AVERAGE ---")
claims_history.withColumn(
    "moving_avg_2", F.round(F.avg("claim_amount").over(w_moving_2), 2)
).show()
```

### Percentage of group total

```python
# Partition-only window (no orderBy) — every row in the policy sees the same sum
w_policy_total = Window.partitionBy("policy_id")

# Divide each row's claim_amount by the total claims for its policy
# Multiply by 100 to express as a percentage, round to 2 decimal places
print("--- PERCENTAGE OF POLICY TOTAL ---")
claims_history.withColumn("pct_of_policy_total",
    F.round(F.col("claim_amount") / F.sum("claim_amount").over(w_policy_total), 4) * 100
).show()
```

---

## 10. When / Otherwise (Conditional Logic)

```python
# F.when() works like an if-elif-else chain
# First matching condition wins — subsequent conditions are skipped
# .otherwise() provides the default value if no conditions match
print("--- SIMPLE WHEN/OTHERWISE ---")
df.withColumn("premium_tier",
    F.when(F.col("premium") > 2000, "HIGH")        # If premium > 2000 → HIGH
     .when(F.col("premium") > 1000, "MEDIUM")       # Else if premium > 1000 → MEDIUM
     .otherwise("LOW")                               # Else → LOW
).select("policy_id", "state", "premium", "premium_tier").show()

# Combine multiple column conditions using & (AND) and | (OR)
# Each compound condition must be wrapped in parentheses
print("--- MULTI-CONDITION WHEN ---")
df.withColumn("risk_level",
    F.when(
        (F.col("state").isin("FL", "LA")) & (F.col("premium") > 2000), "HIGH"
    ).when(
        F.col("state").isin("FL", "LA"), "MEDIUM"
    ).otherwise("LOW")
).select("policy_id", "state", "premium", "risk_level").show()

# A comparison expression directly returns a boolean column (True/False)
print("--- BOOLEAN FLAG ---")
df.withColumn("is_high_value", F.col("premium") > 2000) \
  .select("policy_id", "premium", "is_high_value").show()

# coalesce returns the first non-null value from the arguments
# If state is null, it falls back to the literal string "UNKNOWN"
# We create a row with a null state to demonstrate
print("--- COALESCE ---")
df_with_null = spark.createDataFrame([
    ("POL006", "C104", None, 900.00, "active", "2024-09-01", "2025-09-01")
], ["policy_id", "customer_id", "state", "premium", "status",
    "effective_date", "expiration_date"])

df_with_null.withColumn("state_clean",
    F.coalesce(F.col("state"), F.lit("UNKNOWN"))
).select("policy_id", "state", "state_clean").show()
```

---

## 11. Null Handling

### Create a DataFrame with null values for this section

```python
raw_data = spark.createDataFrame([
    ("POL001", "John Smith",  "FL",   1250.00),
    ("POL002", None,          "TX",   980.50),
    ("POL003", "Jane Doe",    None,   None),
    ("POL004", "Bob Wilson",  "LA",   -500.00),
    ("POL005", "Alice Brown", "FL",   1300.00),
    ("POL006", None,          None,   None),
], ["policy_id", "customer_name", "state", "premium"])

raw_data.show()
```

### Null operations

```python
# Keep only rows where premium IS null
print("--- ROWS WHERE PREMIUM IS NULL ---")
raw_data.filter(F.col("premium").isNull()).show()

# Keep only rows where premium is NOT null
print("--- ROWS WHERE PREMIUM IS NOT NULL ---")
raw_data.filter(F.col("premium").isNotNull()).show()

# Drop rows that have a null in ANY column
print("--- DROP ROWS WITH ANY NULL ---")
raw_data.na.drop().show()

# Drop rows that have a null only in the specified columns
print("--- DROP ROWS WITH NULL IN premium ---")
raw_data.na.drop(subset=["premium"]).show()

# Drop rows only if ALL columns are null (row is completely empty)
print("--- DROP ROWS WHERE ALL ARE NULL (no effect here) ---")
raw_data.na.drop(how="all").show()

# fillna replaces nulls with specified values — pass a dict of column→value
# Only affects the columns listed; other columns are untouched
print("--- FILLNA ---")
raw_data.fillna({"state": "UNKNOWN", "premium": 0.0, "customer_name": "N/A"}).show()

# Alternative syntax — fill 0 in only the premium column
print("--- NA.FILL SUBSET ---")
raw_data.na.fill(0, subset=["premium"]).show()

# coalesce picks the first non-null value across multiple columns
# Falls through customer_name → literal "Unknown"
print("--- COALESCE FALLBACK ---")
raw_data.withColumn("name_clean",
    F.coalesce("customer_name", F.lit("Unknown"))
).show()

# Count nulls per column — a common data quality check pattern
# F.when(isNull) returns the value (counted) or null (skipped by count)
print("--- NULL COUNTS PER COLUMN ---")
raw_data.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in raw_data.columns
]).show()

# Replace a specific value with null — useful for cleaning placeholder values
# If customer_name is "N/A", replace it with None (null); otherwise keep it
print("--- REPLACE VALUE WITH NULL ---")
raw_data.fillna({"customer_name": "N/A"}) \
    .withColumn("customer_name",
        F.when(F.col("customer_name") == "N/A", None)
         .otherwise(F.col("customer_name"))
    ).show()
```

---

## 12. Deduplication

### Create a DataFrame with duplicates

```python
dupes_data = spark.createDataFrame([
    ("POL001", "FL", 1250.00, "2024-01-15 08:00:00"),
    ("POL001", "FL", 1250.00, "2024-01-15 08:00:00"),  # exact duplicate
    ("POL001", "FL", 1300.00, "2024-06-01 10:00:00"),  # same policy, different values
    ("POL002", "TX", 980.50,  "2024-03-01 09:00:00"),
    ("POL002", "TX", 1050.00, "2024-07-15 11:00:00"),  # same policy, updated premium
], ["policy_id", "state", "premium", "updated_at"]) \
    .withColumn("updated_at", F.col("updated_at").cast(TimestampType()))

print("--- ORIGINAL DATA WITH DUPLICATES ---")
dupes_data.show()
```

### Deduplication methods

```python
# Remove rows that are exact duplicates across ALL columns
print("--- DROP EXACT DUPLICATES ---")
dupes_data.dropDuplicates().show()

# Remove duplicates based on specific columns only
# Keeps one arbitrary row per unique policy_id
print("--- DROP DUPLICATES BY policy_id (arbitrary row kept) ---")
dupes_data.dropDuplicates(["policy_id"]).show()

# Controlled deduplication — keep a SPECIFIC row per group (e.g., the most recent)
# Step 1: Assign row numbers within each policy_id, ordered by updated_at descending
# Step 2: Keep only row number 1 (the newest record)
# Step 3: Drop the helper column
print("--- DEDUPLICATE KEEPING MOST RECENT ---")
w_dedup = Window.partitionBy("policy_id").orderBy(F.col("updated_at").desc())
dupes_data.withColumn("rn", F.row_number().over(w_dedup)) \
    .filter(F.col("rn") == 1) \
    .drop("rn") \
    .show()
```

---

## 13. String Functions

### Create a DataFrame with string data

```python
string_data = spark.createDataFrame([
    ("POL001", "John Smith",   "123-45-6789", "Wind damage   to roof",  "42"),
    ("POL002", "jane doe",     "987-65-4321", "Water  leak in  basement","7"),
    ("POL003", "BOB WILSON",   "555-12-3456", "Fire in kitchen",         "123"),
], ["policy_id", "customer_name", "ssn", "notes", "code"])

string_data.show(truncate=False)
```

### String operations

```python
# Convert to lowercase — "BOB WILSON" becomes "bob wilson"
print("--- LOWER ---")
string_data.withColumn("name_lower", F.lower("customer_name")) \
    .select("customer_name", "name_lower").show()

# Convert to uppercase — "jane doe" becomes "JANE DOE"
print("--- UPPER ---")
string_data.withColumn("name_upper", F.upper("customer_name")) \
    .select("customer_name", "name_upper").show()

# Return the number of characters in the string
print("--- LENGTH ---")
string_data.withColumn("name_len", F.length("customer_name")) \
    .select("customer_name", "name_len").show()

# Extract a substring — starts at position 1 (1-indexed), takes 3 characters
# "POL001" → "POL"
print("--- SUBSTRING ---")
string_data.withColumn("prefix", F.substring("policy_id", 1, 3)) \
    .select("policy_id", "prefix").show()

# Replace all occurrences matching a regex — collapses multiple spaces into one
print("--- REGEXP_REPLACE ---")
string_data.withColumn("notes_clean", F.regexp_replace("notes", r"\s+", " ")) \
    .select("notes", "notes_clean").show(truncate=False)

# Extract the first matching group from a regex — pulls out the digits
# "POL001" with pattern (\d+) and group 1 → "001"
print("--- REGEXP_EXTRACT ---")
string_data.withColumn("digits", F.regexp_extract("policy_id", r"(\d+)", 1)) \
    .select("policy_id", "digits").show()

# Split a string by a delimiter — returns an array of strings
# "John Smith" split by " " → ["John", "Smith"]
print("--- SPLIT ---")
string_data.withColumn("name_parts", F.split("customer_name", " ")) \
    .select("customer_name", "name_parts").show(truncate=False)

# concat_ws joins multiple columns with a separator string
print("--- CONCAT_WS ---")
string_data.withColumn("combined", F.concat_ws(" | ", "policy_id", "customer_name")) \
    .select("combined").show(truncate=False)

# Left-pad a string to a specified length — "42" becomes "00042", "7" becomes "00007"
print("--- LPAD ---")
string_data.withColumn("padded", F.lpad("code", 5, "0")) \
    .select("code", "padded").show()

# Mask a SSN by replacing all but the last 4 digits with asterisks
# "123-45-6789" → "***-**-6789"
print("--- SSN MASKING ---")
string_data.withColumn("ssn_masked",
    F.concat(F.lit("***-**-"), F.substring("ssn", -4, 4))
).select("ssn", "ssn_masked").show()
```

---

## 14. Date & Timestamp Functions

```python
# Return today's date as a DateType column — same value for every row
print("--- CURRENT_DATE and CURRENT_TIMESTAMP ---")
df.select("policy_id",
    F.current_date().alias("today"),
    F.current_timestamp().alias("now")
).show(2, truncate=False)

# Extract individual date components from a date column
print("--- DATE PARTS ---")
df.select("policy_id", "effective_date",
    F.year("effective_date").alias("year"),            # 2024
    F.month("effective_date").alias("month"),          # 1 (January)
    F.dayofmonth("effective_date").alias("day"),       # 15
    F.dayofweek("effective_date").alias("dow"),        # 1=Sunday through 7=Saturday
    F.quarter("effective_date").alias("quarter"),      # 1 through 4
).show()

# datediff returns the number of days between two dates (end - start)
print("--- DATEDIFF ---")
df.select("policy_id",
    F.datediff("expiration_date", "effective_date").alias("policy_days")
).show()

# months_between returns fractional months between two dates
print("--- MONTHS_BETWEEN ---")
df.select("policy_id",
    F.round(F.months_between("expiration_date", "effective_date"), 1).alias("months")
).show()

# Date arithmetic — add and subtract days and months
print("--- DATE ARITHMETIC ---")
df.select("policy_id", "effective_date",
    F.date_add("effective_date", 30).alias("plus_30_days"),     # Add 30 calendar days
    F.date_sub("effective_date", 30).alias("minus_30_days"),    # Subtract 30 calendar days
    F.add_months("effective_date", 3).alias("plus_3_months"),   # Add 3 calendar months
).show()

# Truncate a date to the start of the specified period
# "2024-06-15" truncated to month → "2024-06-01"
print("--- DATE_TRUNC ---")
df.select("policy_id", "effective_date",
    F.date_trunc("month", "effective_date").alias("month_start"),
    F.date_trunc("year", "effective_date").alias("year_start"),
).show()

# Format a date as a string with a custom pattern
# date object → "01/15/2024"
print("--- DATE_FORMAT ---")
df.select("policy_id",
    F.date_format("effective_date", "MM/dd/yyyy").alias("formatted")
).show()

# Return the last day of the month for a given date
print("--- LAST_DAY ---")
df.select("policy_id", "effective_date",
    F.last_day("effective_date").alias("month_end")
).show()
```

---

## 15. Pivot & Unpivot

### Create monthly premium data

```python
monthly_data = spark.createDataFrame([
    ("POL001", "2024-01", 104.17),
    ("POL001", "2024-02", 104.17),
    ("POL001", "2024-03", 104.17),
    ("POL002", "2024-01", 81.71),
    ("POL002", "2024-02", 81.71),
    ("POL002", "2024-03", 0.00),
], ["policy_id", "month", "premium_collected"])

monthly_data.show()
```

### Pivot (long to wide)

```python
# Pivot transforms unique values in the "month" column into separate columns
# groupBy defines the row keys, pivot defines the new columns, agg fills the values
# Result: one row per policy_id with a column for each month
print("--- PIVOT (long to wide) ---")
pivoted = monthly_data.groupBy("policy_id") \
    .pivot("month", ["2024-01", "2024-02", "2024-03"]) \
    .agg(F.first("premium_collected"))

pivoted.show()
```

### Unpivot (wide to long) using stack()

```python
# stack() converts columns back into rows — the inverse of pivot
# First argument is the number of column pairs to unpivot (3 months = 3)
# Each pair is: literal key value, then the column reference
# Backticks are needed because column names contain special characters (hyphens)
# Result: each policy_id gets 3 rows, one per month
print("--- UNPIVOT (wide to long) ---")
unpivoted = pivoted.selectExpr(
    "policy_id",
    """stack(3,
        '2024-01', `2024-01`,
        '2024-02', `2024-02`,
        '2024-03', `2024-03`
    ) as (month, premium_collected)"""
)

unpivoted.show()
```

---

## 16. Complex / Nested Data Types

### Create a DataFrame with arrays, structs, and maps

```python
from pyspark.sql.types import ArrayType, MapType

# Build a DataFrame with nested types using JSON-style creation
complex_data = spark.createDataFrame([
    ("POL001", [("dwelling", 250000.0, 1000.0), ("liability", 100000.0, 500.0)],
     {"wind_mitigation": "yes", "alarm_system": "yes"}),
    ("POL002", [("dwelling", 180000.0, 2000.0)],
     {"wind_mitigation": "no"}),
    ("POL003", [("dwelling", 300000.0, 1500.0), ("liability", 200000.0, 1000.0), ("auto", 50000.0, 500.0)],
     {"wind_mitigation": "yes", "flood_zone": "A"}),
], ["policy_id", "coverages", "endorsements"])

# The coverages column is an array of tuples — let's define it properly with schema
complex_schema = StructType([
    StructField("policy_id", StringType()),
    StructField("coverages", ArrayType(StructType([
        StructField("type", StringType()),
        StructField("limit", DoubleType()),
        StructField("deductible", DoubleType()),
    ]))),
    StructField("endorsements", MapType(StringType(), StringType())),
])

complex_rows = [
    ("POL001", [("dwelling", 250000.0, 1000.0), ("liability", 100000.0, 500.0)],
     {"wind_mitigation": "yes", "alarm_system": "yes"}),
    ("POL002", [("dwelling", 180000.0, 2000.0)],
     {"wind_mitigation": "no"}),
    ("POL003", [("dwelling", 300000.0, 1500.0), ("liability", 200000.0, 1000.0), ("auto", 50000.0, 500.0)],
     {"wind_mitigation": "yes", "flood_zone": "A"}),
]

complex_df = spark.createDataFrame(complex_rows, complex_schema)
complex_df.printSchema()
complex_df.show(truncate=False)
```

### Arrays

```python
# explode takes each element of an array and creates a separate row for it
# A row with 3 coverages becomes 3 rows, one per coverage
print("--- EXPLODE ---")
exploded = complex_df.withColumn("coverage", F.explode("coverages")) \
    .select("policy_id", "coverage.*")
exploded.show()

# size returns the number of elements in the array — [a, b, c] → 3
print("--- ARRAY SIZE ---")
complex_df.select("policy_id", F.size("coverages").alias("num_coverages")).show()

# Access an array element by index — arrays are 0-based
# [0] gets the first element
print("--- FIRST ELEMENT ---")
complex_df.select("policy_id", F.col("coverages")[0].alias("first_coverage")).show(truncate=False)

# collect_list and collect_set are the reverse of explode
# They gather multiple rows back into a single array per group
print("--- COLLECT_LIST (re-aggregate after explode) ---")
exploded.groupBy("policy_id").agg(
    F.collect_list(F.struct("type", "limit", "deductible")).alias("coverages_rebuilt")
).show(truncate=False)
```

### Structs

```python
# After exploding, each coverage is a struct — access fields with dot notation
# The .* syntax extracts ALL fields from a struct into top-level columns
print("--- STRUCT FIELD ACCESS ---")
complex_df.select("policy_id", F.col("coverages")[0].type.alias("first_type")).show()

# F.struct creates a new struct column by combining multiple columns
print("--- CREATE STRUCT ---")
df.select("policy_id", F.struct("state", "premium").alias("state_premium")).show(truncate=False)
```

### Maps

```python
# Access a map value by key using bracket notation
# If endorsements = {"wind_mitigation": "yes", ...}, this returns "yes"
print("--- MAP ACCESS BY KEY ---")
complex_df.select("policy_id",
    F.col("endorsements")["wind_mitigation"].alias("wind_mitigation")
).show()

# map_keys returns an array of all keys in the map
print("--- MAP KEYS ---")
complex_df.select("policy_id", F.map_keys("endorsements").alias("keys")).show(truncate=False)

# map_values returns an array of all values in the map
print("--- MAP VALUES ---")
complex_df.select("policy_id", F.map_values("endorsements").alias("values")).show(truncate=False)

# Explode a map into rows — each key-value pair becomes a separate row
# Result columns are named "key" and "value" via .alias()
print("--- EXPLODE MAP ---")
complex_df.select("policy_id", F.explode("endorsements").alias("key", "value")).show()
```

---

## 17. Union & Set Operations

### Create two DataFrames with overlapping data

```python
df_batch1 = spark.createDataFrame([
    ("POL001", "FL", 1250.00),
    ("POL002", "TX", 980.50),
    ("POL003", "FL", 1500.00),
], ["policy_id", "state", "premium"])

df_batch2 = spark.createDataFrame([
    ("POL003", "FL", 1500.00),   # duplicate with batch1
    ("POL004", "LA", 2100.75),
    ("POL005", "FL", 1300.00),
], ["policy_id", "state", "premium"])
```

### Union and set operations

```python
# union stacks rows from two DataFrames — matches columns by POSITION (order matters)
# Both DataFrames must have the same number of columns with compatible types
print("--- UNION (includes duplicates) ---")
df_batch1.union(df_batch2).show()

# unionByName matches columns by NAME instead of position — safer when column order differs
print("--- UNION BY NAME ---")
df_batch1.unionByName(df_batch2).show()

# distinct removes duplicate rows from the result after union
print("--- UNION + DISTINCT ---")
df_batch1.union(df_batch2).distinct().show()

# intersect returns only rows that appear in BOTH DataFrames
print("--- INTERSECT ---")
df_batch1.intersect(df_batch2).show()

# subtract returns rows that are in batch1 but NOT in batch2
print("--- SUBTRACT (in batch1 but not batch2) ---")
df_batch1.subtract(df_batch2).show()
```

### unionByName with missing columns

```python
# Create a DataFrame with an extra column
df_batch3 = spark.createDataFrame([
    ("POL006", "CA", 1800.00, "active"),
], ["policy_id", "state", "premium", "status"])

# allowMissingColumns=True handles DataFrames with different schemas
# Missing columns are filled with null
print("--- UNION BY NAME WITH MISSING COLUMNS ---")
df_batch1.unionByName(df_batch3, allowMissingColumns=True).show()
```

---

## 18. Writing DataFrames

**NOTE:** These commands write to Delta tables or file paths. On Databricks Community Edition, you can write to the default catalog. The commands below are shown with comments explaining each write mode — uncomment to run them.

```python
# Write as a managed Delta table — "overwrite" replaces the entire table
# df.write.format("delta").mode("overwrite").saveAsTable("default.policies_test")

# "append" adds new rows to the existing table without removing old data
# df.write.format("delta").mode("append").saveAsTable("default.policies_test")

# partitionBy creates a directory structure based on the column values
# Data for each state goes into a separate folder for efficient pruning
# df.write.format("delta") \
#     .partitionBy("state") \
#     .mode("overwrite") \
#     .saveAsTable("default.policies_partitioned")

# mergeSchema allows appending data that has NEW columns not in the existing table
# The new columns are added to the table schema automatically
# df.write.format("delta") \
#     .option("mergeSchema", "true") \
#     .mode("append") \
#     .saveAsTable("default.policies_test")

# overwriteSchema replaces the entire schema — use when columns are renamed or removed
# df.write.format("delta") \
#     .option("overwriteSchema", "true") \
#     .mode("overwrite") \
#     .saveAsTable("default.policies_test")

# coalesce reduces the number of output files WITHOUT a full shuffle
# 1 file instead of potentially hundreds of small files
# df.coalesce(1).write.format("delta").mode("overwrite").saveAsTable("default.policies_small")

# Write modes:
# "overwrite" — replace existing data
# "append"    — add to existing data
# "ignore"    — silently skip if table already exists
# "error"     — throw an error if table exists (this is the default)

print("Section 18 — write commands are commented out. Uncomment to run on Databricks.")
```

---

## 19. Delta Lake Operations

**NOTE:** Delta MERGE, time travel, optimize, and vacuum require writing a table first. The cell below creates a table, then demonstrates each Delta operation.

```python
# --- CREATE A DELTA TABLE TO WORK WITH ---
# Write the policies DataFrame as a Delta table
df.write.format("delta").mode("overwrite").saveAsTable("default.policies_demo")
print("Created default.policies_demo")
```

### MERGE (Upsert) — PySpark API

```python
from delta.tables import DeltaTable

# Incoming batch of updates — POL001 has a new premium, POL006 is a new policy
source_df = spark.createDataFrame([
    ("POL001", "C100", "FL", 1400.00, "active",  "2024-01-15", "2025-01-15"),
    ("POL006", "C104", "CA", 1800.00, "active",  "2024-09-01", "2025-09-01"),
], ["policy_id", "customer_id", "state", "premium", "status",
    "effective_date", "expiration_date"]) \
    .withColumn("effective_date", F.col("effective_date").cast(DateType())) \
    .withColumn("expiration_date", F.col("expiration_date").cast(DateType()))

# Get a reference to the existing Delta table
target = DeltaTable.forName(spark, "default.policies_demo")

# alias gives each side a short name for referencing columns
# merge specifies the join condition to match source rows to target rows
target.alias("t").merge(
    source_df.alias("s"),
    "t.policy_id = s.policy_id"                     # Match condition
).whenMatchedUpdate(
    # condition: only update if premium actually changed
    condition="t.premium <> s.premium",
    # set: which target columns to update and what values to use
    set={
        "premium": "s.premium",
    }
).whenNotMatchedInsertAll(                           # Insert all columns for new rows
).execute()

print("After MERGE:")
spark.table("default.policies_demo").show()
```

### Time Travel

```python
# Query the table as it existed at version 0 (before the merge)
print("--- VERSION 0 (before merge) ---")
spark.read.format("delta").option("versionAsOf", 0).table("default.policies_demo").show()

print("--- CURRENT VERSION (after merge) ---")
spark.table("default.policies_demo").show()
```

### DESCRIBE HISTORY

```python
# Show the audit log of all operations performed on the table
spark.sql("DESCRIBE HISTORY default.policies_demo").show(truncate=False)
```

### OPTIMIZE and VACUUM

```python
# OPTIMIZE compacts small files into larger ones for better read performance
# ZORDER co-locates data by the specified columns within files for data skipping
spark.sql("OPTIMIZE default.policies_demo ZORDER BY (state)")

# VACUUM DRY RUN shows which files WOULD be deleted without actually deleting
# 168 hours (7 days) is the default minimum retention period
spark.sql("VACUUM default.policies_demo RETAIN 168 HOURS DRY RUN").show(truncate=False)

print("OPTIMIZE and VACUUM complete")
```

### Schema Evolution

```python
# Add a new column to an existing Delta table without rewriting data
spark.sql("ALTER TABLE default.policies_demo ADD COLUMN (risk_score DOUBLE)")

# Verify the new column
spark.table("default.policies_demo").printSchema()
```

### Cleanup

```python
# Drop the demo table when done
spark.sql("DROP TABLE IF EXISTS default.policies_demo")
print("Cleaned up default.policies_demo")
```

---

## 20. Temporary Views & Spark SQL

```python
# Register the DataFrame as a temporary SQL view
# This lets you query it using SQL syntax — the view lasts for the SparkSession
df.createOrReplaceTempView("policies")
claims.createOrReplaceTempView("claims")

# spark.sql() executes a SQL query string and returns a new DataFrame
# You can use any standard SQL: SELECT, WHERE, GROUP BY, JOIN, etc.
print("--- SQL AGGREGATION ---")
spark.sql("""
    SELECT state, COUNT(*) AS cnt, ROUND(AVG(premium), 2) AS avg_prem
    FROM policies
    WHERE status = 'active'
    GROUP BY state
    ORDER BY avg_prem DESC
""").show()

# You can also join across views
print("--- SQL JOIN ---")
spark.sql("""
    SELECT p.policy_id, p.state, p.premium, c.claim_amount, c.claim_status
    FROM policies p
    INNER JOIN claims c ON p.policy_id = c.policy_id
    ORDER BY c.claim_amount DESC
""").show()

# CTE + Window function — top 1 claim per policy
print("--- SQL CTE + WINDOW ---")
spark.sql("""
    WITH ranked AS (
        SELECT
            c.claim_id,
            c.policy_id,
            c.claim_amount,
            ROW_NUMBER() OVER (PARTITION BY c.policy_id ORDER BY c.claim_amount DESC) AS rn
        FROM claims c
    )
    SELECT claim_id, policy_id, claim_amount
    FROM ranked
    WHERE rn = 1
""").show()
```

---

## 21. Performance Essentials

```python
# Broadcast join — sends the small DataFrame to all executors to avoid a shuffle
# Use when one side of the join is small (roughly under 10MB)
print("--- BROADCAST JOIN ---")
df.join(F.broadcast(state_lookup), "state").show()

# cache() stores the DataFrame in memory for faster repeated access
# Use when you read the same DataFrame multiple times in the same job
df.cache()

# Verify it's cached by running two actions — second one should be faster
print("Count:", df.count())
print("FL count:", df.filter(F.col("state") == "FL").count())

# unpersist() releases the cached data to free up memory
df.unpersist()

# repartition triggers a FULL SHUFFLE to redistribute data across the specified number of partitions
# Use to increase partitions or redistribute evenly
df_repart = df.repartition(4)
print("After repartition(4):", df_repart.rdd.getNumPartitions(), "partitions")

# Repartition by a column — all rows with the same state end up in the same partition
df_repart_col = df.repartition("state")
print("After repartition('state'):", df_repart_col.rdd.getNumPartitions(), "partitions")

# coalesce reduces partitions WITHOUT a shuffle — only merges existing partitions
# Can only decrease partitions, not increase — much cheaper than repartition
df_coal = df.repartition(8).coalesce(2)
print("After repartition(8) then coalesce(2):", df_coal.rdd.getNumPartitions(), "partitions")

# explain() shows the query execution plan — useful for debugging performance
print("--- EXPLAIN ---")
df.filter(F.col("state") == "FL").explain()
```

---

## 22. Auto Loader (Databricks Incremental Ingestion)

**NOTE:** Auto Loader requires actual files in a cloud path to process. The code below is valid syntax that you can adapt — it will not run without source files.

```python
# readStream starts a streaming query instead of a batch read
# "cloudFiles" is the Auto Loader format — it tracks which files have been processed
# so rerunning the job only picks up NEW files (idempotent)

# --- UNCOMMENT TO RUN WITH REAL FILES ---
# bronze_df = (
#     spark.readStream
#     .format("cloudFiles")
#     .option("cloudFiles.format", "json")                # Format of source files
#     .option("cloudFiles.schemaLocation", "/tmp/schema/") # Where to store inferred schema
#     .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Auto-add new columns
#     .load("/tmp/raw/data/")                             # Path to monitor for new files
#
#     # Add metadata columns for auditing
#     .withColumn("_ingestion_timestamp", F.current_timestamp())  # When this row was ingested
#     .withColumn("_source_file", F.input_file_name())            # Which file this row came from
# )
#
# # writeStream writes the streaming DataFrame to a Delta table
# # checkpointLocation tracks streaming progress — required for exactly-once processing
# # mergeSchema handles new columns appearing in future files
# # trigger(availableNow=True) processes all available files in micro-batches then stops
# bronze_df.writeStream \
#     .format("delta") \
#     .option("checkpointLocation", "/tmp/checkpoints/") \
#     .option("mergeSchema", "true") \
#     .trigger(availableNow=True) \
#     .toTable("default.bronze_table")

print("Section 22 — Auto Loader commands are commented out. Uncomment with real file paths.")
```

---

## 23. Unity Catalog Essentials

**NOTE:** Unity Catalog requires a Databricks workspace with Unity Catalog enabled. The SQL below is shown as reference. On Community Edition, use the `default` catalog.

```python
# Unity Catalog uses a three-level namespace: catalog.schema.table
# These SQL commands demonstrate the pattern — uncomment on a UC-enabled workspace

# spark.sql("CREATE CATALOG IF NOT EXISTS acme_insurance")
# spark.sql("CREATE SCHEMA IF NOT EXISTS acme_insurance.gold")
#
# spark.sql("""
#     CREATE TABLE IF NOT EXISTS acme_insurance.gold.policies (
#         policy_id STRING NOT NULL,
#         premium   DOUBLE
#     ) USING DELTA
# """)
#
# -- Grant permissions — users need access at EACH level to reach the table
# spark.sql("GRANT USE CATALOG  ON CATALOG acme_insurance      TO `data_analysts`")
# spark.sql("GRANT USE SCHEMA   ON SCHEMA  acme_insurance.gold TO `data_analysts`")
# spark.sql("GRANT SELECT       ON TABLE   acme_insurance.gold.policies TO `data_analysts`")
#
# -- PII masking view — analysts query the view instead of the base table
# spark.sql("""
#     CREATE OR REPLACE VIEW acme_insurance.gold.policies_masked AS
#     SELECT policy_id, CONCAT('***-**-', RIGHT(ssn, 4)) AS ssn_masked
#     FROM acme_insurance.gold.policies
# """)
#
# -- Row-level security via dynamic view
# spark.sql("""
#     CREATE OR REPLACE VIEW acme_insurance.gold.policies_secured AS
#     SELECT * FROM acme_insurance.gold.policies
#     WHERE state IN (
#         SELECT allowed_state FROM admin.user_assignments
#         WHERE user_email = current_user()
#     )
# """)

print("Section 23 — Unity Catalog commands are commented out. Uncomment on a UC-enabled workspace.")
```

---

## Quick Reference: Most Common Interview Patterns

| Pattern | Code |
|---------|------|
| Deduplicate (keep latest) | `row_number().over(Window.partitionBy("id").orderBy(desc("ts")))` then filter `== 1` |
| Top N per group | `row_number().over(Window.partitionBy("group").orderBy(desc("val")))` then filter `<= N` |
| Running total | `sum("val").over(Window.partitionBy("grp").orderBy("date").rowsBetween(unboundedPreceding, currentRow))` |
| Pct of group total | `col("val") / sum("val").over(Window.partitionBy("grp"))` |
| Days between rows | `datediff(col("date"), lag("date",1).over(w))` |
| Conditional count | `sum(when(condition, 1).otherwise(0))` |
| Null count per column | `select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])` |
| Anti-join (no match) | `df.join(other, "key", "left_anti")` |
| Broadcast small table | `df.join(F.broadcast(small), "key")` |
| Loss ratio | `sum("claims") / sum("premium")` |
