"""
Week 6 - Graded Assignment 5
Dataproc + SparkSQL: Aggregations & Slowly Changing Dimensions (SCD Type I / Type II)

Constraint: SparkSQL ONLY (no PySpark DataFrame API).
  - Tables created with SQL DDL (CREATE TEMP VIEW ... USING csv)
  - All logic via spark.sql("...")
  - Results written to GCS with SQL (INSERT OVERWRITE DIRECTORY ... USING csv)

Real schema (confirmed from the data files):
  transactions     : transaction_id, customer_id, transaction_date, transaction_amount, payment_mode, city
  customer_master  : customer_id, customer_name, city, dob, effective_date, expiry_date, current_flag   (1 = current)
  customer_updates : customer_id, customer_name, city, dob, change_date
SCD tracked attributes: customer_name, city, dob.
"""

from pyspark.sql import SparkSession

# ----------------------------------------------------------------------
# EDIT ONLY THIS
# ----------------------------------------------------------------------
BUCKET = "gs://garv-ga5-bucket"
IN     = BUCKET + "/input"
OUT    = BUCKET + "/output"

TRANSACTIONS = IN + "/transactions.csv"
CUST_MASTER  = IN + "/customer_master.csv"
CUST_UPDATES = IN + "/customer_updates.csv"
# ----------------------------------------------------------------------

spark = SparkSession.builder.appName("GA5-SparkSQL-SCD").getOrCreate()
spark.sparkContext.setLogLevel("INFO")

def run(sql):
    return spark.sql(sql)

def show(title, sql):
    print("\n" + "=" * 72 + "\n" + title + "\n" + "=" * 72)
    run(sql).show(100, truncate=False)

# ======================================================================
# 0. LOAD DATA AS SQL TABLES (DDL only)
# ======================================================================
run(f"CREATE OR REPLACE TEMP VIEW transactions "
    f"USING csv OPTIONS (path '{TRANSACTIONS}', header 'true', inferSchema 'true')")
run(f"CREATE OR REPLACE TEMP VIEW customer_master "
    f"USING csv OPTIONS (path '{CUST_MASTER}', header 'true', inferSchema 'true')")
run(f"CREATE OR REPLACE TEMP VIEW customer_updates "
    f"USING csv OPTIONS (path '{CUST_UPDATES}', header 'true', inferSchema 'true')")

# ======================================================================
# 1. AGGREGATION QUERIES
# ======================================================================
q_top5 = """
SELECT customer_id, ROUND(SUM(transaction_amount), 2) AS total_amount
FROM transactions
GROUP BY customer_id
ORDER BY total_amount DESC
LIMIT 5
"""
show("Q1a: Top 5 customers by total transaction amount", q_top5)

q_city = """
SELECT city, COUNT(DISTINCT customer_id) AS num_customers
FROM transactions
GROUP BY city
ORDER BY num_customers DESC
"""
show("Q1b: Number of customers per city", q_city)

q_avg = """
SELECT ROUND(AVG(transaction_amount), 2) AS avg_transaction_value
FROM transactions
"""
show("Q1c: Average transaction value (overall)", q_avg)

# Save each aggregation result as a separate CSV in GCS
run(f"INSERT OVERWRITE DIRECTORY '{OUT}/top5_customers'     USING csv OPTIONS('header' 'true') {q_top5}")
run(f"INSERT OVERWRITE DIRECTORY '{OUT}/customers_per_city' USING csv OPTIONS('header' 'true') {q_city}")
run(f"INSERT OVERWRITE DIRECTORY '{OUT}/avg_transaction'    USING csv OPTIONS('header' 'true') {q_avg}")

# ======================================================================
# 2. SLOWLY CHANGING DIMENSIONS
# ======================================================================
show("Customer Master - BEFORE (original)",
     "SELECT * FROM customer_master ORDER BY customer_id, effective_date")
show("Customer Updates (incoming changes)",
     "SELECT * FROM customer_updates ORDER BY customer_id")

# Current snapshot of the master (current_flag = 1)
run("CREATE OR REPLACE TEMP VIEW master_cur AS SELECT * FROM customer_master WHERE current_flag = 1")

# ----------------------------------------------------------------------
# 2a. SCD TYPE I : overwrite in place, NO history.
#     Matched customers take new values; unmatched keep old; new ones inserted.
# ----------------------------------------------------------------------
scd1 = """
SELECT
    COALESCE(u.customer_id,   m.customer_id)   AS customer_id,
    COALESCE(u.customer_name, m.customer_name) AS customer_name,
    COALESCE(u.city,          m.city)          AS city,
    COALESCE(u.dob,           m.dob)           AS dob
FROM master_cur m
FULL OUTER JOIN customer_updates u
    ON m.customer_id = u.customer_id
"""
run(f"CREATE OR REPLACE TEMP VIEW customer_master_scd1 AS {scd1}")
show("Customer Master - AFTER (SCD Type I: overwrite, no history)",
     "SELECT * FROM customer_master_scd1 ORDER BY customer_id")
run(f"INSERT OVERWRITE DIRECTORY '{OUT}/scd_type1' USING csv OPTIONS('header' 'true') "
    f"SELECT * FROM customer_master_scd1 ORDER BY customer_id")

# ----------------------------------------------------------------------
# 2b. SCD TYPE II : keep history. Expire changed current row, insert new version.
#     "changed" = customer_name / city / dob differs  (<=> is null-safe equality).
#     All date columns cast to STRING so the UNION column types line up.
# ----------------------------------------------------------------------
scd2 = """
-- (1) rows that stay as-is: history rows, or current rows with no/identical update
SELECT m.customer_id, m.customer_name, m.city, m.dob,
       CAST(m.effective_date AS STRING) AS effective_date,
       CAST(m.expiry_date    AS STRING) AS expiry_date,
       m.current_flag
FROM customer_master m
LEFT JOIN customer_updates u ON m.customer_id = u.customer_id
WHERE m.current_flag = 0
   OR u.customer_id IS NULL
   OR (u.customer_name <=> m.customer_name AND u.city <=> m.city AND u.dob <=> m.dob)

UNION ALL
-- (2) old version of a CHANGED customer -> expire it
SELECT m.customer_id, m.customer_name, m.city, m.dob,
       CAST(m.effective_date AS STRING) AS effective_date,
       CAST(u.change_date    AS STRING) AS expiry_date,
       0 AS current_flag
FROM customer_master m
JOIN customer_updates u ON m.customer_id = u.customer_id
WHERE m.current_flag = 1
  AND NOT (u.customer_name <=> m.customer_name AND u.city <=> m.city AND u.dob <=> m.dob)

UNION ALL
-- (3) new current version of a CHANGED customer
SELECT u.customer_id, u.customer_name, u.city, u.dob,
       CAST(u.change_date AS STRING) AS effective_date,
       CAST(NULL AS STRING)          AS expiry_date,
       1 AS current_flag
FROM customer_master m
JOIN customer_updates u ON m.customer_id = u.customer_id
WHERE m.current_flag = 1
  AND NOT (u.customer_name <=> m.customer_name AND u.city <=> m.city AND u.dob <=> m.dob)

UNION ALL
-- (4) brand-new customers present only in updates -> insert as current
SELECT u.customer_id, u.customer_name, u.city, u.dob,
       CAST(u.change_date AS STRING) AS effective_date,
       CAST(NULL AS STRING)          AS expiry_date,
       1 AS current_flag
FROM customer_updates u
LEFT JOIN customer_master m ON u.customer_id = m.customer_id
WHERE m.customer_id IS NULL
"""
run(f"CREATE OR REPLACE TEMP VIEW customer_master_scd2 AS {scd2}")
show("Customer Master - AFTER (SCD Type II: full history)",
     "SELECT * FROM customer_master_scd2 ORDER BY customer_id, effective_date")
run(f"INSERT OVERWRITE DIRECTORY '{OUT}/scd_type2' USING csv OPTIONS('header' 'true') "
    f"SELECT * FROM customer_master_scd2 ORDER BY customer_id, effective_date")

print("\nDone. Results written under: " + OUT)
spark.stop()
