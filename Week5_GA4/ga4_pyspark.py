"""
Week 5 GA4 - PySpark DataFrame pipeline on Dataproc (GCS input/output).
Run: gcloud dataproc jobs submit pyspark ga4_pyspark.py ... -- --bucket <BUCKET>
DataFrame API only (no Spark SQL).
"""
import argparse
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# ---------------- args / paths / session
parser = argparse.ArgumentParser()
parser.add_argument("--bucket", required=True)
args = parser.parse_args()

BASE = f"gs://{args.bucket}"
CUST_IN = f"{BASE}/input/customer_dataset.csv"
TXN_IN = f"{BASE}/input/transaction_dataset.csv"
OUT = f"{BASE}/output"

spark = (SparkSession.builder.appName("GA4-PySpark-Cleaning-Join-Agg")
         .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
         .config("spark.sql.ansi.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")


def banner(t):
    print("\n" + "=" * 80 + f"\n{t}\n" + "=" * 80)


def save(df, path):
    """Write one CSV (with header) per output folder."""
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(path)
    print(f"[SAVED] {path}")


def blank_to_null(c):
    """Trim spaces; treat '', 'null', 'none', 'nan', 'na' as missing."""
    t = F.trim(F.col(c))
    return F.when(t.isNull() | F.lower(t).isin("", "null", "none", "nan", "na", "n/a"),
                  F.lit(None)).otherwise(t)


def reason(cond, text):
    return F.when(cond, F.lit(text))


# City variants -> standard name (keys lower-case)
CITY_MAP = {
    "kochi": "Kochi", "cochin": "Kochi",
    "bangalore": "Bengaluru", "bengaluru": "Bengaluru", "blr": "Bengaluru",
    "bombay": "Mumbai", "mumbai": "Mumbai",
    "madras": "Chennai", "chennai": "Chennai",
    "trivandrum": "Trivandrum", "trivandrm": "Trivandrum", "thiruvananthapuram": "Trivandrum",
    "cmbt": "Coimbatore", "coimbatore": "Coimbatore", "kovai": "Coimbatore",
    "poona": "Pune", "pune": "Pune",
    "hyd": "Hyderabad", "hyderabad": "Hyderabad",
}
city_map_expr = F.create_map(*[F.lit(x) for kv in CITY_MAP.items() for x in kv])


def standardize_city(c):
    key = F.trim(F.lower(F.regexp_replace(F.regexp_replace(c, r"[.\-_]", " "), r"\s+", " ")))
    return F.when(c.isNull(), None).otherwise(F.coalesce(city_map_expr[key], F.initcap(key)))


def split_valid_invalid(df, clean_cols, key_col, reasons, dup_key_label):
    """1) rule-based invalids  2) exact duplicates (all columns)  3) duplicate key."""
    w_dup = Window.partitionBy(*clean_cols).orderBy("_rid")
    df = df.withColumn("_dup_rn", F.row_number().over(w_dup))
    all_reasons = reasons + [reason(F.col("_dup_rn") > 1, "duplicate_record")]
    df = df.withColumn("invalid_reason", F.concat_ws("; ", *all_reasons))

    valid1 = df.filter(F.col("invalid_reason") == "")
    invalid1 = df.filter(F.col("invalid_reason") != "")

    w_key = Window.partitionBy(key_col).orderBy("_rid")
    valid1 = valid1.withColumn("_key_rn", F.row_number().over(w_key))
    valid = valid1.filter(F.col("_key_rn") == 1).drop("_key_rn")
    invalid2 = (valid1.filter(F.col("_key_rn") > 1).drop("_key_rn")
                .withColumn("invalid_reason", F.lit(dup_key_label)))
    return valid, invalid1.unionByName(invalid2)

# ============================================================ 1. LOAD & INSPECT
banner("STEP 1: LOAD & INSPECT")
# read all columns as string so malformed values are not silently nulled
cust_raw = spark.read.option("header", True).option("inferSchema", False).csv(CUST_IN)
txn_raw = spark.read.option("header", True).option("inferSchema", False).csv(TXN_IN)
cust_raw = cust_raw.toDF(*[c.strip().lower() for c in cust_raw.columns])
txn_raw = txn_raw.toDF(*[c.strip().lower() for c in txn_raw.columns])

print(f"Customers raw rows: {cust_raw.count()}")
cust_raw.printSchema()
cust_raw.show(10, truncate=False)

print(f"Transactions raw rows: {txn_raw.count()}")
txn_raw.printSchema()
txn_raw.show(10, truncate=False)

print("Inferred transaction schema (inspection only):")
spark.read.option("header", True).option("inferSchema", True).csv(TXN_IN).printSchema()

CUST_COLS = cust_raw.columns
TXN_COLS = txn_raw.columns

# ======================================= 2+3a. CUSTOMER: STANDARDIZE & CLEAN
banner("STEP 2/3: CUSTOMER - STANDARDIZE & CLEAN")
c = cust_raw.withColumn("_rid", F.monotonically_increasing_id())
c = (c.withColumn("c_customer_id", F.lower(blank_to_null("customer_id")))
      .withColumn("c_customer_name", F.initcap(F.regexp_replace(blank_to_null("customer_name"), r"\s+", " ")))
      .withColumn("c_city", standardize_city(blank_to_null("city")))
      .withColumn("c_status", F.initcap(blank_to_null("status"))))

cust_reasons = [
    reason(F.col("c_customer_id").isNull(), "missing_customer_id"),
    reason(F.col("c_customer_name").isNull(), "missing_customer_name"),
    reason(F.col("c_city").isNull(), "missing_city"),
    reason(F.col("c_status").isNull(), "missing_status"),
]
c_clean_cols = ["c_customer_id", "c_customer_name", "c_city", "c_status"]
c_valid, c_invalid = split_valid_invalid(c, c_clean_cols, "c_customer_id",
                                         cust_reasons, "duplicate_customer_id")

customers_clean = c_valid.select(*[F.col(x).alias(x[2:]) for x in c_clean_cols])
customers_invalid = c_invalid.select(*CUST_COLS, "invalid_reason")

print(f"Customers -> clean: {customers_clean.count()} | invalid: {customers_invalid.count()}")
print("Sample INVALID customers:")
customers_invalid.show(10, truncate=False)
print("Sample CLEANED customers:")
customers_clean.show(10, truncate=False)
print("City values after standardization:")
customers_clean.groupBy("city").count().orderBy("city").show(50, truncate=False)

# ================================== 2+3b. TRANSACTIONS: TRANSFORM & CLEAN
banner("STEP 2/3: TRANSACTIONS - TRANSFORM & CLEAN")
t = txn_raw.withColumn("_rid", F.monotonically_increasing_id())

date_s = blank_to_null("transaction_date")
parsed_date = F.coalesce(
    F.when(date_s.rlike(r"^\d{4}-\d{1,2}-\d{1,2}$"), F.to_date(date_s, "yyyy-M-d")),
    F.when(date_s.rlike(r"^\d{4}/\d{1,2}/\d{1,2}$"), F.to_date(date_s, "yyyy/M/d")),
    F.when(date_s.rlike(r"^\d{1,2}-\d{1,2}-\d{4}$"), F.to_date(date_s, "d-M-yyyy")),
    F.when(date_s.rlike(r"^\d{1,2}/\d{1,2}/\d{4}$"), F.to_date(date_s, "d/M/yyyy")),
)
amt_s = F.regexp_replace(blank_to_null("transaction_amount"), r"[,\s]", "")
amt = F.when(amt_s.rlike(r"^-?\d+(\.\d+)?$"), amt_s.cast("double"))

t = (t.withColumn("c_transaction_id", F.lower(blank_to_null("transaction_id")))
      .withColumn("c_customer_id", F.lower(blank_to_null("customer_id")))
      .withColumn("c_transaction_date", parsed_date)            # DateType
      .withColumn("c_transaction_amount", F.round(amt, 2))      # DoubleType
      .withColumn("c_payment_mode", F.upper(F.regexp_replace(blank_to_null("payment_mode"), r"[\s_\-]+", " "))))

txn_reasons = [
    reason(F.col("c_transaction_id").isNull(), "missing_transaction_id"),
    reason(F.col("c_customer_id").isNull(), "missing_customer_id"),
    reason(blank_to_null("transaction_date").isNull(), "missing_transaction_date"),
    reason(blank_to_null("transaction_date").isNotNull() & F.col("c_transaction_date").isNull(),
           "malformed_transaction_date"),
    reason(blank_to_null("transaction_amount").isNull(), "missing_transaction_amount"),
    reason(blank_to_null("transaction_amount").isNotNull() & F.col("c_transaction_amount").isNull(),
           "non_numeric_transaction_amount"),
    reason(F.col("c_transaction_amount") <= 0, "non_positive_transaction_amount"),
    reason(F.col("c_payment_mode").isNull(), "missing_payment_mode"),
]
t_clean_cols = ["c_transaction_id", "c_customer_id", "c_transaction_date",
                "c_transaction_amount", "c_payment_mode"]
t_valid, t_invalid = split_valid_invalid(t, t_clean_cols, "c_transaction_id",
                                         txn_reasons, "duplicate_transaction_id")

# Referential integrity: customer_id must exist in CLEANED customers
cust_ids = customers_clean.select(F.col("customer_id").alias("c_customer_id"))
t_orphans = (t_valid.join(cust_ids, "c_customer_id", "left_anti")
             .withColumn("invalid_reason", F.lit("customer_id_not_in_customer_dataset")))
t_valid = t_valid.join(cust_ids, "c_customer_id", "left_semi")
t_invalid = t_invalid.unionByName(t_orphans)

# Column transformations
transactions_clean = (
    t_valid.select(*[F.col(x).alias(x[2:]) for x in t_clean_cols])
    .withColumn("amount_category",
                F.when(F.col("transaction_amount") < 3000, "Low")
                 .when(F.col("transaction_amount") < 7000, "Medium")
                 .otherwise("High"))
    .withColumn("transaction_month", F.month("transaction_date"))
    .withColumn("transaction_month_name", F.date_format("transaction_date", "MMMM"))
)
transactions_invalid = t_invalid.select(*TXN_COLS, "invalid_reason")

print(f"Transactions -> clean: {transactions_clean.count()} | invalid: {transactions_invalid.count()}")
print("Invalid transactions by reason:")
transactions_invalid.groupBy("invalid_reason").count().show(truncate=False)
print("Sample INVALID transactions:")
transactions_invalid.show(10, truncate=False)
print("Sample CLEANED transactions:")
transactions_clean.printSchema()
transactions_clean.show(10, truncate=False)

save(customers_clean, f"{OUT}/cleaned_data/customers")
save(transactions_clean, f"{OUT}/cleaned_data/transactions")
save(customers_invalid, f"{OUT}/invalid_rows/customers")
save(transactions_invalid, f"{OUT}/invalid_rows/transactions")

# ===================================================================== 4. JOIN
banner("STEP 4: JOIN ON customer_id")
joined = (transactions_clean.join(customers_clean, on="customer_id", how="inner")
          .select("transaction_id", "customer_id", "customer_name", "city", "status",
                  "transaction_date", "transaction_month", "transaction_month_name",
                  "transaction_amount", "amount_category", "payment_mode"))
print(f"Joined rows: {joined.count()}")
joined.show(10, truncate=False)
save(joined, f"{OUT}/joined_data")

# ============================================================== 5. AGGREGATIONS
banner("STEP 5: AGGREGATIONS")
per_customer = (joined.groupBy("customer_id", "customer_name")
                .agg(F.round(F.sum("transaction_amount"), 2).alias("total_transaction_amount"),
                     F.round(F.avg("transaction_amount"), 2).alias("avg_transaction_amount"),
                     F.count("*").alias("transaction_count"))
                .orderBy(F.desc("total_transaction_amount")))
print("Total & average transaction amount per customer:")
per_customer.show(10, truncate=False)

per_city = (joined.groupBy("city")
            .agg(F.round(F.sum("transaction_amount"), 2).alias("total_transaction_amount"))
            .orderBy(F.desc("total_transaction_amount")))
print("Total transaction amount per city:")
per_city.show(truncate=False)

top3 = per_customer.select("customer_id", "customer_name", "total_transaction_amount").limit(3)
print("Top 3 customers by total transaction amount:")
top3.show(truncate=False)

save(per_customer, f"{OUT}/aggregates/customer_totals")
save(per_city, f"{OUT}/aggregates/city_totals")
save(top3, f"{OUT}/aggregates/top3_customers")

banner("JOB COMPLETED SUCCESSFULLY")
spark.stop()
