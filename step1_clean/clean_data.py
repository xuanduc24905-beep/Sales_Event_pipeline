#!/usr/bin/env python3
"""
STEP 1 — clean_data.py (Spark)
Read raw JSON from 2 sources (app + web), parse 2 formats,
dedup, fix nulls, remove outliers → write Parquet + JSON Lines.

Parquet  → /sales/processed/        (source of truth for Spark)
JSON Lines → /sales/processed_csv/  (Hadoop Streaming input for Step 4)

Usage:
  spark-submit --master spark://192.168.56.10:7077 clean_data.py
"""

import sys
import time
import logging
from datetime import datetime

import psycopg2

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
HDFS = "hdfs://192.168.56.10:9000"
PG_CONN = dict(host="localhost", port=5432, dbname="sales_db", user="hadoop", password="hadoop")


def _pg_conn():
    return psycopg2.connect(**PG_CONN)


# ── Sellers lookup (200 sellers) ──────────────────────────────────────────────
REGIONS = ["north", "south", "central", "west", "east"]

def build_sellers_data():
    """Return list of (seller_id, seller_type, seller_region) for 200 sellers."""
    rows = []
    for i in range(1, 201):
        sid = f"S{i:03d}"
        if i <= 60:
            stype = "high_performer"
        elif i <= 160:
            stype = "average_seller"
        else:
            stype = "low_performer"
        region = REGIONS[(i - 1) % 5]
        rows.append((sid, stype, region))
    return rows


# ── Spark session ─────────────────────────────────────────────────────────────
def create_spark():
    spark = (
        SparkSession.builder
        .appName("SalesCleanData")
        .master("spark://192.168.56.10:7077")
        .config("spark.executor.memory",         "2g")
        .config("spark.executor.cores",          "2")
        .config("spark.driver.memory",           "1g")
        .config("spark.sql.adaptive.enabled",    "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ── Status normalization ──────────────────────────────────────────────────────
def normalize_status(col_expr):
    """
    Map raw status aliases to canonical values using when().isin() chain.
    Null or unrecognized → 'unknown'.
    """
    completed_vals = ["completed", "done", "success", "paid", "complete", "COMPLETED"]
    cancelled_vals = ["cancelled", "cancel", "CANCEL", "CANCELLED"]
    returned_vals  = ["returned",  "return", "refund", "REFUND"]
    pending_vals   = ["pending",   "PENDING", "processing", "in_progress"]

    return (
        F.when(col_expr.isin(completed_vals), "completed")
        .when(col_expr.isin(cancelled_vals),  "cancelled")
        .when(col_expr.isin(returned_vals),   "returned")
        .when(col_expr.isin(pending_vals),    "pending")
        .otherwise("unknown")
    )


# ── App parsing ───────────────────────────────────────────────────────────────
def parse_app(spark):
    """
    Read from /sales/raw/source=app/ (one JSON record per line).
    Schema:
      oid, ts (unix int), slr, st, usr{uid, rgn}, tot, items[{pid, q, p, d}]
    Returns cleaned DataFrame with canonical column names.
    """
    log.info("Parsing App source from HDFS …")
    raw = spark.read.option("multiLine", "false").json(
        f"{HDFS}/sales/raw/source=app/"
    )

    df = (
        raw
        # Filter out _dup=True records (keep where _dup is null OR _dup == False)
        .filter(F.col("_dup").isNull() | (F.col("_dup") == False))
        .drop("_dup")
        .select(
            F.col("oid").alias("order_id"),
            F.from_unixtime(F.col("ts").cast("long")).alias("_order_ts_str"),
            F.col("slr").alias("seller_id"),
            normalize_status(F.col("st")).alias("status"),
            # usr may be null — use coalesce / when
            F.when(F.col("usr").isNotNull(), F.col("usr.uid"))
             .otherwise(F.lit(None).cast(StringType()))
             .alias("customer_id"),
            F.when(F.col("usr").isNotNull(), F.col("usr.rgn"))
             .otherwise(F.lit(None).cast(StringType()))
             .alias("region"),
            F.col("tot").cast(DoubleType()).alias("total_amount"),
            F.lit("app").alias("data_source"),
        )
        .withColumn("order_ts",   F.to_timestamp(F.col("_order_ts_str")))
        .withColumn("order_date", F.to_date(F.col("_order_ts_str")))
        .drop("_order_ts_str")
    )

    log.info(f"  App rows after _dup filter: {df.count():,}")
    return df


# ── Web parsing ───────────────────────────────────────────────────────────────
def parse_web(spark):
    """
    Read from /sales/raw/source=web/ (one JSON record per line).
    Schema:
      order_id, order_date (ISO with tz), seller_id, status,
      customer_id, region, total_amount, metadata{source, version, _retry}
    Returns cleaned DataFrame with canonical column names.
    """
    log.info("Parsing Web source from HDFS …")
    raw = spark.read.option("multiLine", "false").json(
        f"{HDFS}/sales/raw/source=web/"
    )

    df = (
        raw
        # Filter out _retry=True records
        .filter(F.col("metadata._retry").isNull() | (F.col("metadata._retry") == False))
        .drop("metadata")
        # Strip timezone suffix (+07:00, +08:00, Z) from order_date string
        .withColumn(
            "_stripped",
            F.regexp_replace(F.col("order_date"), r'(\+\d{2}:\d{2}|Z)$', '')
        )
        .select(
            F.col("order_id"),
            F.to_timestamp(F.col("_stripped")).alias("order_ts"),
            F.to_date(F.col("_stripped")).alias("order_date"),
            F.col("seller_id"),
            normalize_status(F.col("status")).alias("status"),
            F.coalesce(F.col("customer_id"), F.lit("UNKNOWN")).alias("customer_id"),
            F.col("region"),
            F.col("total_amount").cast(DoubleType()).alias("total_amount"),
            F.lit("web").alias("data_source"),
        )
    )

    log.info(f"  Web rows after _retry filter: {df.count():,}")
    return df


# ── Deduplication ─────────────────────────────────────────────────────────────
def dedup_orders(df):
    """
    Deduplicate on order_id:
      - Prefer rows where total_amount IS NOT NULL (sort 0 before 1)
      - Among equals, prefer latest order_ts
      - Keep row_number == 1
    """
    log.info("Deduplicating on order_id …")

    null_priority = (
        F.when(F.col("total_amount").isNotNull(), 0).otherwise(1)
    )

    w = (
        Window
        .partitionBy("order_id")
        .orderBy(null_priority, F.col("order_ts").desc())
    )

    deduped = (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    log.info(f"  After dedup: {deduped.count():,}")
    return deduped


# ── Date + amount filters ─────────────────────────────────────────────────────
def apply_filters(df):
    """
    Keep only:
      - order_date between 2022-01-01 and 2024-12-31
      - total_amount > 0
    """
    log.info("Applying date and amount filters …")
    filtered = (
        df
        .filter(F.col("order_date") >= F.lit("2022-01-01"))
        .filter(F.col("order_date") <= F.lit("2024-12-31"))
        .filter(F.col("total_amount") > 0)
    )
    log.info(f"  After date+amount filter: {filtered.count():,}")
    return filtered


# ── IQR×3 outlier removal ─────────────────────────────────────────────────────
def remove_outliers(df):
    """
    IQR×3 outlier removal on total_amount:
      - Compute Q1, Q3 using approxQuantile on non-null, positive amounts
      - lower = Q1 - 3 * IQR,  upper = Q3 + 3 * IQR
      - Keep: total_amount IS NULL OR (total_amount BETWEEN lower AND upper)
    """
    log.info("Removing outliers using IQR×3 …")

    # Compute quantiles on valid amounts only
    valid_amounts = df.filter(
        F.col("total_amount").isNotNull() & (F.col("total_amount") > 0)
    )
    q1, q3 = valid_amounts.approxQuantile("total_amount", [0.25, 0.75], 0.01)
    iqr = q3 - q1
    lower = q1 - 3 * iqr
    upper = q3 + 3 * iqr

    log.info(f"  Q1={q1:.2f}  Q3={q3:.2f}  IQR={iqr:.2f}")
    log.info(f"  Outlier bounds: lower={lower:.2f}  upper={upper:.2f}")

    filtered = df.filter(
        F.col("total_amount").isNull() |
        (F.col("total_amount").between(lower, upper))
    )
    log.info(f"  After outlier removal: {filtered.count():,}")
    return filtered


# ── Fill null region + add seller_type ───────────────────────────────────────
def enrich_with_sellers(spark, df):
    """
    Broadcast sellers lookup, left-join on seller_id:
      - Fill null region from sellers lookup
      - Add seller_type column
    """
    log.info("Enriching with sellers broadcast …")

    sellers_schema = StructType([
        StructField("seller_id",     StringType(), False),
        StructField("seller_type",   StringType(), False),
        StructField("seller_region", StringType(), False),
    ])
    sellers_df = spark.createDataFrame(build_sellers_data(), schema=sellers_schema)
    sellers_bc = F.broadcast(sellers_df)

    enriched = (
        df
        .join(sellers_bc, on="seller_id", how="left")
        .withColumn("region", F.coalesce(F.col("region"), F.col("seller_region")))
        .drop("seller_region")
    )
    log.info(f"  After seller enrichment: {enriched.count():,}")
    return enriched


# ── Derived columns ───────────────────────────────────────────────────────────
def add_derived_columns(df):
    """Add year_month, year, month from order_date."""
    return (
        df
        .withColumn("year_month", F.date_format(F.col("order_date"), "yyyy-MM"))
        .withColumn("year",       F.year(F.col("order_date")))
        .withColumn("month",      F.month(F.col("order_date")))
    )


# ── Write pipeline_runs ───────────────────────────────────────────────────────
def write_pipeline_run(step, status, rows_in, rows_out, duration_seconds):
    """Append a single pipeline_runs record to PostgreSQL via psycopg2."""
    conn = _pg_conn()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_runs (step, status, rows_in, rows_out, duration_seconds, run_at) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (step, status, int(rows_in), int(rows_out), round(duration_seconds, 2),
             datetime.utcnow()),
        )
    conn.commit()
    conn.close()
    log.info("  pipeline_runs row appended.")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("STEP 1 — Clean Data")
    log.info("=" * 60)

    spark = create_spark()
    t0 = time.time()
    rows_in = 0
    rows_out = 0
    run_status = "failed"

    try:
        # ── Parse sources ──────────────────────────────────────────────────
        app_df = parse_app(spark)
        web_df = parse_web(spark)

        # ── Union ──────────────────────────────────────────────────────────
        log.info("Unioning app + web …")
        combined = app_df.unionByName(web_df)
        rows_in = combined.count()
        log.info(f"  Combined rows (rows_in): {rows_in:,}")

        # ── Dedup ──────────────────────────────────────────────────────────
        deduped = dedup_orders(combined)

        # ── Date + amount filters ──────────────────────────────────────────
        filtered = apply_filters(deduped)

        # ── Outlier removal ────────────────────────────────────────────────
        clean = remove_outliers(filtered)

        # ── Enrich with sellers ────────────────────────────────────────────
        enriched = enrich_with_sellers(spark, clean)

        # ── Derived columns ────────────────────────────────────────────────
        final = add_derived_columns(enriched)

        # Cache final for multiple writes
        final.cache()
        rows_out = final.count()

        # ── Write Parquet ──────────────────────────────────────────────────
        parquet_path = f"{HDFS}/sales/processed/"
        log.info(f"Writing Parquet → {parquet_path} (partitioned by data_source) …")
        (
            final.write
            .mode("overwrite")
            .partitionBy("data_source")
            .parquet(parquet_path)
        )
        log.info("  Parquet write complete.")

        # ── Write JSON Lines → HDFS (Hadoop Streaming input for Step 4) ─────
        json_path = f"{HDFS}/sales/processed_csv/"
        log.info(f"Writing JSON Lines → {json_path} …")
        final.write.mode("overwrite").json(json_path)
        log.info("  JSON Lines write complete.")

        run_status = "success"

    except Exception as exc:
        log.error(f"Pipeline failed: {exc}", exc_info=True)
        run_status = "failed"

    finally:
        duration = time.time() - t0

        # ── Log summary ────────────────────────────────────────────────────
        log.info("=" * 60)
        log.info(f"STEP 1 DONE  status={run_status}  duration={duration:.1f}s")
        log.info(f"  rows_in  = {rows_in:,}")
        log.info(f"  rows_out = {rows_out:,}")
        log.info(f"  removed  = {rows_in - rows_out:,}")
        log.info("=" * 60)

        # ── Write pipeline_runs ────────────────────────────────────────────
        try:
            write_pipeline_run(
                step="clean",
                status=run_status,
                rows_in=rows_in,
                rows_out=rows_out,
                duration_seconds=duration,
            )
        except Exception as pg_exc:
            log.error(f"Failed to write pipeline_runs: {pg_exc}")

        spark.stop()

    if run_status == "failed":
        sys.exit(1)


if __name__ == "__main__":
    main()
