#!/usr/bin/env python3
"""
STEP 2 — batch_analytics.py
Read Parquet from /sales/processed/, compute benchmark analytics,
seller monthly features, KPI summary, and seller alerts.
Write results to both PostgreSQL and HDFS Parquet.

Usage: spark-submit --master spark://192.168.56.10:7077 batch_analytics.py
"""

import sys
import time
import logging
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Global constants
# ---------------------------------------------------------------------------
HDFS = "hdfs://192.168.56.10:9000"
SPARK_MASTER = "spark://192.168.56.10:7077"
PG_URL = "jdbc:postgresql://localhost:5432/sales_db"
PG_PROPS = {"user": "hadoop", "password": "hadoop", "driver": "org.postgresql.Driver"}

CATEGORIES = [
    "electronics", "clothing", "home_garden", "sports", "beauty",
    "books", "food_drink", "toys", "automotive", "health",
]


# ---------------------------------------------------------------------------
# PostgreSQL helpers
# ---------------------------------------------------------------------------
def _pg_conn():
    return psycopg2.connect(
        host="localhost", port=5432, dbname="sales_db",
        user="hadoop", password="hadoop",
    )


def truncate_insert(df, table):
    """Collect to pandas, TRUNCATE + INSERT via psycopg2."""
    pdf = df.toPandas()
    conn = _pg_conn()
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table}")
        cols = list(pdf.columns)
        execute_values(
            cur,
            f"INSERT INTO {table} ({','.join(cols)}) VALUES %s",
            [tuple(r) for r in pdf.itertuples(index=False)],
        )
    conn.commit()
    conn.close()


def upsert(df, table, pk_cols):
    """Collect to pandas, INSERT ... ON CONFLICT DO UPDATE via psycopg2."""
    pdf = df.toPandas()
    conn = _pg_conn()
    cols = list(pdf.columns)
    non_pk = [c for c in cols if c not in pk_cols]
    update_set = ", ".join(f"{c}=EXCLUDED.{c}" for c in non_pk)
    sql = (
        f"INSERT INTO {table} ({','.join(cols)}) VALUES %s "
        f"ON CONFLICT ({','.join(pk_cols)}) DO UPDATE SET {update_set}"
    )
    with conn.cursor() as cur:
        execute_values(cur, sql, [tuple(r) for r in pdf.itertuples(index=False)])
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
def create_spark():
    return (
        SparkSession.builder
        .appName("SalesBatchAnalytics")
        .master(SPARK_MASTER)
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# MD5-based derived columns
# ---------------------------------------------------------------------------
def add_category_and_product(df):
    """Assign category (from seller_id) and product_id (from order_id)."""
    cat_array = F.array(*[F.lit(c) for c in CATEGORIES])

    df = df.withColumn(
        "category",
        F.element_at(
            cat_array,
            (
                F.conv(F.substring(F.md5(F.col("seller_id")), 1, 8), 16, 10)
                .cast("bigint") % 10
            ).cast("int") + 1,   # element_at is 1-based
        ),
    )

    df = df.withColumn(
        "product_id",
        F.concat(
            F.lit("P"),
            F.lpad(
                (
                    F.conv(F.substring(F.md5(F.col("order_id")), 1, 8), 16, 10)
                    .cast("bigint") % 500 + 1
                ).cast("string"),
                5, "0",
            ),
        ),
    )
    return df


# ---------------------------------------------------------------------------
# Benchmark analytics
# ---------------------------------------------------------------------------
def compute_revenue_by_category(df):
    log.info("Computing revenue_by_category …")
    result = (
        df.groupBy("category")
        .agg(
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.count("order_id").alias("order_count"),
            F.round(F.avg("total_amount"), 2).alias("avg_revenue"),
        )
    )
    result.write.mode("overwrite").parquet(f"{HDFS}/sales/analytics/revenue_by_category/")
    truncate_insert(result, "revenue_by_category")
    log.info(f"  revenue_by_category: {result.count()} rows")
    return result


def compute_monthly_trend(df):
    log.info("Computing monthly_trend …")
    trend = (
        df.groupBy("year_month")
        .agg(
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.count("order_id").alias("order_count"),
        )
    )
    w = Window.orderBy("year_month")
    prev_rev = F.lag("total_revenue", 1).over(w)
    trend = trend.withColumn(
        "mom_growth_pct",
        F.round(
            (F.col("total_revenue") - prev_rev) / prev_rev * 100,
            2,
        ),
    )
    trend.write.mode("overwrite").parquet(f"{HDFS}/sales/analytics/monthly_trend/")
    upsert(trend, "monthly_trend", ["year_month"])
    log.info(f"  monthly_trend: {trend.count()} rows")
    return trend


def compute_top_products(df):
    log.info("Computing top_products …")
    top = (
        df.groupBy("product_id")
        .agg(
            F.count("order_id").alias("order_count"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        )
        .orderBy(F.desc("order_count"))
        .limit(10)
    )
    top = top.withColumn(
        "product_name",
        F.concat(F.lit("Product "), F.col("product_id")),
    )
    w_rank = Window.partitionBy(F.lit(1)).orderBy(F.desc("order_count"))
    top = top.withColumn("rank", F.row_number().over(w_rank))

    top.write.mode("overwrite").parquet(f"{HDFS}/sales/analytics/top_products/")
    truncate_insert(top, "top_products")
    log.info(f"  top_products: {top.count()} rows")
    return top


# ---------------------------------------------------------------------------
# Seller monthly features
# ---------------------------------------------------------------------------
def compute_seller_monthly(df_completed, df_all):
    log.info("Computing seller_monthly_features …")

    # cancel_rate requires all statuses
    cancel_stats = (
        df_all.groupBy("seller_id", "year_month")
        .agg(
            F.round(
                F.sum(F.when(F.col("status") == "cancelled", 1).otherwise(0)) /
                F.count("order_id") * 100,
                4,
            ).alias("cancel_rate")
        )
    )

    seller_agg = (
        df_completed.groupBy("seller_id", "year_month", "seller_type")
        .agg(
            F.round(F.sum("total_amount"), 2).alias("monthly_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.countDistinct("order_date").alias("active_days"),
        )
    )

    seller_agg = seller_agg.join(cancel_stats, on=["seller_id", "year_month"], how="left")

    seller_agg = (
        seller_agg
        .withColumn("year", F.substring("year_month", 1, 4).cast("int"))
        .withColumn("month", F.substring("year_month", 6, 2).cast("int"))
    )

    w = Window.partitionBy("seller_id").orderBy("year_month")
    seller_agg = (
        seller_agg
        .withColumn("lag1_revenue", F.lag("monthly_revenue", 1).over(w))
        .withColumn("lag2_revenue", F.lag("monthly_revenue", 2).over(w))
        .withColumn("lag3_revenue", F.lag("monthly_revenue", 3).over(w))
    )

    seller_agg = seller_agg.withColumn(
        "revenue_ma3",
        F.when(
            F.col("lag1_revenue").isNotNull() &
            F.col("lag2_revenue").isNotNull() &
            F.col("lag3_revenue").isNotNull(),
            F.round(
                (F.col("lag1_revenue") + F.col("lag2_revenue") + F.col("lag3_revenue")) / 3,
                2,
            ),
        ).otherwise(F.lit(None).cast("double")),
    )

    seller_agg = seller_agg.withColumn(
        "growth_rate",
        F.when(
            F.col("lag1_revenue").isNotNull(),
            F.round(
                (F.col("monthly_revenue") - F.col("lag1_revenue")) /
                F.col("lag1_revenue") * 100,
                4,
            ),
        ).otherwise(F.lit(None).cast("double")),
    )

    seller_agg = seller_agg.withColumn(
        "next_month_revenue",
        F.lead("monthly_revenue", 1).over(w),
    )

    seller_agg.write.mode("overwrite").parquet(f"{HDFS}/sales/analytics/seller_monthly/")
    upsert(seller_agg, "seller_monthly", ["seller_id", "year_month"])
    log.info(f"  seller_monthly: {seller_agg.count()} rows")
    return seller_agg


# ---------------------------------------------------------------------------
# KPI summary
# ---------------------------------------------------------------------------
def compute_kpi_summary(df_completed, df_all):
    log.info("Computing KPI summary …")

    total_gmv = df_completed.agg(F.round(F.sum("total_amount"), 2)).collect()[0][0]
    total_orders = df_completed.count()
    unique_customers = df_completed.select("customer_id").distinct().count()
    active_sellers = df_completed.select("seller_id").distinct().count()
    avg_order_value = round(total_gmv / total_orders, 2) if total_orders else 0.0

    total_all = df_all.count()
    cancelled = df_all.filter(F.col("status") == "cancelled").count()
    cancel_rate = round(cancelled / total_all * 100, 4) if total_all else 0.0

    rows = [
        ("total_gmv",         float(total_gmv)),
        ("total_orders",      float(total_orders)),
        ("unique_customers",  float(unique_customers)),
        ("active_sellers",    float(active_sellers)),
        ("avg_order_value",   float(avg_order_value)),
        ("cancel_rate",       float(cancel_rate)),
    ]

    spark = SparkSession.getActiveSession()
    kpi_df = spark.createDataFrame(rows, ["metric_name", "metric_value"])

    kpi_df.write.mode("overwrite").parquet(f"{HDFS}/sales/analytics/kpi_summary/")
    truncate_insert(kpi_df, "kpi_summary")
    log.info(f"  kpi_summary: {len(rows)} rows")
    return kpi_df


# ---------------------------------------------------------------------------
# Seller alerts
# ---------------------------------------------------------------------------
def compute_seller_alerts(seller_monthly_df):
    log.info("Computing seller_alerts …")

    w = Window.partitionBy("seller_id").orderBy("year_month")
    alerts = seller_monthly_df.withColumn(
        "prev_revenue", F.lag("monthly_revenue", 1).over(w)
    )
    alerts = alerts.withColumn(
        "drop_pct",
        F.when(
            F.col("prev_revenue").isNotNull() & (F.col("prev_revenue") > 0),
            F.round(
                (F.col("prev_revenue") - F.col("monthly_revenue")) /
                F.col("prev_revenue") * 100,
                2,
            ),
        ).otherwise(F.lit(None).cast("double")),
    )
    alerts = (
        alerts.filter(F.col("drop_pct") > 20)
        .withColumn("alert_type", F.lit("revenue_drop"))
        .withColumn("curr_revenue", F.col("monthly_revenue"))
        .select(
            "seller_id", "year_month", "alert_type",
            "prev_revenue", "curr_revenue", "drop_pct",
        )
    )

    alerts.write.mode("overwrite").parquet(f"{HDFS}/sales/analytics/seller_alerts/")
    truncate_insert(alerts, "seller_alerts")
    log.info(f"  seller_alerts: {alerts.count()} rows")
    return alerts


# ---------------------------------------------------------------------------
# Pipeline run logging
# ---------------------------------------------------------------------------
def log_pipeline_run(step, status, rows_in, rows_out, duration):
    conn = _pg_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_runs (step, status, rows_in, rows_out, duration, run_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (step, status, rows_in, rows_out, round(duration, 2), datetime.now()),
        )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    log.info("=" * 60)
    log.info("STEP 2 — Batch Analytics")
    log.info("=" * 60)

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    t0 = time.time()

    rows_out_total = 0
    status = "success"

    try:
        # Read all statuses (for cancel_rate)
        df_all = spark.read.parquet(f"{HDFS}/sales/processed/")
        df_all = add_category_and_product(df_all)

        # Completed orders only
        df = df_all.filter(F.col("status") == "completed")
        rows_in = df.count()
        log.info(f"Loaded {rows_in:,} completed orders from /sales/processed/")

        # Benchmark analytics
        rev_cat = compute_revenue_by_category(df)
        rows_out_total += rev_cat.count()

        monthly = compute_monthly_trend(df)
        rows_out_total += monthly.count()

        top_prod = compute_top_products(df)
        rows_out_total += top_prod.count()

        # Seller monthly features
        seller_monthly = compute_seller_monthly(df, df_all)
        rows_out_total += seller_monthly.count()

        # KPI summary
        kpi = compute_kpi_summary(df, df_all)
        rows_out_total += kpi.count()

        # Seller alerts
        alerts = compute_seller_alerts(seller_monthly)
        rows_out_total += alerts.count()

    except Exception as exc:
        status = "failed"
        log.error(f"Pipeline failed: {exc}", exc_info=True)
        raise
    finally:
        duration = time.time() - t0
        try:
            log_pipeline_run(
                step="analytics",
                status=status,
                rows_in=rows_in if status == "success" else 0,
                rows_out=rows_out_total,
                duration=duration,
            )
        except Exception as pg_exc:
            log.warning(f"Could not write pipeline_run record: {pg_exc}")

    log.info(f"\nSTEP 2 DONE — {duration:.1f}s  |  rows_out={rows_out_total:,}")
    spark.stop()


if __name__ == "__main__":
    main()
