#!/usr/bin/env python3
"""
STEP 4 — benchmark.py
Chạy 3 bài toán analytics bằng Python MapReduce (Hadoop Streaming)
và Spark SQL, so sánh thời gian → ghi /sales/batch/benchmark/comparison.txt

Usage: python3 step4_benchmark/benchmark.py [--mr-only | --spark-only]
"""

import argparse
import logging
import os
import subprocess
import sys
import time
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger(__name__)

HDFS           = "hdfs://192.168.56.10:9000"
STREAMING_JAR  = "/home/hadoop/hadoop-3.3.1/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar"
MR_DIR         = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mapreduce")
CSV_INPUT      = f"{HDFS}/sales/processed_csv/"
MR_OUTPUT_BASE = f"{HDFS}/sales/batch/benchmark/mr_output"
LOCAL_RESULT   = "/tmp/sales_benchmark.txt"
HDFS_RESULT    = f"{HDFS}/sales/batch/benchmark/comparison.txt"

PG_CONN = dict(host="localhost", port=5432, dbname="sales_db", user="hadoop", password="hadoop")

# n_jobs / n_hdfs_writes per task (architecture constants)
TASK_ARCH = {
    "revenue_by_category": {"mapreduce": (1, 1), "spark": (1, 0)},
    "monthly_trend":        {"mapreduce": (2, 2), "spark": (1, 0)},
    "top_products":         {"mapreduce": (1, 2), "spark": (1, 0)},
}

HADOOP_ENV = {
    **os.environ,
    "HADOOP_HOME":     os.environ.get("HADOOP_HOME", "/home/hadoop/hadoop-3.3.1"),
    "HADOOP_CONF_DIR": os.environ.get("HADOOP_CONF_DIR", "/home/hadoop/hadoop-3.3.1/etc/hadoop"),
    "JAVA_HOME":       os.environ.get("JAVA_HOME", "/usr/lib/jvm/java-11-openjdk-amd64"),
}


# ─────────────────────────────────────────────────────────────────────────────
# Python MapReduce via Hadoop Streaming
# ─────────────────────────────────────────────────────────────────────────────
def run_streaming_job(job_name, mapper, reducer, output_dir):
    subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", output_dir],
                   capture_output=True, env=HADOOP_ENV)

    cmd = [
        "hadoop", "jar", STREAMING_JAR,
        "-D", "mapreduce.job.name=" + job_name,
        "-D", "mapreduce.job.reduces=4",
        "-D", "mapreduce.framework.name=yarn",
        "-files", f"{mapper},{reducer}",
        "-mapper",  "mapper.py",
        "-reducer", "reducer.py",
        "-input",   CSV_INPUT,
        "-output",  output_dir,
    ]

    log.info(f"  Submitting: {job_name}")
    t0 = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True, env=HADOOP_ENV)
    elapsed = time.time() - t0

    for line in result.stderr.splitlines():
        if any(k in line for k in ("map =", "reduce =", "Launched", "Stage", "ERROR", "FAILED")):
            log.info(f"    {line.strip()}")

    if result.returncode != 0:
        log.error(f"  FAILED: {result.stderr[-1000:]}")
        return -1.0

    log.info(f"  {job_name} OK — {elapsed:.1f}s")
    return elapsed


def run_mapreduce():
    log.info("=== [Python MapReduce / Hadoop Streaming] ===")
    t0 = time.time()

    jobs = [
        ("MR-RevenueByCategory",
         f"{MR_DIR}/revenue_by_category/mapper.py",
         f"{MR_DIR}/revenue_by_category/reducer.py",
         f"{MR_OUTPUT_BASE}/revenue_by_category"),
        ("MR-MonthlyTrend",
         f"{MR_DIR}/monthly_trend/mapper.py",
         f"{MR_DIR}/monthly_trend/reducer.py",
         f"{MR_OUTPUT_BASE}/monthly_trend"),
        ("MR-TopProducts",
         f"{MR_DIR}/top_products/mapper.py",
         f"{MR_DIR}/top_products/reducer.py",
         f"{MR_OUTPUT_BASE}/top_products"),
    ]

    times = {}
    for job_name, mapper, reducer, output in jobs:
        subprocess.run(["chmod", "+x", mapper, reducer], capture_output=True)
        times[job_name] = run_streaming_job(job_name, mapper, reducer, output)

    total = time.time() - t0
    log.info(f"=== MapReduce tổng: {total:.1f}s ===")
    return total, times


# ─────────────────────────────────────────────────────────────────────────────
# Spark SQL
# ─────────────────────────────────────────────────────────────────────────────
def run_spark():
    log.info("=== [Spark SQL] ===")
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        log.error("PySpark không tìm thấy")
        return -1.0, {}

    spark = (SparkSession.builder
             .appName("SalesBenchmarkSpark")
             .master("spark://192.168.56.10:7077")
             .config("spark.executor.memory", "2g")
             .config("spark.executor.cores", "2")
             .config("spark.sql.adaptive.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    from pyspark.sql import functions as F
    CATEGORIES = ["electronics","clothing","home_garden","sports","beauty",
                  "books","food_drink","toys","automotive","health"]
    cat_array = F.array(*[F.lit(c) for c in CATEGORIES])
    df = spark.read.parquet(f"{HDFS}/sales/processed/")
    df = df.withColumn(
        "category",
        F.element_at(
            cat_array,
            (F.conv(F.substring(F.md5(F.col("seller_id")), 1, 8), 16, 10)
             .cast("bigint") % 10).cast("int") + 1,
        ),
    ).withColumn(
        "product_id",
        F.concat(F.lit("P"), F.lpad(
            (F.conv(F.substring(F.md5(F.col("order_id")), 1, 8), 16, 10)
             .cast("bigint") % 500 + 1).cast("string"), 5, "0",
        )),
    )
    df.createOrReplaceTempView("sales")

    queries = {
        "Spark-RevenueByCategory": """
            SELECT category, ROUND(SUM(total_amount),2) AS total_revenue, COUNT(*) AS items
            FROM sales GROUP BY category ORDER BY total_revenue DESC
        """,
        "Spark-MonthlyTrend": """
            SELECT year_month, COUNT(DISTINCT order_id) AS orders,
                   ROUND(SUM(total_amount),2) AS total_revenue
            FROM sales GROUP BY year_month ORDER BY year_month
        """,
        "Spark-TopProducts": """
            SELECT product_id, ROUND(SUM(total_amount),2) AS total_revenue, COUNT(*) AS order_count
            FROM sales GROUP BY product_id
            ORDER BY order_count DESC LIMIT 10
        """,
    }

    times = {}
    t0_total = time.time()
    for name, sql in queries.items():
        t0 = time.time()
        count = spark.sql(sql).count()
        elapsed = time.time() - t0
        times[name] = elapsed
        log.info(f"  {name}: {elapsed:.1f}s  ({count} rows)")

    total = time.time() - t0_total
    log.info(f"=== Spark SQL tổng: {total:.1f}s ===")
    spark.stop()
    return total, times


# ─────────────────────────────────────────────────────────────────────────────
# PostgreSQL write
# ─────────────────────────────────────────────────────────────────────────────
def write_benchmark_to_pg(mr_times, spark_times):
    """INSERT benchmark_results rows (one per engine×task) via psycopg2."""
    # Normalize key → task name: "MR-RevenueByCategory" → "revenue_by_category"
    def normalize(key):
        return key.split("-", 1)[1].lower().replace("revenuebycat", "revenue_by_cat") \
                  .replace("revenuebycategory", "revenue_by_category") \
                  .replace("monthlytrend", "monthly_trend") \
                  .replace("topproducts", "top_products")

    rows = []
    for key, secs in mr_times.items():
        if secs < 0:
            continue
        task = normalize(key)
        n_jobs, n_writes = TASK_ARCH.get(task, {}).get("mapreduce", (1, 1))
        rows.append((task, "mapreduce", round(secs, 3), round(secs, 3), n_jobs, n_writes, 1))
    for key, secs in spark_times.items():
        if secs < 0:
            continue
        task = normalize(key)
        n_jobs, n_writes = TASK_ARCH.get(task, {}).get("spark", (1, 0))
        rows.append((task, "spark", round(secs, 3), round(secs, 3), n_jobs, n_writes, 1))

    if not rows:
        return

    conn = psycopg2.connect(**PG_CONN)
    with conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO benchmark_results "
            "(task, engine, avg_seconds, min_seconds, n_jobs, n_hdfs_writes, runs) VALUES %s",
            rows,
        )
    conn.commit()
    conn.close()
    log.info(f"  benchmark_results: {len(rows)} rows → PostgreSQL")


# ─────────────────────────────────────────────────────────────────────────────
# So sánh & lưu
# ─────────────────────────────────────────────────────────────────────────────
def save_results(mr_total, mr_times, spark_total, spark_times):
    ts      = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    speedup = mr_total / spark_total if spark_total > 0 and mr_total > 0 else -1

    lines = [
        f"benchmark_timestamp,{ts}",
        f"mr_total_sec,{mr_total:.2f}",
        f"spark_total_sec,{spark_total:.2f}",
        f"speedup_ratio,{speedup:.2f}",
    ]
    for k, v in mr_times.items():
        lines.append(f"mr_{k.lower().replace('-','_')},{v:.2f}")
    for k, v in spark_times.items():
        lines.append(f"spark_{k.lower().replace('-','_')},{v:.2f}")

    log.info("\n" + "=" * 56)
    log.info("BENCHMARK — Python MapReduce vs Spark SQL")
    log.info("=" * 56)
    log.info(f"  MapReduce tổng : {mr_total:.1f}s")
    log.info(f"  Spark SQL tổng : {spark_total:.1f}s")
    if speedup > 0:
        log.info(f"  Speedup        : {speedup:.1f}×")
    log.info("=" * 56)

    with open(LOCAL_RESULT, "w") as f:
        f.write("\n".join(lines) + "\n")

    subprocess.run(["hdfs", "dfs", "-mkdir", "-p",
                    f"{HDFS}/sales/batch/benchmark"], capture_output=True, env=HADOOP_ENV)
    subprocess.run(["hdfs", "dfs", "-put", "-f", LOCAL_RESULT, HDFS_RESULT],
                   capture_output=True, env=HADOOP_ENV)
    log.info(f"Saved → {HDFS_RESULT}")

    try:
        write_benchmark_to_pg(mr_times, spark_times)
    except Exception as e:
        log.warning(f"Could not write benchmark_results to PG: {e}")


def parse_args():
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group()
    g.add_argument("--mr-only",    action="store_true")
    g.add_argument("--spark-only", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    mr_total, mr_times       = (0.0, {})
    spark_total, spark_times = (0.0, {})

    if not args.spark_only:
        mr_total, mr_times = run_mapreduce()
    if not args.mr_only:
        spark_total, spark_times = run_spark()

    save_results(mr_total, mr_times, spark_total, spark_times)


if __name__ == "__main__":
    main()
