#!/bin/bash
set -e
GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
ok()   { echo -e "${GREEN}[✓]${NC} $1"; }
fail() { echo -e "${RED}[✗]${NC} $1"; exit 1; }

cd /home/hadoop/sales_lambda

PG_JAR=${PG_JAR:-/home/hadoop/hive-2.3.9/lib/postgresql-42.6.0.jar}

SPARK_SUBMIT="spark-submit --master spark://192.168.56.10:7077 \
  --jars $PG_JAR \
  --executor-memory 2g --executor-cores 2 \
  --conf spark.executor.instances=2 \
  --conf spark.driver.memory=1g"

ok "STEP 0 — Generate 200k orders"
python3 step0_generate/generate_orders.py || fail "generate_orders failed"

ok "STEP 1 — Clean Data (Spark → Parquet + JSON Lines for MR)"
$SPARK_SUBMIT step1_clean/clean_data.py || fail "clean_data failed"

ok "STEP 2 — Batch Analytics (Spark SQL → PostgreSQL + Parquet)"
$SPARK_SUBMIT step2_analytics/batch_analytics.py || fail "batch_analytics failed"

ok "STEP 3 — Train Model (Spark MLlib, seller_monthly Parquet → PostgreSQL)"
$SPARK_SUBMIT step3_model/train_model.py || fail "train_model failed"

ok "STEP 4 — Benchmark (Python MR vs Spark → PostgreSQL)"
$SPARK_SUBMIT step4_benchmark/benchmark.py || fail "benchmark failed"

ok "STEP 5 — Dashboard (Streamlit + PostgreSQL)"
nohup streamlit run step5_dashboard/dashboard.py \
  --server.port 8501 --server.address 0.0.0.0 --server.headless true \
  > /tmp/sales_dashboard.log 2>&1 &

ok "=== PIPELINE COMPLETE ==="
echo ""
echo "  Dashboard : http://192.168.56.10:8501"
echo "  YARN UI   : http://192.168.56.10:8088"
echo "  Spark UI  : http://192.168.56.10:8080"
