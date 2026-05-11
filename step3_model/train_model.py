#!/usr/bin/env python3
"""
STEP 3 — train_model.py (Spark MLlib)
Reads pre-computed seller_monthly Parquet (Step 2 output),
trains Linear Regression to predict next month revenue.

Sources:
  /sales/analytics/seller_monthly/  (Parquet from Step 2, 12 features)

Outputs:
  /sales/model/lr_revenue            (Spark ML model)
  /sales/batch/predictions/          (Parquet)
  PostgreSQL: seller_predictions, model_metrics

Usage: spark-submit --master spark://192.168.56.10:7077 train_model.py
"""

import json
import sys
import time
import logging
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger(__name__)

HDFS = "hdfs://192.168.56.10:9000"
PG_CONN = dict(host="localhost", port=5432, dbname="sales_db", user="hadoop", password="hadoop")


def _pg_conn():
    return psycopg2.connect(**PG_CONN)


def create_spark():
    return (SparkSession.builder
            .appName("SalesTrainModel")
            .master("spark://192.168.56.10:7077")
            .config("spark.executor.memory", "2g")
            .config("spark.executor.cores", "2")
            .config("spark.driver.memory", "1g")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate())


def build_features(spark):
    """Read seller_monthly Parquet (Step 2), encode seller_type, fill lag nulls."""
    log.info("Loading seller_monthly features from HDFS …")
    df = spark.read.parquet(f"{HDFS}/sales/analytics/seller_monthly/")

    df = df.withColumn(
        "seller_type_enc",
        F.when(F.col("seller_type") == "high_performer", 2.0)
         .when(F.col("seller_type") == "average_seller", 1.0)
         .otherwise(0.0),
    )

    # Lag/rolling features are null for first months — fill with 0
    fill_cols = ["lag1_revenue", "lag2_revenue", "lag3_revenue",
                 "revenue_ma3", "growth_rate", "cancel_rate"]
    df = df.fillna(0.0, subset=fill_cols)

    # Only rows that have a known next_month_revenue (label)
    df = df.filter(F.col("next_month_revenue").isNotNull())

    log.info(f"  Feature rows: {df.count():,}")
    return df


def write_predictions_to_pg(pred_df):
    """TRUNCATE + INSERT seller_predictions via psycopg2."""
    pdf = pred_df.toPandas()
    conn = _pg_conn()
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE seller_predictions")
        execute_values(
            cur,
            "INSERT INTO seller_predictions (seller_id, year_month, actual_revenue, predicted_revenue) VALUES %s",
            [(r.seller_id, r.year_month, float(r.actual_revenue), float(r.predicted_revenue))
             for r in pdf.itertuples(index=False)],
        )
    conn.commit()
    conn.close()
    log.info(f"  seller_predictions: {len(pdf):,} rows → PostgreSQL")


def write_metrics_to_pg(train_rmse, test_rmse, test_mae, test_r2,
                        n_train, n_test, feature_cols):
    """INSERT one row into model_metrics via psycopg2."""
    conn = _pg_conn()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO model_metrics "
            "(rmse, mae, r2, train_rmse, n_train, n_test, feature_json, run_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (round(test_rmse, 4), round(test_mae, 4), round(test_r2, 6),
             round(train_rmse, 4), int(n_train), int(n_test),
             json.dumps(feature_cols), datetime.utcnow()),
        )
    conn.commit()
    conn.close()
    log.info("  model_metrics → PostgreSQL")


def main():
    log.info("=" * 56)
    log.info("STEP 3 — Train Linear Regression Model")
    log.info("=" * 56)

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    t0 = time.time()

    features_df = build_features(spark)

    # ── Feature engineering ───────────────────────────────────────────────────
    feature_cols = [
        "month", "monthly_revenue", "avg_order_value",
        "cancel_rate", "unique_customers", "active_days",
        "lag1_revenue", "lag2_revenue", "lag3_revenue",
        "revenue_ma3", "growth_rate", "seller_type_enc",
    ]
    label_col = "next_month_revenue"

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw",
                                handleInvalid="skip")
    scaler    = StandardScaler(inputCol="features_raw", outputCol="features",
                               withMean=True, withStd=True)

    df_vec    = assembler.transform(features_df.na.drop(subset=feature_cols + [label_col]))
    sc_model  = scaler.fit(df_vec)
    df_scaled = sc_model.transform(df_vec)

    # ── Train / Test split ────────────────────────────────────────────────────
    train_df = df_scaled.filter(F.col("year") < 2024)
    test_df  = df_scaled.filter(F.col("year") == 2024)

    n_train = train_df.count()
    n_test  = test_df.count()
    log.info(f"Train: {n_train:,}  Test: {n_test:,}")

    # ── Linear Regression ─────────────────────────────────────────────────────
    log.info("Training Linear Regression …")
    lr = LinearRegression(
        featuresCol="features", labelCol=label_col,
        maxIter=50, regParam=0.1, elasticNetParam=0.0,
        predictionCol="predicted_revenue",
    )
    lr_model = lr.fit(train_df)

    # ── Evaluate ──────────────────────────────────────────────────────────────
    evaluator = RegressionEvaluator(labelCol=label_col, predictionCol="predicted_revenue")

    train_rmse = evaluator.setMetricName("rmse").evaluate(lr_model.transform(train_df))
    test_preds = lr_model.transform(test_df)
    test_rmse  = evaluator.setMetricName("rmse").evaluate(test_preds)
    test_mae   = evaluator.setMetricName("mae").evaluate(test_preds)
    test_r2    = evaluator.setMetricName("r2").evaluate(test_preds)

    log.info(f"  Train RMSE: {train_rmse:.2f}")
    log.info(f"  Test  RMSE: {test_rmse:.2f}")
    log.info(f"  Test  MAE:  {test_mae:.2f}")
    log.info(f"  Test  R²:   {test_r2:.4f}")
    log.info(f"  Coefficients: {lr_model.coefficients}")
    log.info(f"  Intercept:    {lr_model.intercept:.2f}")

    # ── Predictions ───────────────────────────────────────────────────────────
    predictions = (
        test_preds
        .select(
            "seller_id", "year_month",
            F.col(label_col).alias("actual_revenue"),
            F.round("predicted_revenue", 2).alias("predicted_revenue"),
        )
        .orderBy("seller_id", "year_month")
    )

    predictions.write.mode("overwrite").parquet(f"{HDFS}/sales/batch/predictions")
    predictions.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        f"{HDFS}/sales/batch/csv/predictions")

    write_predictions_to_pg(predictions)

    # ── Save model ────────────────────────────────────────────────────────────
    lr_model.write().overwrite().save(f"{HDFS}/sales/model/lr_revenue")
    log.info("Model saved → /sales/model/lr_revenue")

    # ── Metrics → PostgreSQL ──────────────────────────────────────────────────
    write_metrics_to_pg(train_rmse, test_rmse, test_mae, test_r2,
                        n_train, n_test, feature_cols)

    log.info(f"\nSTEP 3 DONE — {time.time()-t0:.1f}s")
    log.info(f"Predictions: {predictions.count():,} seller-months")
    spark.stop()


if __name__ == "__main__":
    main()
