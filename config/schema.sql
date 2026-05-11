-- Sales Lambda Pipeline — PostgreSQL Schema
-- Setup: psql -U postgres -c "CREATE DATABASE sales_db; CREATE USER hadoop WITH PASSWORD 'hadoop'; GRANT ALL PRIVILEGES ON DATABASE sales_db TO hadoop;"
-- Apply: psql -U hadoop -d sales_db -f config/schema.sql

-- orders_clean removed: 200k rows live in HDFS Parquet (/sales/processed/)
-- PostgreSQL is serving layer only — aggregated results queried by Streamlit

CREATE TABLE IF NOT EXISTS revenue_by_category (
    category        VARCHAR(30) PRIMARY KEY,
    total_revenue   NUMERIC(15,2),
    order_count     BIGINT,
    avg_revenue     NUMERIC(10,2),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS monthly_trend (
    year_month          VARCHAR(7) PRIMARY KEY,
    total_revenue       NUMERIC(15,2),
    order_count         BIGINT,
    mom_growth_pct      NUMERIC(8,2),
    updated_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS top_products (
    rank            SMALLINT PRIMARY KEY,
    product_id      VARCHAR(10),
    product_name    VARCHAR(50),
    order_count     BIGINT,
    total_revenue   NUMERIC(15,2),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS kpi_summary (
    metric_name     VARCHAR(50) PRIMARY KEY,
    metric_value    NUMERIC(20,4),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS seller_monthly (
    seller_id           VARCHAR(10),
    year_month          VARCHAR(7),
    seller_type         VARCHAR(20),
    monthly_revenue     NUMERIC(15,2),
    avg_order_value     NUMERIC(10,2),
    cancel_rate         NUMERIC(6,4),
    unique_customers    INTEGER,
    active_days         INTEGER,
    lag1_revenue        NUMERIC(15,2),
    lag2_revenue        NUMERIC(15,2),
    lag3_revenue        NUMERIC(15,2),
    revenue_ma3         NUMERIC(15,2),
    growth_rate         NUMERIC(10,4),
    next_month_revenue  NUMERIC(15,2),
    year                SMALLINT,
    month               SMALLINT,
    PRIMARY KEY (seller_id, year_month)
);

CREATE TABLE IF NOT EXISTS seller_alerts (
    seller_id       VARCHAR(10),
    year_month      VARCHAR(7),
    alert_type      VARCHAR(30),
    prev_revenue    NUMERIC(15,2),
    curr_revenue    NUMERIC(15,2),
    drop_pct        NUMERIC(8,2),
    created_at      TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (seller_id, year_month, alert_type)
);

CREATE TABLE IF NOT EXISTS seller_predictions (
    seller_id           VARCHAR(10),
    year_month          VARCHAR(7),
    actual_revenue      NUMERIC(15,2),
    predicted_revenue   NUMERIC(15,2),
    PRIMARY KEY (seller_id, year_month)
);

CREATE TABLE IF NOT EXISTS model_metrics (
    id              SERIAL PRIMARY KEY,
    rmse            NUMERIC(12,4),
    mae             NUMERIC(12,4),
    r2              NUMERIC(8,6),
    train_rmse      NUMERIC(12,4),
    n_train         INTEGER,
    n_test          INTEGER,
    feature_json    TEXT,
    run_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS benchmark_results (
    id              SERIAL PRIMARY KEY,
    task            VARCHAR(30),
    engine          VARCHAR(20),
    avg_seconds     NUMERIC(10,3),
    min_seconds     NUMERIC(10,3),
    n_jobs          INTEGER,
    n_hdfs_writes   INTEGER,
    runs            INTEGER,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id                  SERIAL PRIMARY KEY,
    step                VARCHAR(20),
    status              VARCHAR(10),
    rows_in             BIGINT,
    rows_out            BIGINT,
    duration_seconds    NUMERIC(10,2),
    run_at              TIMESTAMP DEFAULT NOW()
);
