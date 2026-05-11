# Sales Lambda Pipeline

200k đơn hàng · Hadoop 3.3.1 · Spark 3.4.1 · Python MapReduce · PostgreSQL · Streamlit

## Cấu trúc

```
sales_lambda/
├── step0_generate/     Sinh dữ liệu giả 200k orders → HDFS
├── step1_clean/        Spark: parse + clean → Parquet (HDFS)
├── step2_analytics/    Spark SQL: revenue / monthly / top products → Parquet
├── step3_model/        Spark MLlib: Linear Regression → Parquet + HDFS model
├── step4_benchmark/    Python MR vs Spark SQL benchmark
│   └── mapreduce/      Hadoop Streaming mapper/reducer per job
├── step5_serving/      Import Parquet → PostgreSQL
│   ├── import_to_pg.py
│   └── schema.sql
├── step6_dashboard/    Streamlit query PostgreSQL
├── config/             hive-site.xml
├── run_pipeline.sh     Chạy toàn bộ pipeline
└── start.sh            Khởi động HDFS + YARN + Spark
```

## Chạy nhanh

```bash
# 1. Khởi động services
bash start.sh

# 2. Tạo PostgreSQL database
psql -U postgres -c "CREATE DATABASE sales_db; CREATE USER hadoop WITH PASSWORD 'hadoop'; GRANT ALL ON DATABASE sales_db TO hadoop;"
psql -U hadoop -d sales_db -f step5_serving/schema.sql

# 3. Chạy pipeline
bash run_pipeline.sh

# 4. Xem dashboard
# http://192.168.56.10:8501
```

## Cấu hình PostgreSQL

```bash
export PG_HOST=localhost
export PG_PORT=5432
export PG_DB=sales_db
export PG_USER=hadoop
export PG_PASSWORD=hadoop
```

## HDFS Layout

```
/sales/
├── raw/app/              120k orders (format App)
├── raw/web/               80k orders (format Web)
├── processed/            Parquet sau khi clean
├── processed_csv/        CSV cho MapReduce streaming
├── batch/
│   ├── revenue_by_category/
│   ├── monthly_trend/
│   ├── top_products/
│   ├── predictions/
│   └── benchmark/
└── model/lr_revenue      Spark MLlib model
```
