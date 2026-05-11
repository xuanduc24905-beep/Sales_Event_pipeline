# Sales Event Analysis Pipeline

Hệ thống xử lý và phân tích dữ liệu bán hàng quy mô lớn theo kiến trúc **Batch Lambda**, triển khai trên cụm Hadoop + Spark single-node (VirtualBox).

---

## Tổng quan

| Thông số | Giá trị |
|---|---|
| Số đơn hàng | 200.000 (giả lập) |
| Thời gian dữ liệu | 2022-01 → 2024-12 (3 năm) |
| Số seller | 200 |
| Nguồn dữ liệu | 2 nguồn: App (120k) + Web (80k) |
| Môi trường | VirtualBox · Ubuntu 20.04 · 2 CPU · 8GB RAM |

---

## Kiến trúc hệ thống

```
[Data Sources]
  App JSON (120k)  ──┐
  Web JSON (80k)   ──┴──► HDFS /sales/raw/
                              │
                         [Step 1 - Spark]
                         Parse, Dedup, Clean
                         Outlier Removal (IQR×3)
                              │
                    ┌─────────┴──────────┐
               HDFS Parquet          JSON Lines
            /sales/processed/    /sales/processed_csv/
                    │                    │
           [Step 2 - Spark SQL]   [Step 4 - MapReduce]
           Batch Analytics        Hadoop Streaming
                    │                    │
             ┌──────┴──────┐             │
          Parquet        PostgreSQL ◄────┘
        (analytics/)    (serving layer)
                    │
           [Step 3 - Spark MLlib]
           Linear Regression
           next_month_revenue
                    │
              seller_predictions ──► PostgreSQL
              model_metrics      ──► PostgreSQL
                                         │
                                  [Step 5 - Streamlit]
                                  Dashboard · port 8501
```

**Nguyên tắc thiết kế:**
- **HDFS Parquet** = source of truth cho big data (raw + processed)
- **PostgreSQL** = serving layer chỉ chứa aggregated results (Streamlit query)
- Spark không bao giờ đọc ngược lại từ PostgreSQL

---

## Tech Stack

| Thành phần | Version | Vai trò |
|---|---|---|
| Hadoop HDFS | 3.3.1 | Distributed storage |
| YARN | 3.3.1 | Resource management |
| Apache Spark | 3.4.1 | Batch processing + ML |
| Python MapReduce | — | Hadoop Streaming benchmark |
| PostgreSQL | 14 | Serving layer (dashboard queries) |
| Streamlit | latest | Interactive dashboard |
| Plotly | latest | Charts |
| psycopg2 | latest | Python → PostgreSQL connector |
| PySpark MLlib | 3.4.1 | Linear Regression |

---

## Cấu trúc thư mục

```
sales_lambda/
├── config/
│   ├── db.conf                    # PostgreSQL connection config
│   ├── hive-site.xml
│   └── schema.sql                 # PostgreSQL table definitions
├── step0_generate/
│   └── generate_orders.py         # Sinh 200k đơn hàng giả lập → HDFS
├── step1_clean/
│   └── clean_data.py              # Spark: parse + clean → Parquet + JSON Lines
├── step2_analytics/
│   └── batch_analytics.py         # Spark SQL: analytics → PostgreSQL + Parquet
├── step3_model/
│   └── train_model.py             # Spark MLlib: Linear Regression → PostgreSQL
├── step4_benchmark/
│   ├── benchmark.py               # Driver: MR vs Spark → PostgreSQL
│   └── mapreduce/
│       ├── revenue_by_category/   # mapper.py + reducer.py
│       ├── monthly_trend/         # mapper.py + reducer.py
│       └── top_products/          # mapper.py + reducer.py
├── step5_dashboard/
│   └── dashboard.py               # Streamlit app (query PostgreSQL)
├── run_pipeline.sh                # Chạy toàn bộ pipeline
└── start.sh                       # Khởi động HDFS + YARN + Spark
```

---

## HDFS Layout

```
/sales/
├── raw/
│   ├── source=app/           120k orders (JSON, format App: ts, slr, st, tot, items)
│   └── source=web/            80k orders (JSON, format Web: order_id, order_date, ...)
├── processed/                 Parquet sau clean (partitioned by data_source)
├── processed_csv/             JSON Lines (input cho Hadoop Streaming)
├── analytics/
│   ├── revenue_by_category/   Parquet (10 rows)
│   ├── monthly_trend/         Parquet (36 rows)
│   ├── top_products/          Parquet (10 rows)
│   ├── kpi_summary/           Parquet (6 rows)
│   ├── seller_monthly/        Parquet (~4.800 rows, 12 features)
│   └── seller_alerts/         Parquet (sellers revenue drop >20%)
├── batch/
│   ├── predictions/           Parquet (seller_predictions 2024)
│   ├── csv/predictions/       CSV export
│   └── benchmark/
│       ├── comparison.txt     MR vs Spark timing
│       └── mr_output/         MapReduce job outputs
└── model/
    └── lr_revenue/            Spark MLlib LinearRegressionModel
```

---

## PostgreSQL Schema (Serving Layer)

Chỉ chứa aggregated results nhỏ để Streamlit query. Không chứa raw data.

| Bảng | Rows | Mô tả |
|---|---|---|
| `revenue_by_category` | 10 | Doanh thu theo 10 danh mục sản phẩm |
| `monthly_trend` | 36 | Doanh thu + MoM growth% theo từng tháng |
| `top_products` | 10 | Top 10 sản phẩm theo số đơn hàng |
| `kpi_summary` | 6 | Total GMV, orders, customers, sellers, AOV, cancel rate |
| `seller_monthly` | ~4.800 | Features/tháng/seller (lag, cancel rate, MA3, growth rate) |
| `seller_alerts` | biến động | Seller có revenue drop >20% MoM |
| `seller_predictions` | ~2.400 | Actual vs Predicted revenue 2024 |
| `model_metrics` | log | RMSE, MAE, R², n_train, n_test, feature list |
| `benchmark_results` | log | MR vs Spark timing per task |
| `pipeline_runs` | log | Lịch sử chạy (step, status, rows_in, rows_out, duration) |

---

## Chi tiết từng Step

### Step 0 — Generate Data

Sinh **200.000 đơn hàng** giả lập từ 2 nguồn với format khác nhau:

- **App** (120k): field viết tắt — `oid`, `ts` (unix timestamp), `slr`, `st`, `tot`, `items[]`
- **Web** (80k): field đầy đủ + timezone — `order_id`, `order_date` (ISO 8601 +07:00), `metadata._retry`

Cố tình nhúng lỗi để test cleaning pipeline:
- Duplicate records (`_dup=True`, cross-source duplicates)
- Null values (`total_amount`, `region`, `customer_id`)
- Status aliases: `done`, `paid`, `success`, `cancel`, `refund`, `in_progress`, ...
- Outliers trong `total_amount`

---

### Step 1 — Clean Data (Spark)

**Input:** `/sales/raw/`
**Output:** `/sales/processed/` (Parquet), `/sales/processed_csv/` (JSON Lines)

Quy trình xử lý:

1. **Parse App** — map field tắt sang canonical, convert `from_unixtime(ts)`, filter `_dup=True`
2. **Parse Web** — strip timezone suffix, filter `metadata._retry=True`
3. **Union** 2 nguồn theo schema chuẩn
4. **Normalize status** — 15+ aliases → 4 canonical: `completed`, `cancelled`, `returned`, `pending`
5. **Dedup** — `Window.partitionBy("order_id").orderBy(null_priority, ts.desc())`, giữ row_number=1
6. **Date filter** — 2022-01-01 → 2024-12-31, `total_amount > 0`
7. **Outlier removal** — IQR×3 trên `total_amount` (approxQuantile)
8. **Seller enrichment** — broadcast join với 200-seller lookup, fill null region, thêm `seller_type`
9. **Derived columns** — `year_month`, `year`, `month`

Ghi Parquet phân vùng theo `data_source`. Ghi JSON Lines (`df.write.json()`) cho MapReduce Streaming.
Ghi `pipeline_runs` log vào PostgreSQL.

| Metric | Giá trị |
|---|---|
| rows_in (combined) | ~200.000 |
| rows_out (sau clean) | ~185.000 |

---

### Step 2 — Batch Analytics (Spark SQL)

**Input:** `/sales/processed/` (Parquet)
**Output:** PostgreSQL + `/sales/analytics/` (Parquet)

Tính 6 bảng analytics song song:

**1. revenue_by_category**
- Assign category từ MD5(seller_id) % 10 → 10 categories
- GroupBy category: SUM revenue, COUNT orders, AVG revenue

**2. monthly_trend**
- GroupBy year_month: SUM revenue, COUNT orders
- MoM growth% = `Window.orderBy("year_month").lag(1)` — 1 DAG duy nhất

**3. top_products**
- Assign product_id từ MD5(order_id) % 500
- GroupBy product_id: COUNT orders, SUM revenue → top 10

**4. seller_monthly** (12 features/seller/tháng)
- monthly_revenue, avg_order_value, unique_customers, active_days
- cancel_rate (join với df_all, tất cả statuses)
- lag1/2/3_revenue, revenue_ma3, growth_rate — `Window.partitionBy("seller_id").orderBy("year_month")`
- next_month_revenue — `Window.lead(1)` (label cho ML)

**5. kpi_summary**
- Total GMV, total orders, unique customers, active sellers, avg order value, cancel rate

**6. seller_alerts**
- Seller có revenue drop >20% so với tháng trước
- alert_type = "revenue_drop"

---

### Step 3 — Train Model (Spark MLlib)

**Input:** `/sales/analytics/seller_monthly/` (Parquet từ Step 2)
**Output:** `/sales/model/lr_revenue/`, PostgreSQL `seller_predictions` + `model_metrics`

**Pipeline ML:**

```
seller_monthly Parquet
    │
    ├── Encode seller_type (high=2, avg=1, low=0)
    ├── Fill nulls lag/MA features = 0 (first months)
    ├── Filter next_month_revenue IS NOT NULL
    │
    ├── VectorAssembler (12 features)
    ├── StandardScaler (withMean=True, withStd=True)
    │
    ├── Train: year < 2024
    └── Test:  year == 2024
         │
         └── LinearRegression (maxIter=50, regParam=0.1)
```

**12 Features:**

| Feature | Mô tả |
|---|---|
| `month` | Tháng trong năm (1-12), capture seasonality |
| `monthly_revenue` | Doanh thu tháng hiện tại |
| `avg_order_value` | Giá trị đơn hàng trung bình |
| `cancel_rate` | Tỉ lệ đơn hủy (tín hiệu tiêu cực) |
| `unique_customers` | Số khách hàng unique |
| `active_days` | Số ngày có đơn hàng |
| `lag1_revenue` | Doanh thu tháng trước (predictor mạnh nhất) |
| `lag2_revenue` | Doanh thu 2 tháng trước |
| `lag3_revenue` | Doanh thu 3 tháng trước |
| `revenue_ma3` | Moving average 3 tháng (giảm nhiễu) |
| `growth_rate` | Tốc độ tăng trưởng MoM gần nhất |
| `seller_type_enc` | high=2, average=1, low=0 |

**Label:** `next_month_revenue`

Kết quả lưu vào PostgreSQL:
- `seller_predictions`: actual_revenue vs predicted_revenue cho 2024
- `model_metrics`: RMSE, MAE, R², train_rmse, n_train, n_test, feature_json

---

### Step 4 — Benchmark (Python MapReduce vs Spark SQL)

**Input:** `/sales/processed_csv/` (MapReduce), `/sales/processed/` (Spark)
**Output:** PostgreSQL `benchmark_results`, HDFS `comparison.txt`

So sánh 2 engine trên 3 bài toán analytics:

| Task | MR Jobs | MR HDFS Writes | Spark Stages | Spark HDFS Writes |
|---|---|---|---|---|
| Revenue by Category | 1 | 1 | 1 | 0 |
| Monthly Trend | **2** | **2** | 1 | 0 |
| Top 10 Products | 1 | ~2 | 1 | 0 |

**Case Study — Monthly Trend:**

MapReduce cần **2 jobs** vì không có Window function:
- Job 1: GroupBy year_month → SUM revenue → ghi HDFS
- Job 2: Đọc output Job 1, sort, tính lag(prev_month) → ghi HDFS lần 2

Spark xử lý trong **1 DAG duy nhất** với `Window.orderBy("year_month").lag(1)` — không có intermediate HDFS write. Đây là lợi thế cốt lõi của Spark DAG so với MapReduce chaining.

**Kết quả thực đo:**

| Engine | Thời gian (3 tasks) | Ghi chú |
|---|---|---|
| Python MapReduce | ~1183s (~20 phút) | JVM startup + YARN scheduling + Python↔Java pipe |
| Spark SQL | ~11.8s | In-memory DAG execution |
| **Speedup** | **100.7×** | |

> **Disclaimer:** Môi trường VirtualBox 2-core. Absolute time phản ánh framework overhead, không phải production performance. Điểm so sánh có giá trị là số MR Jobs và HDFS Writes.

---

### Step 5 — Dashboard (Streamlit)

**Input:** PostgreSQL (primary), HDFS Parquet (fallback khi PG down)
**Port:** 8501

**Tab 1 — Sales Overview:**
- KPI cards: Total GMV, Total Orders, Unique Customers, Active Sellers, Avg Order Value
- Monthly Revenue Trend: stacked bar theo năm (2022/2023/2024) + MoM growth% line (dual-axis)
- Revenue by Category: horizontal bar chart
- Top 10 Products by Order Count: horizontal bar chart
- Seller Alerts: bảng sellers có revenue drop >20%

**Tab 2 — Revenue Forecast:**
- Model metrics: RMSE, MAE, R², n_test
- Actual vs Predicted Revenue: line chart per seller (chọn tối đa 5)
- Filter theo seller_type
- Residual Error% bar chart
- Feature Importance table
- Model History log

**Tab 3 — Benchmark:**
- Bar chart: MR vs Spark wall-clock time per task
- Architecture comparison: số Jobs, HDFS Writes
- Case Study: Monthly Trend deep dive
- Links YARN UI + Spark UI

Cache 5 phút (`@st.cache_data(ttl=300)`). Nút Refresh trên sidebar.

---

## Cài đặt & Chạy

### Prerequisites

```bash
# Services cần chạy trước
- Hadoop HDFS + YARN  (port 9000, 8088)
- Apache Spark        (port 7077, 8080)
- PostgreSQL 14       (port 5432)

# Python packages
pip install pyspark psycopg2-binary streamlit plotly pandas pyarrow
```

### 1. Khởi động services

```bash
bash start.sh
```

### 2. Tạo PostgreSQL database

```bash
psql -U postgres -c "CREATE DATABASE sales_db; CREATE USER hadoop WITH PASSWORD 'hadoop'; GRANT ALL PRIVILEGES ON DATABASE sales_db TO hadoop;"
psql -U hadoop -d sales_db -h 127.0.0.1 -f config/schema.sql
```

### 3. Chạy toàn bộ pipeline

```bash
bash run_pipeline.sh
```

Ước tính thời gian:
- Step 0: ~1 phút
- Step 1: ~3-5 phút
- Step 2: ~3-5 phút
- Step 3: ~2-3 phút
- Step 4: ~25 phút (MapReduce ~20 phút + Spark ~12 giây)
- Step 5: background (Streamlit)

### 4. Truy cập

```
Dashboard : http://192.168.56.10:8501
YARN UI   : http://192.168.56.10:8088
Spark UI  : http://192.168.56.10:8080
```

### Chạy riêng từng step

```bash
# Spark submit
spark-submit --master spark://192.168.56.10:7077 \
  --executor-memory 2g --executor-cores 2 \
  step1_clean/clean_data.py

# Benchmark chỉ Spark (bỏ qua MapReduce ~20 phút)
python3 step4_benchmark/benchmark.py --spark-only

# Dashboard
streamlit run step5_dashboard/dashboard.py --server.port 8501 --server.address 0.0.0.0
```

---

## Cấu hình

### `config/db.conf`

```ini
[postgresql]
host     = localhost
port     = 5432
dbname   = sales_db
user     = hadoop
password = hadoop
```

### Spark Submit config (trong `run_pipeline.sh`)

```bash
spark-submit \
  --master spark://192.168.56.10:7077 \
  --executor-memory 2g \
  --executor-cores 2 \
  --conf spark.executor.instances=2 \
  --conf spark.driver.memory=1g \
  --conf spark.sql.adaptive.enabled=true
```

---

## Môi trường

| | |
|---|---|
| OS | Ubuntu 20.04 (VirtualBox) |
| CPU | 2 cores |
| RAM | 8GB |
| Hadoop | 3.3.1 (single-node pseudo-distributed) |
| Spark | 3.4.1 (standalone mode) |
| Python | 3.8 |
| PostgreSQL | 14 |
| IP Master | 192.168.56.10 |
