#!/usr/bin/env python3
"""STEP 5 — dashboard.py (Streamlit + PostgreSQL / Parquet fallback)
Usage: streamlit run step5_dashboard/dashboard.py --server.port 8501 --server.address 0.0.0.0
"""
import configparser, os, shutil, subprocess, tempfile, warnings
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

warnings.filterwarnings("ignore")

HDFS           = "hdfs://192.168.56.10:9000"
HDFS_ANALYTICS = f"{HDFS}/sales/analytics"
CONFIG_PATH    = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../config/db.conf")
BLUE, RED, GREEN, ORANGE = "#0088FE", "#FF4444", "#00C49F", "#FFBB28"

st.set_page_config(page_title="Sales Analytics", page_icon="📊",
                   layout="wide", initial_sidebar_state="expanded")


# ── DB config ─────────────────────────────────────────────────────────────────
def load_pg_config():
    cfg = configparser.ConfigParser()
    cfg.read(CONFIG_PATH)
    s = cfg["postgresql"] if "postgresql" in cfg else {}
    return dict(host=s.get("host","localhost"), port=int(s.get("port",5432)),
                dbname=s.get("dbname","sales_db"),
                user=s.get("user","hadoop"), password=s.get("password","hadoop"))


@st.cache_resource
def get_pg():
    try:
        import psycopg2
        cfg = load_pg_config()
        conn = psycopg2.connect(**cfg)
        conn.cursor().execute("SELECT 1")
        return conn, "postgresql"
    except Exception as e:
        return None, f"unavailable: {e}"


# ── Data loaders ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner=False)
def qpg(sql):
    conn, mode = get_pg()
    if conn is None: return pd.DataFrame()
    try: return pd.read_sql(sql, conn)
    except Exception: return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def read_parquet(hdfs_path):
    tmp = tempfile.mkdtemp(prefix="dash_")
    try:
        r = subprocess.run(["hdfs","dfs","-get", hdfs_path, tmp],
                           capture_output=True, text=True, timeout=60)
        if r.returncode != 0: return pd.DataFrame()
        return pd.read_parquet(os.path.join(tmp, os.path.basename(hdfs_path)), engine="pyarrow")
    except Exception: return pd.DataFrame()
    finally: shutil.rmtree(tmp, ignore_errors=True)


# ── Sidebar ───────────────────────────────────────────────────────────────────
_, conn_mode = get_pg()
with st.sidebar:
    if conn_mode == "postgresql":
        st.success("🟢 PostgreSQL")
    else:
        st.warning(f"🟡 Parquet fallback\n\n_{conn_mode}_")
    st.caption("Dữ liệu cache 5 phút")
    if st.button("🔄 Refresh"):
        st.cache_data.clear(); st.cache_resource.clear(); st.rerun()

using_pg = conn_mode == "postgresql"

if using_pg:
    rev_df    = qpg("SELECT * FROM revenue_by_category ORDER BY total_revenue DESC")
    mon_df    = qpg("SELECT * FROM monthly_trend ORDER BY year_month")
    top_df    = qpg("SELECT * FROM top_products ORDER BY rank")
    kpi_df    = qpg("SELECT * FROM kpi_summary")
    alert_df  = qpg("SELECT * FROM seller_alerts ORDER BY year_month DESC, drop_pct DESC")
    pred_df   = qpg("SELECT * FROM seller_predictions ORDER BY seller_id, year_month")
    metrics_df= qpg("SELECT * FROM model_metrics ORDER BY run_at DESC")
    bm_df     = qpg("SELECT * FROM benchmark_results ORDER BY created_at DESC, task")
else:
    rev_df    = read_parquet(f"{HDFS_ANALYTICS}/revenue_by_category")
    mon_df    = read_parquet(f"{HDFS_ANALYTICS}/monthly_trend")
    top_df    = read_parquet(f"{HDFS_ANALYTICS}/top_products")
    kpi_df    = read_parquet(f"{HDFS_ANALYTICS}/kpi_summary")
    alert_df  = read_parquet(f"{HDFS_ANALYTICS}/seller_alerts")
    pred_df   = read_parquet(f"{HDFS_ANALYTICS}/seller_monthly")  # use seller_monthly as proxy
    metrics_df= pd.DataFrame()
    bm_df     = pd.DataFrame()


# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style='background:linear-gradient(135deg,#1a1a2e,#16213e,#0f3460);
            padding:24px;border-radius:12px;margin-bottom:16px;'>
  <h1 style='color:#e94560;margin:0;font-size:2rem;'>📊 Sales Analytics Dashboard</h1>
  <p style='color:#a8b2d8;margin:4px 0 0;'>
    200k đơn hàng · Hadoop 3.3.1 · Spark 3.4.1 · Python MapReduce · PostgreSQL · Streamlit
  </p>
</div>""", unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs(["📊 Sales Overview", "🤖 Revenue Forecast", "⚡ Benchmark"])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Sales Overview
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    # KPI cards
    if not kpi_df.empty and "metric_name" in kpi_df.columns:
        kpi = dict(zip(kpi_df["metric_name"], kpi_df["metric_value"]))
    elif not rev_df.empty:
        kpi = {
            "total_gmv":        rev_df["total_revenue"].sum(),
            "total_orders":     rev_df["order_count"].sum(),
            "active_sellers":   0,
            "unique_customers": 0,
            "avg_order_value":  rev_df["avg_revenue"].mean() if "avg_revenue" in rev_df.columns else 0,
        }
    else:
        kpi = {}

    if kpi:
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("💰 Total GMV",         f"${float(kpi.get('total_gmv',0)):,.0f}")
        c2.metric("📦 Total Orders",      f"{int(kpi.get('total_orders',0)):,}")
        c3.metric("👥 Unique Customers",  f"{int(kpi.get('unique_customers',0)):,}")
        c4.metric("🏪 Active Sellers",    f"{int(kpi.get('active_sellers',0)):,}")
        c5.metric("💵 Avg Order Value",   f"${float(kpi.get('avg_order_value',0)):,.0f}")

    st.divider()

    # Monthly trend — stacked bar by year + MoM line
    if not mon_df.empty:
        mon_df["year"] = mon_df["year_month"].str[:4]
        fig_mon = make_subplots(specs=[[{"secondary_y": True}]])
        colors = {"2022": "#4e79a7", "2023": "#f28e2b", "2024": "#e94560"}
        for yr, grp in mon_df.groupby("year"):
            fig_mon.add_trace(go.Bar(x=grp["year_month"], y=grp["total_revenue"],
                                     name=str(yr), marker_color=colors.get(str(yr), BLUE),
                                     opacity=0.85), secondary_y=False)
        if "mom_growth_pct" in mon_df.columns:
            fig_mon.add_trace(go.Scatter(x=mon_df["year_month"], y=mon_df["mom_growth_pct"],
                                         name="MoM %", line=dict(color=ORANGE, width=2),
                                         mode="lines+markers"), secondary_y=True)
        fig_mon.update_layout(title="Monthly Revenue Trend (stacked by year) + MoM Growth %",
                               barmode="stack", height=420)
        fig_mon.update_yaxes(title_text="Revenue ($)", secondary_y=False)
        fig_mon.update_yaxes(title_text="MoM Growth %", secondary_y=True)
        st.plotly_chart(fig_mon, use_container_width=True)

    col_l, col_r = st.columns(2)
    with col_l:
        if not rev_df.empty:
            fig_rev = px.bar(rev_df.sort_values("total_revenue"),
                             x="total_revenue", y="category", orientation="h",
                             color="total_revenue", color_continuous_scale="Viridis",
                             title="Revenue by Category",
                             labels={"total_revenue":"Revenue ($)","category":""}, height=400)
            fig_rev.update_layout(coloraxis_showscale=False)
            st.plotly_chart(fig_rev, use_container_width=True)

    with col_r:
        if not top_df.empty:
            name_col = "product_name" if "product_name" in top_df.columns else "product_id"
            fig_top = px.bar(top_df.sort_values("order_count"),
                             x="order_count", y=name_col, orientation="h",
                             color="order_count", color_continuous_scale="Blues",
                             title="Top 10 Products by Order Count",
                             labels={"order_count":"Orders", name_col:""}, height=400)
            fig_top.update_layout(coloraxis_showscale=False)
            st.plotly_chart(fig_top, use_container_width=True)

    # Seller alerts
    if not alert_df.empty:
        st.divider()
        st.markdown(f"### ⚠️ Seller Alerts — {len(alert_df)} cảnh báo revenue drop")
        disp = alert_df.head(10).copy()
        if "drop_pct" in disp.columns:
            disp["drop_pct"] = disp["drop_pct"].apply(lambda x: f"▼ {x:.1f}%")
        st.dataframe(disp[["seller_id","year_month","prev_revenue","curr_revenue","drop_pct"]
                           if all(c in disp for c in ["prev_revenue","curr_revenue"]) 
                           else disp.columns[:5]],
                     use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Revenue Forecast
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.markdown("**Dự đoán doanh thu tháng tới của từng seller dựa trên Linear Regression với 12 features.**")

    if not metrics_df.empty:
        latest = metrics_df.iloc[0]
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("📉 RMSE",    f"{float(latest.get('rmse',0)):,.0f}")
        m2.metric("📉 MAE",     f"{float(latest.get('mae',0)):,.0f}")
        m3.metric("📈 R²",      f"{float(latest.get('r2',0)):.4f}")
        m4.metric("🧪 n_test",  f"{int(latest.get('n_test',0)):,}")
        st.divider()

    if not pred_df.empty and "actual_revenue" in pred_df.columns:
        # Filters
        f1, f2 = st.columns([1, 3])
        seller_types = ["All"] + sorted(pred_df["seller_type"].unique().tolist()) \
            if "seller_type" in pred_df.columns else ["All"]
        with f1:
            sel_type = st.selectbox("Seller type", seller_types)
        filtered = pred_df if sel_type == "All" else pred_df[pred_df["seller_type"] == sel_type]
        seller_list = sorted(filtered["seller_id"].unique().tolist())
        with f2:
            sel_sellers = st.multiselect("Seller IDs (max 5)", seller_list,
                                         default=seller_list[:3], max_selections=5)
        if sel_sellers:
            df_plot = filtered[filtered["seller_id"].isin(sel_sellers)]
            fig_pred = go.Figure()
            palette = [BLUE, RED, GREEN, ORANGE, "#9B59B6"]
            for i, sid in enumerate(sel_sellers):
                s = df_plot[df_plot["seller_id"] == sid].sort_values("year_month")
                c = palette[i % len(palette)]
                fig_pred.add_trace(go.Scatter(x=s["year_month"], y=s["actual_revenue"],
                                              name=f"{sid} actual", line=dict(color=c, width=2)))
                fig_pred.add_trace(go.Scatter(x=s["year_month"], y=s["predicted_revenue"],
                                              name=f"{sid} predicted",
                                              line=dict(color=c, width=2, dash="dash")))
            fig_pred.update_layout(title="Actual vs Predicted Revenue", height=420)
            st.plotly_chart(fig_pred, use_container_width=True)

            # Residual
            df_plot = df_plot.copy()
            df_plot["residual_pct"] = (df_plot["predicted_revenue"] - df_plot["actual_revenue"]) \
                                      / df_plot["actual_revenue"] * 100
            fig_res = px.bar(df_plot, x="year_month", y="residual_pct", color="seller_id",
                             barmode="group", title="Residual Error %",
                             labels={"residual_pct":"Error %","year_month":""}, height=300)
            st.plotly_chart(fig_res, use_container_width=True)

    # Feature importance table
    st.divider()
    st.markdown("#### Feature Importance (Linear Regression)")
    fi_data = [
        ("monthly_revenue",  "↑ Tăng mạnh", "Doanh thu hiện tại → tín hiệu trực tiếp nhất"),
        ("lag1_revenue",     "↑ Tăng",       "Tháng trước là predictor mạnh nhất"),
        ("lag2_revenue",     "↑ Nhỏ",        "Ảnh hưởng giảm dần theo thời gian"),
        ("lag3_revenue",     "↑ Nhỏ",        "Xu hướng 3 tháng"),
        ("revenue_ma3",      "↑ Tăng",       "Trung bình động 3 tháng — giảm nhiễu"),
        ("growth_rate",      "↑ Tăng",       "Tốc độ tăng trưởng gần nhất"),
        ("avg_order_value",  "↑ Nhỏ",        "Giá trị đơn hàng trung bình"),
        ("cancel_rate",      "↓ Giảm",       "Tỉ lệ huỷ cao → tín hiệu tiêu cực"),
        ("active_days",      "↑ Tăng",       "Số ngày hoạt động → độ consistent"),
        ("month",            "~  Tuỳ",       "Mùa vụ: Q4 (Nov/Dec) cao hơn"),
        ("seller_type_enc",  "↑ Tăng",       "high=2, avg=1, low=0 → ảnh hưởng lớn"),
        ("unique_customers", "↑ Nhỏ",        "Độ đa dạng khách hàng"),
    ]
    st.dataframe(pd.DataFrame(fi_data, columns=["Feature","Hướng","Giải thích"]),
                 use_container_width=True, hide_index=True)

    if not metrics_df.empty:
        st.divider()
        st.markdown("#### Model History")
        show_cols = [c for c in ["run_at","rmse","mae","r2","train_rmse","n_train","n_test"]
                     if c in metrics_df.columns]
        st.dataframe(metrics_df[show_cols], use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — Benchmark
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.subheader("⚡ Python MapReduce vs Spark SQL")

    if not bm_df.empty and "avg_seconds" in bm_df.columns:
        fig_bm = px.bar(bm_df, x="task", y="avg_seconds", color="engine", barmode="group",
                        color_discrete_map={"spark": BLUE, "mapreduce": RED},
                        title="Wall-clock time per task (avg of 3 runs)",
                        labels={"avg_seconds":"Seconds","task":"","engine":"Engine"},
                        text_auto=".1f", height=400)
        st.plotly_chart(fig_bm, use_container_width=True)
    else:
        st.info("Chạy step4_benchmark/benchmark.py để có dữ liệu.")

    st.divider()

    # Architecture table
    st.markdown("#### Kiến trúc — số Jobs và HDFS Writes")
    arch = pd.DataFrame([
        ("Revenue by Category", 1, 1, 1, 0, "GroupBy straightforward"),
        ("Monthly Trend",       2, 2, 1, 0, "MR cần Job2 cho MoM lag; Spark: 1 DAG Window ✨"),
        ("Top 10 Products",     1, "~2", 1, 0, "MR: 1 reducer bottleneck; Spark: partial agg"),
    ], columns=["Task","MR Jobs","MR HDFS Writes","Spark Stages","Spark HDFS Writes","Key Insight"])
    st.dataframe(arch, use_container_width=True, hide_index=True)

    st.info("""**Case Study — Monthly Trend:**
MapReduce cần **2 jobs** để tính MoM growth:
- Job 1: GroupBy year_month → sum revenue (ghi HDFS)
- Job 2: Đọc output Job 1, sort, tính lag(prev_month) → ghi HDFS

Spark làm trong **1 DAG duy nhất** với `Window.orderBy("year_month").lag(1)` — không có intermediate HDFS write.
Đây là lợi thế cốt lõi của Spark DAG so với MR chaining.""")

    st.warning("""⚠️ **Disclaimer:** Data giả lập trên VirtualBox 2-core.
Số giây đo được phản ánh **framework overhead** (JVM startup, YARN scheduling, Python↔Java pipe).
Điểm so sánh **có giá trị** là số MR Jobs và HDFS Writes — không phải absolute time.""")

    st.caption("YARN UI: http://192.168.56.10:8088  ·  Spark UI: http://192.168.56.10:8080")

st.markdown("""<hr style='border-color:#2a2a4a;'>
<p style='text-align:center;color:#555;font-size:0.8rem;'>
  Sales Lambda · Hadoop 3.3.1 · Spark 3.4.1 · Python MapReduce · PostgreSQL · Streamlit
</p>""", unsafe_allow_html=True)
