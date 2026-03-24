"""
Healthcare Revenue Cycle Management (RCM) Analytics Dashboard
=============================================================

This is the main Streamlit application that provides an interactive,
multi-tab dashboard for monitoring healthcare revenue cycle KPIs.

Architecture:
    ┌──────────────────────────────────────────────────────────┐
    │                    Streamlit App (app.py)                 │
    │  ┌─────────┐  ┌──────────┐  ┌──────────┐  ┌─────────┐  │
    │  │  Tab 1   │  │  Tab 2   │  │  Tab 3   │  │ Tab 4-6 │  │
    │  │ Summary  │  │ Revenue  │  │ Claims   │  │  More   │  │
    │  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬────┘  │
    │       └──────────────┴─────────────┴─────────────┘       │
    │                         │                                 │
    │              ┌──────────▼──────────┐                     │
    │              │ Metrics Engine      │                     │
    │              │ (src/metrics.py)    │                     │
    │              └──────────┬──────────┘                     │
    │              ┌──────────▼──────────┐                     │
    │              │ Data Loader         │                     │
    │              │ (src/data_loader.py)│                     │
    │              └──────────┬──────────┘                     │
    │              ┌──────────▼──────────┐                     │
    │              │ SQLite Database     │                     │
    │              │ (data/*.db)         │                     │
    │              └─────────────────────┘                     │
    └──────────────────────────────────────────────────────────┘

Dashboard Tabs:
    1. Executive Summary  — 8 KPI scorecards + key trends + volume
    2. Collections & Revenue — Revenue waterfall, collection trends, cost analysis
    3. Claims & Denials   — Denial analysis, clean claims, charge lag, appeals
    4. A/R Aging & Cash   — Aging buckets, DAR trend, monthly cash flow
    5. Payer Analysis     — Revenue by payer, denial rates, payer comparison
    6. Department Perf.   — Revenue by department, encounter mix

How Streamlit Works (for educational purposes):
    - Streamlit reruns this entire script top-to-bottom on every user interaction
      (filter change, tab switch, etc.).
    - @st.cache_data prevents reloading data from the database on every rerun.
    - Sidebar widgets (selectbox, date_input) return the user's current selection.
    - st.tabs() creates a tabbed interface; content under each `with tab:` block
      only renders when that tab is active.

Running the Dashboard:
    pip install -r requirements.txt
    streamlit run app.py
"""

import io

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots
import streamlit_shadcn_ui as ui
from streamlit_extras.metric_cards import style_metric_cards

# ── Design System ─────────────────────────────────────────────────────
# Set global Plotly template so every chart starts with a clean white
# background and consistent grid styling.
pio.templates.default = "plotly_white"

# Brand color sequence — used as color_discrete_sequence on all charts.
# Order: clinical blue, emerald green, amber, red, indigo, sky, violet, teal.
RCM_COLORS = [
    "#1E6FBF",  # clinical blue   — primary
    "#10B981",  # emerald green   — positive / paid
    "#F59E0B",  # amber           — warning / pending
    "#EF4444",  # red             — bad / denied
    "#6366F1",  # indigo          — payer accent
    "#0EA5E9",  # sky blue        — secondary metric
    "#8B5CF6",  # violet          — department accent
    "#14B8A6",  # teal            — cash flow
]

# Import our custom modules
from src.data_loader import load_all_data       # Loads all tables from SQLite
from src.validators import validate_all         # Data integrity checks
from src.metadata_pages import (               # Four supplemental metadata pages
    render_data_lineage,
    render_data_catalog,
    render_knowledge_graph,
    render_semantic_layer,
)
from src.metrics import (                        # 17 SQL-based KPI query functions
    FilterParams,
    query_days_in_ar,
    query_net_collection_rate,
    query_gross_collection_rate,
    query_clean_claim_rate,
    query_denial_rate,
    query_denial_reasons,
    query_first_pass_rate,
    query_charge_lag,
    query_cost_to_collect,
    query_ar_aging,
    query_payment_accuracy,
    query_bad_debt_rate,
    query_appeal_success_rate,
    query_avg_reimbursement,
    query_payer_mix,
    query_denial_rate_by_payer,
    query_department_performance,
    query_provider_performance,
    query_cpt_analysis,
    query_underpayment_analysis,
    query_underpayment_trend,
)

# ── Page Config ──────────────────────────────────────────────────────
# set_page_config() MUST be the first Streamlit command in the script.
# It configures the browser tab title, favicon, and default layout.
# "wide" layout uses the full browser width instead of a centered column.
st.set_page_config(
    page_title="Healthcare RCM Analytics",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

if "active_page" not in st.session_state:
    st.session_state["active_page"] = "dashboard"

# ── Custom CSS ───────────────────────────────────────────────────────
# KPI cards use a clean white card with a colored left border — a well-known
# modern SaaS pattern (see Linear, Vercel, Retool dashboards). Status is
# communicated through the left-border color rather than background color,
# which is easier to read and works well in both light and dark contexts.
st.markdown("""
<style>
    /* ── KPI metric cards ── */
    .metric-card {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-left: 4px solid #1E6FBF;
        border-radius: 8px;
        padding: 18px 20px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.07), 0 1px 2px rgba(0,0,0,0.05);
        margin-bottom: 10px;
    }
    .metric-card h2 {
        margin: 0 0 4px 0;
        font-size: 2rem;
        font-weight: 700;
        color: #1A2332;
        letter-spacing: -0.02em;
    }
    .metric-card p { margin: 0; font-size: 0.875rem; color: #64748b; }
    /* Status variants — only the left border changes */
    .metric-good { border-left-color: #10B981; }
    .metric-warn { border-left-color: #F59E0B; }
    .metric-bad  { border-left-color: #EF4444; }
    .benchmark-text { font-size: 0.75rem; color: #94a3b8; margin-top: 6px; }
</style>
""", unsafe_allow_html=True)


def metric_card(label, value, benchmark="", status="neutral"):
    """
    Render a styled KPI card with color coding based on performance status.

    Args:
        label:     KPI name (e.g., "Days in A/R")
        value:     KPI value to display (e.g., "32.5")
        benchmark: Industry benchmark text (e.g., "Benchmark: < 35 days")
        status:    "good", "warn", "bad", or "neutral" — controls card color
    """
    css_class = {
        "good": "metric-card metric-good",
        "warn": "metric-card metric-warn",
        "bad": "metric-card metric-bad",
    }.get(status, "metric-card")
    bench_html = f'<p class="benchmark-text">{benchmark}</p>' if benchmark else ""
    st.markdown(
        f'<div class="{css_class}"><h2>{value}</h2><p>{label}</p>{bench_html}</div>',
        unsafe_allow_html=True,
    )


# ── Export Helpers ───────────────────────────────────────────────────
def df_to_csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def dfs_to_excel(sheets: dict[str, pd.DataFrame]) -> bytes:
    """Write multiple DataFrames to an in-memory Excel file, one sheet each."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
    return buf.getvalue()


def export_buttons(label: str, sheets: dict[str, pd.DataFrame]):
    """Render side-by-side CSV and Excel download buttons."""
    primary_df = next(iter(sheets.values()))
    col_csv, col_xlsx, _ = st.columns([1, 1, 4])
    with col_csv:
        st.download_button(
            label="Download CSV",
            data=df_to_csv(primary_df),
            file_name=f"{label}.csv",
            mime="text/csv",
        )
    with col_xlsx:
        st.download_button(
            label="Download Excel",
            data=dfs_to_excel(sheets),
            file_name=f"{label}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


# ── Load Data ────────────────────────────────────────────────────────
# @st.cache_data is a Streamlit decorator that caches the return value.
# On the first run, it calls load_all_data() (which queries SQLite).
# On subsequent reruns (user interactions), it returns the cached result
# instantly. This is essential for performance — without caching, every
# filter change would re-query the entire database.
#
# The cache is invalidated when:
#   - The function code changes
#   - The app is restarted
#   - You call st.cache_data.clear()
@st.cache_data
def get_data():
    """Load all RCM data from SQLite (cached after first call)."""
    return load_all_data()


try:
    data = get_data()
except FileNotFoundError as e:
    st.error(f"**Data files not found.** {e}")
    st.info("Run `python generate_sample_data.py` from the project root to create the required data files.")
    st.stop()
except ValueError as e:
    st.error(f"**Data validation error.** {e}")
    st.stop()
except Exception as e:
    st.error(f"**Unexpected error loading data:** {e}")
    st.stop()
claims = data["claims"]
payments = data["payments"]
denials = data["denials"]
adjustments = data["adjustments"]
encounters = data["encounters"]
charges = data["charges"]
payers = data["payers"]
operating_costs = data["operating_costs"]

# ── Data Validation ───────────────────────────────────────────────────
_validation_issues = validate_all()   # reads directly from Silver tables

# ── Sidebar Filters ─────────────────────────────────────────────────
# Sidebar filters allow users to slice data interactively. The filter
# cascade works as follows:
#   1. Date range -> filters claims and encounters by date_of_service
#   2. Payer -> filters claims to a specific insurance company
#   3. Department -> filters encounters (and thus claims) by dept
#   4. Encounter Type -> filters by visit type (outpatient, ED, etc.)
#
# All filters are applied BEFORE any metrics are calculated, so the
# KPIs always reflect the filtered subset of data.
st.sidebar.title("Filters")

# Date range filter — lets users focus on a specific time period
min_date = claims["date_of_service"].min().date()
max_date = claims["date_of_service"].max().date()
date_range = st.sidebar.date_input(
    "Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

# Payer filter
payer_options = ["All"] + sorted(payers["payer_name"].tolist())
selected_payer = st.sidebar.selectbox("Payer", payer_options)

# Department filter
dept_options = ["All"] + sorted(encounters["department"].unique().tolist())
selected_dept = st.sidebar.selectbox("Department", dept_options)

# Encounter type filter
enc_type_options = ["All"] + sorted(encounters["encounter_type"].unique().tolist())
selected_enc_type = st.sidebar.selectbox("Encounter Type", enc_type_options)

# ── Apply Filters ────────────────────────────────────────────────────
# The filtering strategy: start with claims (the central table) and filter
# outward to related tables. This ensures all metrics use consistent data.
# We use .copy() to avoid pandas SettingWithCopyWarning.
if len(date_range) == 2:
    start_dt, end_dt = pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1])
else:
    start_dt, end_dt = pd.Timestamp(min_date), pd.Timestamp(max_date)

# Filter claims by date
f_claims = claims[(claims["date_of_service"] >= start_dt) & (claims["date_of_service"] <= end_dt)].copy()

# Filter by payer
if selected_payer != "All":
    payer_id = payers[payers["payer_name"] == selected_payer]["payer_id"].values[0]
    f_claims = f_claims[f_claims["payer_id"] == payer_id]

# Filter encounters by department and type
f_encounters = encounters[
    (encounters["date_of_service"] >= start_dt) & (encounters["date_of_service"] <= end_dt)
].copy()
if selected_dept != "All":
    f_encounters = f_encounters[f_encounters["department"] == selected_dept]
    enc_ids = f_encounters["encounter_id"].unique()
    f_claims = f_claims[f_claims["encounter_id"].isin(enc_ids)]
if selected_enc_type != "All":
    f_encounters = f_encounters[f_encounters["encounter_type"] == selected_enc_type]
    enc_ids = f_encounters["encounter_id"].unique()
    f_claims = f_claims[f_claims["encounter_id"].isin(enc_ids)]

# Filter related tables by cascading from filtered claims.
# These filtered DataFrames are used for drill-down sections and direct
# DataFrame aggregations (waterfall, cash flow, claim status pie, etc.).
claim_ids = f_claims["claim_id"].unique()
f_payments = payments[payments["claim_id"].isin(claim_ids)].copy()
f_denials = denials[denials["claim_id"].isin(claim_ids)].copy()
f_adjustments = adjustments[adjustments["claim_id"].isin(claim_ids)].copy()
f_charges = charges[charges["encounter_id"].isin(f_encounters["encounter_id"].unique())].copy()

# ── Build FilterParams for SQL-based metric queries ──────────────────
# All 17 metric query_* functions accept a FilterParams object that
# encodes the same four sidebar dimensions as SQL WHERE clause parameters.
_payer_id = (
    payers[payers["payer_name"] == selected_payer]["payer_id"].values[0]
    if selected_payer != "All" else None
)
params = FilterParams(
    start_date=str(start_dt.date()),
    end_date=str(end_dt.date()),
    payer_id=_payer_id,
    department=selected_dept if selected_dept != "All" else None,
    encounter_type=selected_enc_type if selected_enc_type != "All" else None,
)

# ── Metadata navigation (sidebar) ────────────────────────────────────
# These buttons must render BEFORE the page router so they appear on
# every page, including metadata pages that call st.stop() early.
st.sidebar.divider()
st.sidebar.markdown("### Metadata")
if st.sidebar.button("Data Catalog", width="stretch"):
    st.session_state["active_page"] = "data_catalog"
if st.sidebar.button("Data Lineage", width="stretch"):
    st.session_state["active_page"] = "data_lineage"
if st.sidebar.button("Knowledge Graph", width="stretch"):
    st.session_state["active_page"] = "knowledge_graph"
if st.sidebar.button("Semantic Layer", width="stretch"):
    st.session_state["active_page"] = "semantic_layer"
if st.session_state["active_page"] != "dashboard":
    if st.sidebar.button("Back to Dashboard", type="primary", width="stretch"):
        st.session_state["active_page"] = "dashboard"

# ── Page router ──────────────────────────────────────────────────────
_active = st.session_state["active_page"]
if _active == "data_catalog":
    render_data_catalog()
    st.stop()
elif _active == "data_lineage":
    render_data_lineage()
    st.stop()
elif _active == "knowledge_graph":
    render_knowledge_graph()
    st.stop()
elif _active == "semantic_layer":
    render_semantic_layer()
    st.stop()

# ── Header ───────────────────────────────────────────────────────────
st.title("Healthcare RCM Analytics Dashboard")
st.caption(f"Analyzing {len(f_claims):,} claims | {len(f_encounters):,} encounters | Date range: {start_dt.strftime('%b %Y')} to {end_dt.strftime('%b %Y')}")

if f_claims.empty:
    st.warning("No claims match the selected filters. Adjust the sidebar filters to see data.")
    st.stop()

# Apply streamlit-extras card styling to all st.metric() widgets
style_metric_cards(background_color="#ffffff", border_left_color="#1E6FBF", border_color="#e2e8f0", box_shadow=True)

# ── Tabs ─────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9 = st.tabs([
    "Executive Summary",
    "Collections & Revenue",
    "Claims & Denials",
    "A/R Aging & Cash Flow",
    "Payer Analysis",
    "Department Performance",
    "Provider Performance",
    "CPT Code Analysis",
    "Underpayment Analysis",
])

# =====================================================================
# TAB 1: EXECUTIVE SUMMARY
# =====================================================================
# The Executive Summary provides a "single pane of glass" view of the
# 8 most critical RCM KPIs. Each KPI card is color-coded:
#   Green  = Meeting industry benchmark
#   Yellow = Approaching danger zone
#   Red    = Below benchmark, needs immediate attention
#
# Below the KPI cards, trend charts show how DAR and NCR are tracking
# over time, with benchmark lines for reference.
# =====================================================================
with tab1:
    st.header("Executive Summary")

    # Calculate all KPIs via parameterized SQL queries against the Silver layer
    dar_val, dar_trend = query_days_in_ar(params)
    ncr_val, ncr_trend = query_net_collection_rate(params)
    gcr_val, gcr_trend = query_gross_collection_rate(params)
    ccr_val, ccr_trend = query_clean_claim_rate(params)
    denial_val, denial_trend = query_denial_rate(params)
    fpr_val, fpr_trend = query_first_pass_rate(params)
    accuracy_val = query_payment_accuracy(params)
    bad_debt_val, bad_debt_amt, total_charges = query_bad_debt_rate(params)

    # Top-level KPI cards — shadcn ui.metric_card for polished design
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        dar_status = "✅" if dar_val < 35 else ("⚠️" if dar_val < 50 else "🔴")
        ui.metric_card(title="Days in A/R", content=f"{dar_val}", description=f"{dar_status} Benchmark: < 35 days", key="card_dar")
    with col2:
        ncr_status = "✅" if ncr_val > 95 else ("⚠️" if ncr_val > 90 else "🔴")
        ui.metric_card(title="Net Collection Rate", content=f"{ncr_val}%", description=f"{ncr_status} Benchmark: > 95%", key="card_ncr")
    with col3:
        ccr_status = "✅" if ccr_val > 90 else ("⚠️" if ccr_val > 80 else "🔴")
        ui.metric_card(title="Clean Claim Rate", content=f"{ccr_val}%", description=f"{ccr_status} Benchmark: > 90%", key="card_ccr")
    with col4:
        denial_status = "✅" if denial_val < 10 else ("⚠️" if denial_val < 15 else "🔴")
        ui.metric_card(title="Denial Rate", content=f"{denial_val}%", description=f"{denial_status} Benchmark: < 10%", key="card_denial")

    col5, col6, col7, col8 = st.columns(4)
    with col5:
        gcr_status = "✅" if gcr_val > 70 else ("⚠️" if gcr_val > 55 else "🔴")
        ui.metric_card(title="Gross Collection Rate", content=f"{gcr_val}%", description=f"{gcr_status} Benchmark: > 70%", key="card_gcr")
    with col6:
        fpr_status = "✅" if fpr_val > 85 else ("⚠️" if fpr_val > 75 else "🔴")
        ui.metric_card(title="First-Pass Rate", content=f"{fpr_val}%", description=f"{fpr_status} Benchmark: > 85%", key="card_fpr")
    with col7:
        acc_status = "✅" if accuracy_val > 95 else ("⚠️" if accuracy_val > 90 else "🔴")
        ui.metric_card(title="Payment Accuracy", content=f"{accuracy_val}%", description=f"{acc_status} Benchmark: > 95%", key="card_acc")
    with col8:
        bd_status = "✅" if bad_debt_val < 3 else ("⚠️" if bad_debt_val < 5 else "🔴")
        ui.metric_card(title="Bad Debt Rate", content=f"{bad_debt_val}%", description=f"{bd_status} Benchmark: < 3%", key="card_bd")

    st.divider()

    # Key metric trends
    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Days in A/R Trend")
        fig = px.line(dar_trend.reset_index(), x="year_month", y="days_in_ar",
                      labels={"year_month": "Month", "days_in_ar": "Days in A/R"})
        fig.add_hline(y=35, line_dash="dash", line_color="green", annotation_text="Benchmark: 35 days")
        fig.update_layout(height=350, margin=dict(t=30, b=30))
        st.plotly_chart(fig, theme="streamlit", width="stretch")

    with col_right:
        st.subheader("Net Collection Rate Trend")
        fig = px.line(ncr_trend.reset_index(), x="year_month", y="ncr",
                      labels={"year_month": "Month", "ncr": "NCR (%)"})
        fig.add_hline(y=95, line_dash="dash", line_color="green", annotation_text="Benchmark: 95%")
        fig.update_layout(height=350, margin=dict(t=30, b=30))
        st.plotly_chart(fig, theme="streamlit", width="stretch")

    # Volume summary
    st.subheader("Monthly Volume")
    enc_monthly = f_encounters.copy()
    enc_monthly["year_month"] = enc_monthly["date_of_service"].dt.to_period("M").astype(str)
    vol = enc_monthly.groupby("year_month").agg(
        encounters=("encounter_id", "count")
    ).reset_index()
    claims_monthly = f_claims.copy()
    claims_monthly["year_month"] = claims_monthly["date_of_service"].dt.to_period("M").astype(str)
    claims_vol = claims_monthly.groupby("year_month")["claim_id"].count().reset_index()
    claims_vol.columns = ["year_month", "claims"]
    vol = vol.merge(claims_vol, on="year_month", how="outer").fillna(0)

    fig = go.Figure()
    fig.add_trace(go.Bar(x=vol["year_month"], y=vol["encounters"], name="Encounters", marker_color=RCM_COLORS[0]))
    fig.add_trace(go.Bar(x=vol["year_month"], y=vol["claims"], name="Claims", marker_color=RCM_COLORS[5]))
    fig.update_layout(barmode="group", height=350, margin=dict(t=30, b=30),
                      xaxis_title="Month", yaxis_title="Count")
    st.plotly_chart(fig, theme="streamlit", width="stretch")


# =====================================================================
# TAB 2: COLLECTIONS & REVENUE
# =====================================================================
# This tab focuses on the financial health of the revenue cycle:
#   - Revenue Waterfall: Shows how charges flow to net revenue (charges
#     minus adjustments, denials, resulting in net collections)
#   - Collection rate trends: GCR and NCR over time
#   - Cost to Collect: How efficient is the billing operation?
#   - Average reimbursement: Revenue per claim over time
# =====================================================================
with tab2:
    st.header("Collections & Revenue Analysis")

    gcr_val, gcr_trend = query_gross_collection_rate(params)
    ncr_val, ncr_trend = query_net_collection_rate(params)
    ctc_val, ctc_trend = query_cost_to_collect(params)
    avg_reimb, reimb_trend = query_avg_reimbursement(params)
    bad_debt_val, bad_debt_amt, total_charges_val = query_bad_debt_rate(params)

    # KPIs
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Gross Collection Rate", f"{gcr_val}%")
    with col2:
        st.metric("Net Collection Rate", f"{ncr_val}%")
    with col3:
        st.metric("Cost to Collect", f"{ctc_val}%", help="Benchmark: 3-8%")
    with col4:
        st.metric("Avg Reimbursement / Claim", f"${avg_reimb:,.2f}")

    st.divider()

    # Revenue waterfall
    st.subheader("Revenue Waterfall")
    total_charges_w = f_claims["total_charge_amount"].sum()
    total_payments_w = f_payments["payment_amount"].sum()
    total_adj_w = f_adjustments["adjustment_amount"].sum()
    total_denials_w = f_denials["denied_amount"].sum()
    net_revenue = total_payments_w

    fig = go.Figure(go.Waterfall(
        name="Revenue Flow",
        orientation="v",
        measure=["absolute", "relative", "relative", "relative", "total"],
        x=["Total Charges", "Adjustments", "Denials (Lost)", "Collections", "Net Revenue"],
        y=[total_charges_w, -total_adj_w, -(total_denials_w - f_denials["recovered_amount"].sum()),
           total_payments_w - total_charges_w + total_adj_w + (total_denials_w - f_denials["recovered_amount"].sum()),
           0],
        connector={"line": {"color": "rgb(63, 63, 63)"}},
        text=[f"${total_charges_w:,.0f}", f"-${total_adj_w:,.0f}",
              f"-${(total_denials_w - f_denials['recovered_amount'].sum()):,.0f}",
              "", f"${net_revenue:,.0f}"],
        textposition="outside",
    ))
    fig.update_layout(height=400, margin=dict(t=30, b=30), showlegend=False)
    st.plotly_chart(fig, theme="streamlit", width="stretch")

    # Collection rate trends
    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Collection Rates Over Time")
        combined_trend = gcr_trend[["gcr"]].join(ncr_trend[["ncr"]], how="outer").fillna(0).reset_index()
        combined_trend.columns = ["Month", "Gross Collection Rate", "Net Collection Rate"]
        fig = px.line(combined_trend, x="Month", y=["Gross Collection Rate", "Net Collection Rate"])
        fig.update_layout(height=350, margin=dict(t=30, b=30), yaxis_title="%")
        st.plotly_chart(fig, theme="streamlit", width="stretch")

    with col_right:
        st.subheader("Cost to Collect Trend")
        fig = px.area(ctc_trend.reset_index(), x="year_month", y="cost_to_collect_pct",
                      labels={"year_month": "Month", "cost_to_collect_pct": "Cost to Collect (%)"})
        fig.add_hline(y=5, line_dash="dash", line_color="green", annotation_text="Target: 5%")
        fig.update_layout(height=350, margin=dict(t=30, b=30))
        st.plotly_chart(fig, theme="streamlit", width="stretch")

    # Avg reimbursement trend
    st.subheader("Average Reimbursement per Claim")
    fig = px.bar(reimb_trend.reset_index(), x="year_month", y="payment_amount",
                 labels={"year_month": "Month", "payment_amount": "Avg Reimbursement ($)"})
    fig.update_layout(height=300, margin=dict(t=30, b=30))
    st.plotly_chart(fig, theme="streamlit", width="stretch")

    # Financial summary
    st.subheader("Financial Summary")
    fin_data = {
        "Metric": ["Total Charges", "Total Payments", "Total Adjustments",
                    "Bad Debt Write-offs", "Net Revenue"],
        "Amount": [
            f"${total_charges_w:,.2f}",
            f"${total_payments_w:,.2f}",
            f"${total_adj_w:,.2f}",
            f"${bad_debt_amt:,.2f}",
            f"${net_revenue:,.2f}",
        ]
    }
    fin_df = pd.DataFrame(fin_data)
    st.dataframe(fin_df, hide_index=True, width="stretch")
    export_buttons("collections_revenue", {
        "Financial Summary": fin_df,
        "Filtered Claims": f_claims,
        "Filtered Payments": f_payments,
        "Filtered Adjustments": f_adjustments,
    })


# =====================================================================
# TAB 3: CLAIMS & DENIALS
# =====================================================================
# This tab helps identify and fix billing process problems:
#   - Clean Claim Rate: Are we submitting error-free claims?
#   - Denial Rate & Reasons: Why are claims being rejected?
#   - First-Pass Rate: Are claims being paid on first submission?
#   - Charge Lag: How quickly are services being billed?
#   - Appeal Success: Are we recovering revenue from denied claims?
#
# The denial reasons bar chart is the most actionable visualization —
# it shows exactly where to focus process improvement efforts.
# =====================================================================
with tab3:
    st.header("Claims & Denials Analysis")

    ccr_val, ccr_trend = query_clean_claim_rate(params)
    denial_val, denial_trend = query_denial_rate(params)
    fpr_val, fpr_trend = query_first_pass_rate(params)
    denial_reasons = query_denial_reasons(params)
    charge_lag_val, charge_lag_trend, charge_lag_dist = query_charge_lag(params)
    appeal_rate, total_appealed, won_appeals = query_appeal_success_rate(params)

    # KPIs
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Clean Claim Rate", f"{ccr_val}%", help="Benchmark: > 90%")
    with col2:
        st.metric("Denial Rate", f"{denial_val}%", help="Benchmark: < 10%")
    with col3:
        st.metric("First-Pass Rate", f"{fpr_val}%", help="Benchmark: > 85%")
    with col4:
        st.metric("Appeal Success Rate", f"{appeal_rate}%", help=f"{won_appeals} won of {total_appealed} appealed")

    st.divider()

    # Claims status distribution
    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Claim Status Distribution")
        status_counts = f_claims["claim_status"].value_counts().reset_index()
        status_counts.columns = ["Status", "Count"]
        fig = px.pie(status_counts, values="Count", names="Status",
                     color_discrete_sequence=RCM_COLORS)
        fig.update_layout(height=350, margin=dict(t=30, b=30))
        st.plotly_chart(fig, theme="streamlit", width="stretch")

    with col_right:
        st.subheader("Top Denial Reasons")
        fig = px.bar(denial_reasons.head(10), x="count", y="denial_reason_description",
                     orientation="h", color="total_denied_amount",
                     color_continuous_scale="Reds",
                     labels={"count": "Denial Count", "denial_reason_description": "Reason",
                             "total_denied_amount": "$ Denied"})
        fig.update_layout(height=350, margin=dict(t=30, b=30), yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, theme="streamlit", width="stretch")

    # Denial & Clean Claim trends
    col_left2, col_right2 = st.columns(2)
    with col_left2:
        st.subheader("Denial Rate Trend")
        fig = px.line(denial_trend.reset_index(), x="year_month", y="denial_rate",
                      labels={"year_month": "Month", "denial_rate": "Denial Rate (%)"})
        fig.add_hline(y=10, line_dash="dash", line_color="green", annotation_text="Target: 10%")
        fig.update_layout(height=300, margin=dict(t=30, b=30))
        st.plotly_chart(fig, theme="streamlit", width="stretch")

    with col_right2:
        st.subheader("Clean Claim Rate Trend")
        fig = px.line(ccr_trend.reset_index(), x="year_month", y="ccr",
                      labels={"year_month": "Month", "ccr": "Clean Claim Rate (%)"})
        fig.add_hline(y=90, line_dash="dash", line_color="green", annotation_text="Target: 90%")
        fig.update_layout(height=300, margin=dict(t=30, b=30))
        st.plotly_chart(fig, theme="streamlit", width="stretch")

    # Charge lag
    col_left3, col_right3 = st.columns(2)
    with col_left3:
        st.subheader(f"Charge Lag Distribution (Avg: {charge_lag_val} days)")
        lag_df = charge_lag_dist.reset_index()
        lag_df.columns = ["Days", "Count"]
        lag_df = lag_df[lag_df["Days"] <= 30]
        fig = px.bar(lag_df, x="Days", y="Count",
                     labels={"Days": "Lag (Days)", "Count": "# of Charges"})
        fig.update_layout(height=300, margin=dict(t=30, b=30))
        st.plotly_chart(fig, theme="streamlit", width="stretch")

    with col_right3:
        st.subheader("First-Pass Rate Trend")
        fig = px.line(fpr_trend.reset_index(), x="year_month", y="fpr",
                      labels={"year_month": "Month", "fpr": "First-Pass Rate (%)"})
        fig.add_hline(y=85, line_dash="dash", line_color="green", annotation_text="Target: 85%")
        fig.update_layout(height=300, margin=dict(t=30, b=30))
        st.plotly_chart(fig, theme="streamlit", width="stretch")

    # Denial details table
    with st.expander("Denial Reasons Detail Table"):
        denial_detail_df = denial_reasons[["denial_reason_code", "denial_reason_description", "count",
                        "total_denied_amount", "total_recovered", "recovery_rate"]].round(2)
        st.dataframe(denial_detail_df, hide_index=True, width="stretch")

    export_buttons("claims_denials", {
        "Denial Reasons": denial_detail_df,
        "Filtered Claims": f_claims,
        "Filtered Denials": f_denials,
    })


# =====================================================================
# TAB 4: A/R AGING & CASH FLOW
# =====================================================================
# This tab monitors cash flow and the age of unpaid balances:
#   - A/R Aging Buckets: How old are unpaid balances? (0-30, 31-60,
#     61-90, 91-120, 120+ days). Older = harder to collect.
#   - Days in A/R Trend: Dual-axis chart showing A/R balance (bars)
#     and DAR metric (line) over time.
#   - Monthly Cash Flow: Charges vs. payments per month, with net
#     cash flow line showing whether the practice is cash-positive.
# =====================================================================
with tab4:
    st.header("Accounts Receivable Aging & Cash Flow")

    dar_val, dar_trend = query_days_in_ar(params)
    aging_summary, total_ar = query_ar_aging(params)

    col1, col2, col3 = st.columns(3)
    with col1:
        status = "good" if dar_val < 35 else ("warn" if dar_val < 50 else "bad")
        metric_card("Days in A/R", f"{dar_val}", "Benchmark: < 35 days", status)
    with col2:
        metric_card("Total Outstanding A/R", f"${total_ar:,.0f}", "", "neutral")
    with col3:
        pct_0_60 = aging_summary.loc[["0-30", "31-60"], "pct_of_total"].sum() if len(aging_summary) > 0 else 0
        status = "good" if pct_0_60 > 70 else ("warn" if pct_0_60 > 50 else "bad")
        metric_card("A/R in 0-60 Days", f"{pct_0_60:.1f}%", "Benchmark: > 70%", status)

    st.divider()

    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("A/R Aging Buckets")
        aging_df = aging_summary.reset_index()
        aging_df.columns = ["Bucket", "Claim Count", "Total A/R", "% of Total"]
        fig = px.bar(aging_df, x="Bucket", y="Total A/R",
                     text="% of Total",
                     color="Bucket",
                     color_discrete_sequence=["#10B981","#F59E0B","#F97316","#EF4444","#991B1B"])
        fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig.update_layout(height=400, margin=dict(t=30, b=30), showlegend=False)
        st.plotly_chart(fig, theme="streamlit", width="stretch")

    with col_right:
        st.subheader("A/R Aging Distribution")
        fig = px.pie(aging_df, values="Total A/R", names="Bucket",
                     color_discrete_sequence=["#10B981","#F59E0B","#F97316","#EF4444","#991B1B"])
        fig.update_layout(height=400, margin=dict(t=30, b=30))
        st.plotly_chart(fig, theme="streamlit", width="stretch")

    # DAR trend
    st.subheader("Days in A/R Trend")
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(x=dar_trend.reset_index()["year_month"], y=dar_trend["ar_balance"],
               name="A/R Balance", marker_color=RCM_COLORS[0], opacity=0.6),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(x=dar_trend.reset_index()["year_month"], y=dar_trend["days_in_ar"],
                   name="Days in A/R", line=dict(color=RCM_COLORS[3], width=3)),
        secondary_y=True,
    )
    fig.add_hline(y=35, line_dash="dash", line_color="green", secondary_y=True,
                  annotation_text="Target: 35 days")
    fig.update_layout(height=400, margin=dict(t=30, b=30))
    fig.update_yaxes(title_text="A/R Balance ($)", secondary_y=False)
    fig.update_yaxes(title_text="Days in A/R", secondary_y=True)
    st.plotly_chart(fig, theme="streamlit", width="stretch")

    # Cash flow
    st.subheader("Monthly Cash Flow")
    claims_cf = f_claims.copy()
    claims_cf["year_month"] = claims_cf["date_of_service"].dt.to_period("M").astype(str)
    pay_cf = f_payments.merge(claims_cf[["claim_id", "year_month"]], on="claim_id", how="left")

    cf = pd.DataFrame({
        "charges": claims_cf.groupby("year_month")["total_charge_amount"].sum(),
        "payments": pay_cf.groupby("year_month")["payment_amount"].sum()
    }).fillna(0).reset_index()
    cf.columns = ["Month", "Charges", "Payments"]
    cf["Net Cash Flow"] = cf["Payments"] - cf["Charges"]

    fig = go.Figure()
    fig.add_trace(go.Bar(x=cf["Month"], y=cf["Charges"], name="Charges", marker_color=RCM_COLORS[0]))
    fig.add_trace(go.Bar(x=cf["Month"], y=cf["Payments"], name="Payments", marker_color=RCM_COLORS[1]))
    fig.add_trace(go.Scatter(x=cf["Month"], y=cf["Net Cash Flow"], name="Net Cash Flow",
                             line=dict(color=RCM_COLORS[3], width=2, dash="dot")))
    fig.update_layout(barmode="group", height=400, margin=dict(t=30, b=30),
                      yaxis_title="Amount ($)")
    st.plotly_chart(fig, theme="streamlit", width="stretch")

    # A/R aging table
    with st.expander("A/R Aging Detail"):
        aging_detail = aging_df.copy()
        aging_detail["Total A/R"] = aging_detail["Total A/R"].apply(lambda x: f"${x:,.2f}")
        aging_detail["% of Total"] = aging_detail["% of Total"].apply(lambda x: f"{x:.1f}%")
        st.dataframe(aging_detail, hide_index=True, width="stretch")

    export_buttons("ar_aging_cashflow", {
        "AR Aging Summary": aging_df,
        "Cash Flow": cf,
    })


# =====================================================================
# TAB 5: PAYER ANALYSIS
# =====================================================================
# This tab compares performance across insurance companies:
#   - Revenue by Payer: Which payers generate the most revenue?
#   - Payer Mix: Volume distribution across payers (pie chart)
#   - Collection Rate by Payer: Which payers pay best/worst?
#   - Denial Rate by Payer: Which payers deny most frequently?
#   - Payer Comparison Table: Side-by-side metrics for all payers
#
# This data is critical for payer contract negotiations and for
# prioritizing follow-up on underpaying or high-denial payers.
# =====================================================================
with tab5:
    st.header("Payer Performance Analysis")

    payer_mix = query_payer_mix(params)
    denial_by_payer = query_denial_rate_by_payer(params)

    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Revenue by Payer")
        fig = px.bar(payer_mix, x="payer_name", y="total_payments",
                     color="payer_type",
                     color_discrete_sequence=RCM_COLORS,
                     labels={"payer_name": "Payer", "total_payments": "Total Payments ($)",
                             "payer_type": "Type"})
        fig.update_layout(height=400, margin=dict(t=30, b=30), xaxis_tickangle=-45)
        st.plotly_chart(fig, theme="streamlit", width="stretch")

    with col_right:
        st.subheader("Payer Mix (by Volume)")
        fig = px.pie(payer_mix, values="claim_count", names="payer_name",
                     color_discrete_sequence=RCM_COLORS)
        fig.update_layout(height=400, margin=dict(t=30, b=30))
        st.plotly_chart(fig, theme="streamlit", width="stretch")

    # Collection rate by payer
    col_left2, col_right2 = st.columns(2)
    with col_left2:
        st.subheader("Collection Rate by Payer")
        fig = px.bar(payer_mix.sort_values("collection_rate"),
                     x="collection_rate", y="payer_name", orientation="h",
                     color="collection_rate",
                     color_continuous_scale="RdYlGn",
                     labels={"collection_rate": "Collection Rate (%)", "payer_name": "Payer"})
        fig.update_layout(height=400, margin=dict(t=30, b=30))
        st.plotly_chart(fig, theme="streamlit", width="stretch")

    with col_right2:
        st.subheader("Denial Rate by Payer")
        fig = px.bar(denial_by_payer.sort_values("denial_rate"),
                     x="denial_rate", y="payer_name", orientation="h",
                     color="denial_rate",
                     color_continuous_scale="RdYlGn_r",
                     labels={"denial_rate": "Denial Rate (%)", "payer_name": "Payer"})
        fig.update_layout(height=400, margin=dict(t=30, b=30))
        st.plotly_chart(fig, theme="streamlit", width="stretch")

    # Payer comparison table
    st.subheader("Payer Comparison Table")
    payer_table = payer_mix.merge(
        denial_by_payer[["payer_id", "denial_rate"]], on="payer_id", how="left"
    )[["payer_name", "payer_type", "claim_count", "total_charges", "total_payments",
       "collection_rate", "denial_rate"]].round(2)
    payer_table.columns = ["Payer", "Type", "Claims", "Total Charges", "Total Payments",
                           "Collection Rate (%)", "Denial Rate (%)"]
    st.dataframe(payer_table, hide_index=True, width="stretch")

    export_buttons("payer_analysis", {
        "Payer Comparison": payer_table,
        "Payer Mix": payer_mix,
        "Denial by Payer": denial_by_payer,
    })

    # Payer type summary
    st.subheader("Performance by Payer Type")
    type_summary = payer_mix.groupby("payer_type").agg(
        claims=("claim_count", "sum"),
        charges=("total_charges", "sum"),
        payments=("total_payments", "sum"),
    ).reset_index()
    type_summary["collection_rate"] = (type_summary["payments"] / type_summary["charges"] * 100).round(2)
    fig = px.bar(type_summary, x="payer_type", y=["charges", "payments"],
                 barmode="group",
                 color_discrete_sequence=RCM_COLORS,
                 labels={"value": "Amount ($)", "payer_type": "Payer Type", "variable": "Metric"})
    fig.update_layout(height=350, margin=dict(t=30, b=30))
    st.plotly_chart(fig, theme="streamlit", width="stretch")

    # ── Payer Drill-Down ──────────────────────────────────────────────
    st.divider()
    st.subheader("Payer Drill-Down")
    payer_names = sorted(payer_mix["payer_name"].tolist())
    selected_drilldown_payer = st.selectbox("Select a payer to inspect", payer_names, key="payer_drilldown")
    if selected_drilldown_payer:
        drill_payer_id = payers[payers["payer_name"] == selected_drilldown_payer]["payer_id"].values[0]
        drill_claims = f_claims[f_claims["payer_id"] == drill_payer_id].copy()
        drill_payments = f_payments[f_payments["claim_id"].isin(drill_claims["claim_id"])].copy()
        drill_denials = f_denials[f_denials["claim_id"].isin(drill_claims["claim_id"])].copy()

        kc1, kc2, kc3, kc4 = st.columns(4)
        with kc1:
            st.metric("Claims", f"{len(drill_claims):,}")
        with kc2:
            st.metric("Total Charges", f"${drill_claims['total_charge_amount'].sum():,.0f}")
        with kc3:
            st.metric("Total Payments", f"${drill_payments['payment_amount'].sum():,.0f}")
        with kc4:
            denied_count = drill_claims["claim_status"].isin(["Denied", "Appealed"]).sum()
            st.metric("Denied Claims", f"{denied_count:,}")

        col_d1, col_d2 = st.columns(2)
        with col_d1:
            status_counts = drill_claims["claim_status"].value_counts().reset_index()
            status_counts.columns = ["Status", "Count"]
            fig = px.pie(status_counts, values="Count", names="Status",
                         title="Claim Status Mix",
                         color_discrete_sequence=RCM_COLORS)
            fig.update_layout(height=300, margin=dict(t=40, b=10))
            st.plotly_chart(fig, theme="streamlit", width="stretch")
        with col_d2:
            if not drill_denials.empty:
                denial_reasons_drill = drill_denials["denial_reason_description"].value_counts().reset_index()
                denial_reasons_drill.columns = ["Reason", "Count"]
                fig = px.bar(denial_reasons_drill, x="Count", y="Reason", orientation="h",
                             title="Denial Reasons",
                             labels={"Count": "# Denials", "Reason": ""})
                fig.update_layout(height=300, margin=dict(t=40, b=10),
                                  yaxis={"categoryorder": "total ascending"})
                st.plotly_chart(fig, theme="streamlit", width="stretch")
            else:
                st.info("No denials for this payer in the selected date range.")

        with st.expander("Claim-Level Detail"):
            claim_detail = drill_claims[["claim_id", "date_of_service", "submission_date",
                                          "total_charge_amount", "claim_status", "is_clean_claim"]].copy()
            pay_totals = drill_payments.groupby("claim_id")["payment_amount"].sum().reset_index()
            claim_detail = claim_detail.merge(pay_totals, on="claim_id", how="left")
            claim_detail["payment_amount"] = claim_detail["payment_amount"].fillna(0)
            claim_detail.columns = ["Claim ID", "Date of Service", "Submission Date",
                                     "Charge Amount", "Status", "Clean Claim", "Payment Amount"]
            st.dataframe(claim_detail, hide_index=True, width="stretch")
            export_buttons(f"payer_drilldown_{selected_drilldown_payer.replace(' ', '_')}", {
                "Claims": claim_detail,
                "Denials": drill_denials,
            })


# =====================================================================
# TAB 6: DEPARTMENT PERFORMANCE
# =====================================================================
# This tab breaks down revenue cycle performance by clinical department:
#   - Revenue by Department: Charges vs. payments for each dept
#   - Collection Rate: Which departments collect most effectively?
#   - Encounter Volume: How busy is each department?
#   - Avg Payment per Encounter: Revenue intensity by department
#   - Encounter Type Mix: Distribution of visit types per department
#
# This helps administrators identify departments that may need
# additional coding support, billing staff, or process improvement.
# =====================================================================
with tab6:
    st.header("Department Performance")

    dept_perf = query_department_performance(params)

    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Revenue by Department")
        fig = px.bar(dept_perf, x="department", y=["total_charges", "total_payments"],
                     barmode="group",
                     color_discrete_sequence=RCM_COLORS,
                     labels={"value": "Amount ($)", "department": "Department", "variable": "Metric"})
        fig.update_layout(height=400, margin=dict(t=30, b=30), xaxis_tickangle=-45)
        st.plotly_chart(fig, theme="streamlit", width="stretch")

    with col_right:
        st.subheader("Collection Rate by Department")
        fig = px.bar(dept_perf.sort_values("collection_rate"),
                     x="collection_rate", y="department", orientation="h",
                     color="collection_rate",
                     color_continuous_scale="RdYlGn",
                     labels={"collection_rate": "Collection Rate (%)", "department": "Department"})
        fig.update_layout(height=400, margin=dict(t=30, b=30))
        st.plotly_chart(fig, theme="streamlit", width="stretch")

    # Encounter volume by department
    col_left2, col_right2 = st.columns(2)
    with col_left2:
        st.subheader("Encounter Volume by Department")
        fig = px.pie(dept_perf, values="encounter_count", names="department",
                     color_discrete_sequence=RCM_COLORS)
        fig.update_layout(height=400, margin=dict(t=30, b=30))
        st.plotly_chart(fig, theme="streamlit", width="stretch")

    with col_right2:
        st.subheader("Avg Payment per Encounter")
        fig = px.bar(dept_perf.sort_values("avg_payment_per_encounter"),
                     x="avg_payment_per_encounter", y="department", orientation="h",
                     color="avg_payment_per_encounter",
                     color_continuous_scale="Blues",
                     labels={"avg_payment_per_encounter": "Avg $/Encounter", "department": "Department"})
        fig.update_layout(height=400, margin=dict(t=30, b=30))
        st.plotly_chart(fig, theme="streamlit", width="stretch")

    # Department encounter type breakdown
    st.subheader("Encounter Type by Department")
    dept_enc = f_encounters.groupby(["department", "encounter_type"]).size().reset_index(name="count")
    fig = px.bar(dept_enc, x="department", y="count", color="encounter_type",
                 color_discrete_sequence=RCM_COLORS,
                 labels={"count": "Count", "department": "Department", "encounter_type": "Type"})
    fig.update_layout(height=400, margin=dict(t=30, b=30), xaxis_tickangle=-45)
    st.plotly_chart(fig, theme="streamlit", width="stretch")

    # Department table
    st.subheader("Department Performance Summary")
    dept_table = dept_perf.copy()
    dept_table["total_charges"] = dept_table["total_charges"].apply(lambda x: f"${x:,.2f}")
    dept_table["total_payments"] = dept_table["total_payments"].apply(lambda x: f"${x:,.2f}")
    dept_table["collection_rate"] = dept_table["collection_rate"].apply(lambda x: f"{x:.1f}%")
    dept_table["avg_payment_per_encounter"] = dept_table["avg_payment_per_encounter"].apply(lambda x: f"${x:,.2f}")
    dept_table.columns = ["Department", "Encounters", "Total Charges", "Total Payments",
                          "Collection Rate", "Avg $/Encounter"]
    st.dataframe(dept_table, hide_index=True, width="stretch")
    export_buttons("department_performance", {
        "Department Summary": dept_perf,
        "Encounter Type Mix": dept_enc,
    })

    # ── Department Drill-Down ─────────────────────────────────────────
    st.divider()
    st.subheader("Department Drill-Down")
    dept_names = sorted(dept_perf["department"].tolist())
    selected_drilldown_dept = st.selectbox("Select a department to inspect", dept_names, key="dept_drilldown")
    if selected_drilldown_dept:
        drill_encs = f_encounters[f_encounters["department"] == selected_drilldown_dept].copy()
        drill_enc_ids = drill_encs["encounter_id"].unique()
        drill_dept_claims = f_claims[f_claims["encounter_id"].isin(drill_enc_ids)].copy()
        drill_dept_payments = f_payments[f_payments["claim_id"].isin(drill_dept_claims["claim_id"])].copy()
        drill_dept_denials = f_denials[f_denials["claim_id"].isin(drill_dept_claims["claim_id"])].copy()

        kd1, kd2, kd3, kd4 = st.columns(4)
        with kd1:
            st.metric("Encounters", f"{len(drill_encs):,}")
        with kd2:
            st.metric("Claims", f"{len(drill_dept_claims):,}")
        with kd3:
            st.metric("Total Charges", f"${drill_dept_claims['total_charge_amount'].sum():,.0f}")
        with kd4:
            st.metric("Total Payments", f"${drill_dept_payments['payment_amount'].sum():,.0f}")

        col_dd1, col_dd2 = st.columns(2)
        with col_dd1:
            enc_type_counts = drill_encs["encounter_type"].value_counts().reset_index()
            enc_type_counts.columns = ["Type", "Count"]
            fig = px.pie(enc_type_counts, values="Count", names="Type",
                         title="Encounter Type Mix",
                         color_discrete_sequence=RCM_COLORS)
            fig.update_layout(height=300, margin=dict(t=40, b=10))
            st.plotly_chart(fig, theme="streamlit", width="stretch")
        with col_dd2:
            status_counts_dept = drill_dept_claims["claim_status"].value_counts().reset_index()
            status_counts_dept.columns = ["Status", "Count"]
            fig = px.bar(status_counts_dept, x="Status", y="Count",
                         title="Claim Status",
                         color="Status",
                         color_discrete_sequence=RCM_COLORS)
            fig.update_layout(height=300, margin=dict(t=40, b=10), showlegend=False)
            st.plotly_chart(fig, theme="streamlit", width="stretch")

        with st.expander("Encounter & Claim Detail"):
            enc_detail = drill_encs[["encounter_id", "date_of_service", "encounter_type",
                                      "patient_id", "provider_id"]].copy()
            enc_detail = enc_detail.merge(
                drill_dept_claims[["encounter_id", "claim_id", "total_charge_amount", "claim_status"]],
                on="encounter_id", how="left"
            )
            pay_totals_dept = drill_dept_payments.groupby("claim_id")["payment_amount"].sum().reset_index()
            enc_detail = enc_detail.merge(pay_totals_dept, on="claim_id", how="left")
            enc_detail["payment_amount"] = enc_detail["payment_amount"].fillna(0)
            enc_detail.columns = ["Encounter ID", "Date of Service", "Encounter Type",
                                   "Patient ID", "Provider ID", "Claim ID",
                                   "Charge Amount", "Claim Status", "Payment Amount"]
            st.dataframe(enc_detail, hide_index=True, width="stretch")
            export_buttons(f"dept_drilldown_{selected_drilldown_dept.replace(' ', '_')}", {
                "Encounters & Claims": enc_detail,
                "Denials": drill_dept_denials,
            })


# =====================================================================
# TAB 7: PROVIDER PERFORMANCE
# =====================================================================
# Breaks down RCM metrics by individual rendering provider — a key view
# for practice managers and CFOs who need to identify which providers
# have high denial rates, low clean claim rates, or lagging charge lag.
#
# Benchmark thresholds mirror industry standards and flag outliers with
# color-coded status indicators so action items are immediately visible.
# =====================================================================
with tab7:
    st.header("Provider Performance")

    provider_perf = query_provider_performance(params)

    if provider_perf.empty:
        st.info("No provider data available for the selected filters.")
    else:
        # ── Summary KPIs ──────────────────────────────────────────────
        avg_prov_collection = provider_perf["collection_rate"].mean()
        avg_prov_denial = provider_perf["denial_rate"].mean()
        avg_prov_ccr = provider_perf["clean_claim_rate"].mean()
        outlier_count = int((provider_perf["denial_rate"] > 15).sum())

        kp1, kp2, kp3, kp4 = st.columns(4)
        with kp1:
            cr_status = "✅" if avg_prov_collection > 95 else ("⚠️" if avg_prov_collection > 85 else "🔴")
            ui.metric_card(title="Avg Collection Rate", content=f"{avg_prov_collection:.1f}%",
                           description=f"{cr_status} Benchmark: > 95%", key="prov_cr")
        with kp2:
            dr_status = "✅" if avg_prov_denial < 10 else ("⚠️" if avg_prov_denial < 15 else "🔴")
            ui.metric_card(title="Avg Denial Rate", content=f"{avg_prov_denial:.1f}%",
                           description=f"{dr_status} Benchmark: < 10%", key="prov_dr")
        with kp3:
            ccr_status = "✅" if avg_prov_ccr > 90 else ("⚠️" if avg_prov_ccr > 80 else "🔴")
            ui.metric_card(title="Avg Clean Claim Rate", content=f"{avg_prov_ccr:.1f}%",
                           description=f"{ccr_status} Benchmark: > 90%", key="prov_ccr")
        with kp4:
            out_status = "✅" if outlier_count == 0 else ("⚠️" if outlier_count <= 3 else "🔴")
            ui.metric_card(title="High-Denial Providers", content=f"{outlier_count}",
                           description=f"{out_status} Denial rate > 15%", key="prov_outliers")

        st.divider()

        # ── Charts ────────────────────────────────────────────────────
        col_p1, col_p2 = st.columns(2)
        with col_p1:
            st.subheader("Revenue by Provider")
            fig = px.bar(
                provider_perf.head(15).sort_values("total_payments"),
                x="total_payments", y="provider_name", orientation="h",
                color="collection_rate",
                color_continuous_scale="RdYlGn",
                labels={"total_payments": "Total Payments ($)", "provider_name": "",
                        "collection_rate": "Collection Rate (%)"},
                title="Top 15 Providers by Collections",
            )
            fig.update_layout(height=450, margin=dict(t=40, b=10))
            st.plotly_chart(fig, theme="streamlit", width="stretch")

        with col_p2:
            st.subheader("Denial Rate by Provider")
            fig = px.bar(
                provider_perf.sort_values("denial_rate", ascending=False).head(15),
                x="denial_rate", y="provider_name", orientation="h",
                color="denial_rate",
                color_continuous_scale="RdYlGn_r",
                labels={"denial_rate": "Denial Rate (%)", "provider_name": ""},
                title="Top 15 Providers by Denial Rate",
            )
            fig.add_vline(x=10, line_dash="dash", line_color="#F59E0B",
                          annotation_text="10% benchmark", annotation_position="top right")
            fig.update_layout(height=450, margin=dict(t=40, b=10))
            st.plotly_chart(fig, theme="streamlit", width="stretch")

        col_p3, col_p4 = st.columns(2)
        with col_p3:
            st.subheader("Clean Claim Rate by Provider")
            fig = px.bar(
                provider_perf.sort_values("clean_claim_rate").head(15),
                x="clean_claim_rate", y="provider_name", orientation="h",
                color="clean_claim_rate",
                color_continuous_scale="RdYlGn",
                labels={"clean_claim_rate": "Clean Claim Rate (%)", "provider_name": ""},
            )
            fig.add_vline(x=90, line_dash="dash", line_color="#F59E0B",
                          annotation_text="90% benchmark", annotation_position="top right")
            fig.update_layout(height=450, margin=dict(t=40, b=10))
            st.plotly_chart(fig, theme="streamlit", width="stretch")

        with col_p4:
            st.subheader("Avg Payment per Encounter")
            fig = px.bar(
                provider_perf.sort_values("avg_payment_per_encounter", ascending=False).head(15),
                x="avg_payment_per_encounter", y="provider_name", orientation="h",
                color="avg_payment_per_encounter",
                color_continuous_scale="Blues",
                labels={"avg_payment_per_encounter": "Avg Payment / Encounter ($)", "provider_name": ""},
            )
            fig.update_layout(height=450, margin=dict(t=40, b=10))
            st.plotly_chart(fig, theme="streamlit", width="stretch")

        # ── Provider Scorecard Table ──────────────────────────────────
        st.subheader("Provider Scorecard")
        scorecard = provider_perf[[
            "provider_name", "specialty", "department", "encounter_count", "claim_count",
            "total_charges", "total_payments", "collection_rate", "denial_rate", "clean_claim_rate",
        ]].copy().round(2)
        scorecard.columns = [
            "Provider", "Specialty", "Department", "Encounters", "Claims",
            "Total Charges ($)", "Total Payments ($)", "Collection Rate (%)",
            "Denial Rate (%)", "Clean Claim Rate (%)",
        ]
        st.dataframe(scorecard, hide_index=True, width="stretch")
        export_buttons("provider_performance", {"Provider Scorecard": scorecard})

        # ── Provider Drill-Down ───────────────────────────────────────
        st.divider()
        st.subheader("Provider Drill-Down")
        provider_names = sorted(provider_perf["provider_name"].tolist())
        selected_provider = st.selectbox("Select a provider to inspect", provider_names, key="provider_drilldown")
        if selected_provider:
            prov_row = provider_perf[provider_perf["provider_name"] == selected_provider].iloc[0]
            prov_enc_ids = encounters[encounters["provider_id"] == prov_row["provider_id"]]["encounter_id"].unique()
            drill_prov_claims = f_claims[f_claims["encounter_id"].isin(prov_enc_ids)].copy()
            drill_prov_payments = f_payments[f_payments["claim_id"].isin(drill_prov_claims["claim_id"])].copy()
            drill_prov_denials = f_denials[f_denials["claim_id"].isin(drill_prov_claims["claim_id"])].copy()

            pd1, pd2, pd3, pd4 = st.columns(4)
            with pd1:
                st.metric("Encounters", f"{int(prov_row['encounter_count']):,}")
            with pd2:
                st.metric("Claims", f"{int(prov_row['claim_count']):,}")
            with pd3:
                cr_delta = round(prov_row["collection_rate"] - avg_prov_collection, 1)
                st.metric("Collection Rate", f"{prov_row['collection_rate']:.1f}%",
                          delta=f"{cr_delta:+.1f}% vs avg")
            with pd4:
                dr_delta = round(prov_row["denial_rate"] - avg_prov_denial, 1)
                st.metric("Denial Rate", f"{prov_row['denial_rate']:.1f}%",
                          delta=f"{dr_delta:+.1f}% vs avg", delta_color="inverse")

            col_pd1, col_pd2 = st.columns(2)
            with col_pd1:
                status_counts = drill_prov_claims["claim_status"].value_counts().reset_index()
                status_counts.columns = ["Status", "Count"]
                fig = px.pie(status_counts, values="Count", names="Status",
                             title="Claim Status Mix", color_discrete_sequence=RCM_COLORS)
                fig.update_layout(height=300, margin=dict(t=40, b=10))
                st.plotly_chart(fig, theme="streamlit", width="stretch")
            with col_pd2:
                if not drill_prov_denials.empty:
                    denial_reasons_prov = drill_prov_denials["denial_reason_description"].value_counts().reset_index()
                    denial_reasons_prov.columns = ["Reason", "Count"]
                    fig = px.bar(denial_reasons_prov, x="Count", y="Reason", orientation="h",
                                 title="Top Denial Reasons",
                                 labels={"Count": "# Denials", "Reason": ""})
                    fig.update_layout(height=300, margin=dict(t=40, b=10),
                                      yaxis={"categoryorder": "total ascending"})
                    st.plotly_chart(fig, theme="streamlit", width="stretch")
                else:
                    st.info("No denials for this provider in the selected date range.")


# =====================================================================
# TAB 8: CPT CODE ANALYSIS
# =====================================================================
# Surfaces revenue and denial patterns at the procedure-code level.
# Billing managers use this view to identify high-denial CPT codes,
# charge master pricing outliers, and high-volume procedures that drive
# the most revenue — critical for contract negotiations and coding audits.
# =====================================================================
with tab8:
    st.header("CPT Code Analysis")

    cpt_data = query_cpt_analysis(params)

    if cpt_data.empty:
        st.info("No CPT code data available for the selected filters.")
    else:
        # ── Summary KPIs ──────────────────────────────────────────────
        total_cpt_charges = cpt_data["total_charges"].sum()
        total_cpt_codes = len(cpt_data)
        high_denial_cpts = int((cpt_data["denial_rate"] > 15).sum())
        top_cpt_pct = cpt_data["total_charges"].head(10).sum() / total_cpt_charges * 100 if total_cpt_charges > 0 else 0

        kc1, kc2, kc3, kc4 = st.columns(4)
        with kc1:
            ui.metric_card(title="Distinct CPT Codes", content=f"{total_cpt_codes:,}",
                           description="Active procedure codes billed", key="cpt_count")
        with kc2:
            ui.metric_card(title="Total Charges", content=f"${total_cpt_charges:,.0f}",
                           description="Across all CPT codes", key="cpt_charges")
        with kc3:
            hd_status = "✅" if high_denial_cpts == 0 else ("⚠️" if high_denial_cpts <= 5 else "🔴")
            ui.metric_card(title="High-Denial CPT Codes", content=f"{high_denial_cpts}",
                           description=f"{hd_status} Denial rate > 15%", key="cpt_highdeny")
        with kc4:
            ui.metric_card(title="Top-10 CPT Concentration", content=f"{top_cpt_pct:.1f}%",
                           description="Revenue share from top 10 codes", key="cpt_concentration")

        st.divider()

        # ── Charts ────────────────────────────────────────────────────
        col_c1, col_c2 = st.columns(2)
        with col_c1:
            st.subheader("Top CPT Codes by Revenue")
            top_cpt = cpt_data.head(15).copy()
            top_cpt["label"] = top_cpt["cpt_code"] + " — " + top_cpt["cpt_description"].str[:30]
            fig = px.bar(
                top_cpt.sort_values("total_charges"),
                x="total_charges", y="label", orientation="h",
                color="denial_rate",
                color_continuous_scale="RdYlGn_r",
                labels={"total_charges": "Total Charges ($)", "label": "",
                        "denial_rate": "Denial Rate (%)"},
                title="Top 15 CPT Codes (color = denial rate)",
            )
            fig.update_layout(height=450, margin=dict(t=40, b=10))
            st.plotly_chart(fig, theme="streamlit", width="stretch")

        with col_c2:
            st.subheader("Top CPT Codes by Denial Rate")
            high_vol = cpt_data[cpt_data["charge_count"] >= 10].copy()
            high_vol["label"] = high_vol["cpt_code"] + " — " + high_vol["cpt_description"].str[:25]
            fig = px.bar(
                high_vol.sort_values("denial_rate", ascending=False).head(15),
                x="denial_rate", y="label", orientation="h",
                color="total_charges",
                color_continuous_scale="Blues",
                labels={"denial_rate": "Denial Rate (%)", "label": "",
                        "total_charges": "Total Charges ($)"},
                title="Highest Denial Rate (min 10 charges, color = revenue)",
            )
            fig.add_vline(x=10, line_dash="dash", line_color="#F59E0B",
                          annotation_text="10% benchmark")
            fig.update_layout(height=450, margin=dict(t=40, b=10))
            st.plotly_chart(fig, theme="streamlit", width="stretch")

        col_c3, col_c4 = st.columns(2)
        with col_c3:
            st.subheader("Charge Volume by CPT Code")
            fig = px.pie(
                cpt_data.head(12),
                values="total_charges", names="cpt_code",
                title="Revenue Share — Top 12 CPT Codes",
                color_discrete_sequence=RCM_COLORS,
            )
            fig.update_layout(height=400, margin=dict(t=40, b=10))
            st.plotly_chart(fig, theme="streamlit", width="stretch")

        with col_c4:
            st.subheader("Avg Charge per Unit by CPT Code")
            fig = px.bar(
                cpt_data.sort_values("avg_charge_per_unit", ascending=False).head(15),
                x="avg_charge_per_unit", y="cpt_code", orientation="h",
                color="avg_charge_per_unit",
                color_continuous_scale="Blues",
                labels={"avg_charge_per_unit": "Avg Charge per Unit ($)", "cpt_code": "CPT Code"},
                title="Top 15 CPT Codes by Avg Charge per Unit",
            )
            fig.update_layout(height=400, margin=dict(t=40, b=10))
            st.plotly_chart(fig, theme="streamlit", width="stretch")

        # ── CPT Detail Table ──────────────────────────────────────────
        st.subheader("CPT Code Detail")
        cpt_table = cpt_data[[
            "cpt_code", "cpt_description", "charge_count", "total_units",
            "total_charges", "avg_charge_per_unit", "claim_count", "denied_claims", "denial_rate",
        ]].copy().round(2)
        cpt_table.columns = [
            "CPT Code", "Description", "Charge Count", "Total Units",
            "Total Charges ($)", "Avg Charge/Unit ($)", "Claims", "Denied Claims", "Denial Rate (%)",
        ]
        st.dataframe(cpt_table, hide_index=True, width="stretch")
        export_buttons("cpt_code_analysis", {"CPT Code Analysis": cpt_table})


# =====================================================================
# TAB 9: UNDERPAYMENT ANALYSIS
# =====================================================================
# Compares what payers actually remitted (payment_amount) against what
# they were contractually obligated to pay (allowed_amount from the ERA).
# The gap is a recovery opportunity — identifying systematic underpayments
# by payer is one of the highest-ROI activities in enterprise RCM.
# =====================================================================
with tab9:
    st.header("Underpayment Analysis")
    st.caption("Compares ERA allowed amounts vs. actual payments to surface contractual underpayments.")

    underpay_df, total_recovery = query_underpayment_analysis(params)
    underpay_trend = query_underpayment_trend(params)

    if underpay_df.empty:
        st.info("No underpayment data available for the selected filters.")
    else:
        total_allowed = underpay_df["total_allowed"].sum()
        total_paid = underpay_df["total_paid"].sum()
        overall_underpay_rate = (total_recovery / total_allowed * 100) if total_allowed > 0 else 0
        total_underpaid_claims = int(underpay_df["underpaid_count"].sum())

        # ── Summary KPIs ──────────────────────────────────────────────
        ku1, ku2, ku3, ku4 = st.columns(4)
        with ku1:
            ui.metric_card(title="Recovery Opportunity", content=f"${total_recovery:,.0f}",
                           description="Total contractual underpayments", key="underpay_opp")
        with ku2:
            ur_status = "✅" if overall_underpay_rate < 1 else ("⚠️" if overall_underpay_rate < 3 else "🔴")
            ui.metric_card(title="Underpayment Rate", content=f"{overall_underpay_rate:.2f}%",
                           description=f"{ur_status} Allowed vs. paid variance", key="underpay_rate")
        with ku3:
            ui.metric_card(title="Underpaid Claims", content=f"{total_underpaid_claims:,}",
                           description="Claims paid below contracted rate", key="underpay_claims")
        with ku4:
            avg_underpay = total_recovery / total_underpaid_claims if total_underpaid_claims > 0 else 0
            ui.metric_card(title="Avg Underpayment / Claim", content=f"${avg_underpay:,.2f}",
                           description="Average shortfall per underpaid claim", key="underpay_avg")

        st.divider()

        # ── Charts ────────────────────────────────────────────────────
        col_u1, col_u2 = st.columns(2)
        with col_u1:
            st.subheader("Recovery Opportunity by Payer")
            fig = px.bar(
                underpay_df.sort_values("total_underpaid", ascending=False),
                x="total_underpaid", y="payer_name", orientation="h",
                color="underpayment_rate",
                color_continuous_scale="RdYlGn_r",
                labels={"total_underpaid": "Underpayment Amount ($)", "payer_name": "",
                        "underpayment_rate": "Underpayment Rate (%)"},
                title="Total Underpayments by Payer (color = rate)",
            )
            fig.update_layout(height=400, margin=dict(t=40, b=10))
            st.plotly_chart(fig, theme="streamlit", width="stretch")

        with col_u2:
            st.subheader("Underpayment Rate by Payer")
            fig = px.bar(
                underpay_df.sort_values("underpayment_rate", ascending=False),
                x="underpayment_rate", y="payer_name", orientation="h",
                color="total_underpaid",
                color_continuous_scale="Reds",
                labels={"underpayment_rate": "Underpayment Rate (%)", "payer_name": "",
                        "total_underpaid": "$ Underpaid"},
                title="Underpayment Rate by Payer (color = dollar amount)",
            )
            fig.update_layout(height=400, margin=dict(t=40, b=10))
            st.plotly_chart(fig, theme="streamlit", width="stretch")

        # ── Monthly Trend ─────────────────────────────────────────────
        if not underpay_trend.empty:
            st.subheader("Monthly Underpayment Trend")
            col_u3, col_u4 = st.columns(2)
            with col_u3:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=underpay_trend.index, y=underpay_trend["total_underpaid"],
                    name="Underpayment Amount", marker_color=RCM_COLORS[3],
                ))
                fig.update_layout(
                    height=320, margin=dict(t=40, b=10),
                    yaxis_title="Amount ($)", xaxis_title="Month",
                    title="Monthly Underpayment Dollars",
                )
                st.plotly_chart(fig, theme="streamlit", width="stretch")
            with col_u4:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=underpay_trend.index, y=underpay_trend["underpayment_rate"],
                    mode="lines+markers", name="Underpayment Rate",
                    line=dict(color=RCM_COLORS[3], width=2),
                ))
                fig.add_hline(y=1, line_dash="dash", line_color=RCM_COLORS[2],
                              annotation_text="1% target", annotation_position="bottom right")
                fig.update_layout(
                    height=320, margin=dict(t=40, b=10),
                    yaxis_title="Rate (%)", xaxis_title="Month",
                    title="Monthly Underpayment Rate",
                )
                st.plotly_chart(fig, theme="streamlit", width="stretch")

        # ── Allowed vs. Paid Waterfall ────────────────────────────────
        st.subheader("Allowed vs. Paid vs. Underpaid — by Payer")
        waterfall_df = underpay_df.sort_values("total_allowed", ascending=False)
        fig = go.Figure()
        fig.add_trace(go.Bar(name="Paid", x=waterfall_df["payer_name"],
                             y=waterfall_df["total_paid"], marker_color=RCM_COLORS[1]))
        fig.add_trace(go.Bar(name="Underpaid (Recovery Opportunity)", x=waterfall_df["payer_name"],
                             y=waterfall_df["total_underpaid"], marker_color=RCM_COLORS[3]))
        fig.update_layout(
            barmode="stack", height=380, margin=dict(t=40, b=60),
            yaxis_title="Amount ($)", xaxis_title="Payer",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            xaxis_tickangle=-30,
        )
        st.plotly_chart(fig, theme="streamlit", width="stretch")

        # ── Underpayment Detail Table ─────────────────────────────────
        st.subheader("Underpayment Summary by Payer")
        underpay_table = underpay_df[[
            "payer_name", "payer_type", "payment_count", "total_allowed",
            "total_paid", "total_underpaid", "underpaid_count", "underpayment_rate",
        ]].copy().round(2)
        underpay_table.columns = [
            "Payer", "Payer Type", "Payment Count", "Total Allowed ($)",
            "Total Paid ($)", "Underpaid Amount ($)", "Underpaid Claims", "Underpayment Rate (%)",
        ]
        st.dataframe(underpay_table, hide_index=True, width="stretch")
        export_buttons("underpayment_analysis", {"Underpayment by Payer": underpay_table})


# ── Sidebar Footer ───────────────────────────────────────────────────
# Show a summary of the filtered data volume in the sidebar so users
# always know how much data they're looking at.
st.sidebar.divider()
st.sidebar.markdown("### Data Summary")
st.sidebar.markdown(f"- **Patients:** {len(data['patients']):,}")
st.sidebar.markdown(f"- **Providers:** {len(data['providers']):,}")
st.sidebar.markdown(f"- **Encounters:** {len(f_encounters):,}")
st.sidebar.markdown(f"- **Claims:** {len(f_claims):,}")
st.sidebar.markdown(f"- **Payments:** {len(f_payments):,}")
st.sidebar.markdown(f"- **Denials:** {len(f_denials):,}")

if _validation_issues:
    st.sidebar.divider()
    errors = [i for i in _validation_issues if i["level"] == "error"]
    warnings = [i for i in _validation_issues if i["level"] == "warning"]
    with st.sidebar.expander(
        f"{'🔴' if errors else '🟡'} Data Quality ({len(_validation_issues)} issue{'s' if len(_validation_issues) != 1 else ''})",
        expanded=bool(errors),
    ):
        for issue in _validation_issues:
            icon = "🔴" if issue["level"] == "error" else "🟡"
            st.markdown(f"{icon} **{issue['table']}**: {issue['message']}")

st.sidebar.divider()
st.sidebar.caption("Healthcare RCM Analytics Dashboard v1.0")
