"""
Metadata Pages for Healthcare RCM Analytics Dashboard
======================================================

This module contains four supplemental pages accessible from the sidebar:
  - Data Catalog      : Searchable table of all 17 KPIs + 10 data tables
  - Data Lineage      : DAG showing the full pipeline from CSV to dashboard
  - Knowledge Graph   : Entity-relationship diagram of the 10 data entities
  - Semantic Layer    : Business concept → KPI → raw column mapping

Each render_*() function is called from app.py based on st.session_state["active_page"].
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

# ── Shared graph helper ───────────────────────────────────────────────


def _draw_network_graph(nodes, edges, title, height=600):
    """
    Render a network/DAG diagram using Plotly scatter traces.

    Args:
        nodes: list of dicts with keys: id, label, x, y, color, size, hover
        edges: list of dicts with keys: source, target, label (optional)
        title: chart title string
        height: pixel height of the figure
    """
    node_map = {n["id"]: n for n in nodes}

    fig = go.Figure()

    # Draw edges first (so they appear behind nodes)
    for edge in edges:
        src = node_map[edge["source"]]
        tgt = node_map[edge["target"]]
        fig.add_trace(go.Scatter(
            x=[src["x"], tgt["x"], None],
            y=[src["y"], tgt["y"], None],
            mode="lines",
            line=dict(width=1.5, color="#aaaaaa"),
            hoverinfo="none",
            showlegend=False,
        ))
        # Edge label midpoint
        if edge.get("label"):
            mx = (src["x"] + tgt["x"]) / 2
            my = (src["y"] + tgt["y"]) / 2
            fig.add_annotation(
                x=mx, y=my,
                text=edge["label"],
                showarrow=False,
                font=dict(size=9, color="#666666"),
                bgcolor="rgba(255,255,255,0.7)",
            )

    # Draw nodes grouped by color for legend
    color_groups = {}
    for n in nodes:
        color_groups.setdefault(n.get("group", ""), []).append(n)

    for group, group_nodes in color_groups.items():
        fig.add_trace(go.Scatter(
            x=[n["x"] for n in group_nodes],
            y=[n["y"] for n in group_nodes],
            mode="markers+text",
            marker=dict(
                size=[n.get("size", 30) for n in group_nodes],
                color=[n["color"] for n in group_nodes],
                line=dict(width=1, color="white"),
            ),
            text=[n["label"] for n in group_nodes],
            textposition="bottom center",
            textfont=dict(size=10),
            hovertext=[n.get("hover", n["label"]) for n in group_nodes],
            hoverinfo="text",
            name=group if group else "Nodes",
            showlegend=bool(group),
        ))

    fig.update_layout(
        title=title,
        height=height,
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin=dict(l=20, r=20, t=50, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    st.plotly_chart(fig, use_container_width=True)


# ── KPI catalog data ──────────────────────────────────────────────────

_KPI_CATALOG = [
    {
        "Metric": "Days in A/R (DAR)",
        "Category": "Financial Performance",
        "Definition": "How many days of charges are sitting unpaid. The single most important cash-flow metric in RCM.",
        "Formula": "A/R Balance / Avg Daily Charges  (A/R = Cumulative Charges − Cumulative Payments; Avg Daily = Monthly Charges / 30)",
        "Data Sources": "claims.total_charge_amount, payments.payment_amount",
        "Dashboard Tab": "Executive Summary, A/R Aging & Cash Flow",
    },
    {
        "Metric": "Net Collection Rate (NCR)",
        "Category": "Financial Performance",
        "Definition": "Percentage of collectible revenue actually collected. Adjustments remove contractually non-collectible amounts.",
        "Formula": "Payments / (Charges − Adjustments) × 100",
        "Data Sources": "payments.payment_amount, claims.total_charge_amount, adjustments.adjustment_amount",
        "Dashboard Tab": "Executive Summary, Collections & Revenue",
    },
    {
        "Metric": "Gross Collection Rate (GCR)",
        "Category": "Financial Performance",
        "Definition": "Total collections as a percentage of gross charges billed, before adjustments.",
        "Formula": "SUM(payments) / SUM(charges) × 100",
        "Data Sources": "charges.charge_amount, payments.payment_amount",
        "Dashboard Tab": "Executive Summary, Collections & Revenue",
    },
    {
        "Metric": "Cost to Collect",
        "Category": "Financial Performance",
        "Definition": "RCM operating cost per dollar collected — measures billing department efficiency.",
        "Formula": "Total RCM Cost / Total Collections × 100",
        "Data Sources": "operating_costs.total_rcm_cost, payments.payment_amount",
        "Dashboard Tab": "Executive Summary, Collections & Revenue",
    },
    {
        "Metric": "Bad Debt Rate",
        "Category": "Financial Performance",
        "Definition": "Percentage of charges written off as uncollectable bad debt.",
        "Formula": "Bad Debt Write-offs / Total Charges × 100",
        "Data Sources": "adjustments.adjustment_type_code, adjustments.adjustment_amount, claims.total_charge_amount",
        "Dashboard Tab": "Executive Summary",
    },
    {
        "Metric": "Average Reimbursement per Encounter",
        "Category": "Financial Performance",
        "Definition": "Average payment received per patient encounter — tracks revenue per visit.",
        "Formula": "Total Payments / Number of Encounters",
        "Data Sources": "payments.payment_amount, encounters.encounter_id",
        "Dashboard Tab": "Executive Summary",
    },
    {
        "Metric": "Clean Claim Rate",
        "Category": "Claims Quality",
        "Definition": "Percentage of claims submitted without errors that are accepted on first pass.",
        "Formula": "Clean Claims / Total Claims × 100",
        "Data Sources": "claims.is_clean_claim",
        "Dashboard Tab": "Executive Summary, Claims & Denials",
    },
    {
        "Metric": "Denial Rate",
        "Category": "Claims Quality",
        "Definition": "Percentage of submitted claims denied by payers.",
        "Formula": "Denied Claims / Total Claims × 100",
        "Data Sources": "claims.claim_status",
        "Dashboard Tab": "Executive Summary, Claims & Denials",
    },
    {
        "Metric": "First-Pass Resolution Rate",
        "Category": "Claims Quality",
        "Definition": "Percentage of claims resolved (paid or denied) on first submission without rework.",
        "Formula": "Claims Resolved on First Pass / Total Claims × 100",
        "Data Sources": "claims.claim_status, claims.is_clean_claim",
        "Dashboard Tab": "Claims & Denials",
    },
    {
        "Metric": "Charge Lag",
        "Category": "Claims Quality",
        "Definition": "Average days between date of service and claim submission — delays increase A/R.",
        "Formula": "AVG(submission_date − date_of_service) in days",
        "Data Sources": "claims.submission_date, claims.date_of_service",
        "Dashboard Tab": "Claims & Denials",
    },
    {
        "Metric": "Denial Reasons",
        "Category": "Claims Quality",
        "Definition": "Distribution of denial reason codes — identifies root causes for process improvement.",
        "Formula": "COUNT(*) GROUP BY denial_reason_code",
        "Data Sources": "denials.denial_reason_code, denials.denial_reason_description, denials.denied_amount",
        "Dashboard Tab": "Claims & Denials",
    },
    {
        "Metric": "Appeal Success Rate",
        "Category": "Recovery & Appeals",
        "Definition": "Percentage of appealed denials that are successfully overturned.",
        "Formula": "Successful Appeals / Total Appealed Denials × 100",
        "Data Sources": "denials.appeal_status",
        "Dashboard Tab": "Claims & Denials",
    },
    {
        "Metric": "A/R Aging",
        "Category": "Recovery & Appeals",
        "Definition": "Dollar value of unpaid claims bucketed by age (0-30, 31-60, 61-90, 90+ days).",
        "Formula": "SUM(outstanding_amount) GROUP BY age_bucket",
        "Data Sources": "claims.date_of_service, claims.total_charge_amount, claims.claim_status, payments.payment_amount",
        "Dashboard Tab": "A/R Aging & Cash Flow",
    },
    {
        "Metric": "Payment Accuracy Rate",
        "Category": "Recovery & Appeals",
        "Definition": "Percentage of payments received that match the contracted reimbursement amount.",
        "Formula": "Accurate Payments / Total Payments × 100",
        "Data Sources": "payments.is_accurate_payment",
        "Dashboard Tab": "Executive Summary",
    },
    {
        "Metric": "Payer Mix",
        "Category": "Segmentation",
        "Definition": "Revenue distribution across payer types (Medicare, Medicaid, Commercial, Self-pay).",
        "Formula": "SUM(payments) GROUP BY payer_type",
        "Data Sources": "payments.payment_amount, payers.payer_type, claims.payer_id",
        "Dashboard Tab": "Payer Analysis",
    },
    {
        "Metric": "Denial Rate by Payer",
        "Category": "Segmentation",
        "Definition": "Denial rate broken down per payer — identifies problematic payer relationships.",
        "Formula": "Denied Claims / Total Claims GROUP BY payer_id",
        "Data Sources": "claims.claim_status, claims.payer_id, payers.payer_name",
        "Dashboard Tab": "Payer Analysis",
    },
    {
        "Metric": "Department Performance",
        "Category": "Segmentation",
        "Definition": "Revenue and encounter volume broken down by clinical department.",
        "Formula": "SUM(payments), COUNT(encounters) GROUP BY department",
        "Data Sources": "encounters.department, payments.payment_amount, claims.encounter_id",
        "Dashboard Tab": "Department Performance",
    },
]

_TABLE_CATALOG = [
    {"Table": "payers", "Key Columns": "payer_id, payer_name, payer_type", "Description": "Insurance payer master list", "Relationships": "1-to-many → patients, claims"},
    {"Table": "patients", "Key Columns": "patient_id, primary_payer_id", "Description": "Patient demographics and primary payer", "Relationships": "Many-to-1 → payers; 1-to-many → encounters"},
    {"Table": "providers", "Key Columns": "provider_id, department", "Description": "Clinician roster with department assignment", "Relationships": "1-to-many → encounters"},
    {"Table": "encounters", "Key Columns": "encounter_id, patient_id, provider_id, date_of_service", "Description": "Individual patient visits", "Relationships": "Many-to-1 → patients, providers; 1-to-many → charges, claims"},
    {"Table": "charges", "Key Columns": "charge_id, encounter_id, charge_amount", "Description": "Line-item charges per encounter", "Relationships": "Many-to-1 → encounters"},
    {"Table": "claims", "Key Columns": "claim_id, encounter_id, payer_id, total_charge_amount, claim_status", "Description": "Insurance claims submitted for payment", "Relationships": "Many-to-1 → encounters, payers; 1-to-many → payments, denials, adjustments"},
    {"Table": "payments", "Key Columns": "payment_id, claim_id, payer_id, payment_amount", "Description": "Payments received against claims", "Relationships": "Many-to-1 → claims, payers"},
    {"Table": "denials", "Key Columns": "denial_id, claim_id, denial_reason_code, denied_amount, appeal_status", "Description": "Claim denials with reason codes and appeal tracking", "Relationships": "Many-to-1 → claims"},
    {"Table": "adjustments", "Key Columns": "adjustment_id, claim_id, adjustment_type_code, adjustment_amount", "Description": "Contractual write-offs and other balance adjustments", "Relationships": "Many-to-1 → claims"},
    {"Table": "operating_costs", "Key Columns": "period, total_rcm_cost", "Description": "Monthly RCM department operating costs", "Relationships": "Standalone (joined by period/month)"},
]


# ── Page 1: Data Catalog ──────────────────────────────────────────────

def render_data_catalog():
    """Searchable table of all 17 KPIs and 10 data tables."""
    st.title("Data Catalog")
    st.caption("Reference guide for all metrics and data tables used in this dashboard.")

    # ── KPI section ──
    st.subheader("KPI Metrics Catalog")
    col1, col2 = st.columns([2, 1])
    with col1:
        search = st.text_input("Search metrics", placeholder="e.g. denial, collection, days...")
    with col2:
        categories = ["All"] + sorted({r["Category"] for r in _KPI_CATALOG})
        cat_filter = st.selectbox("Category", categories)

    df = pd.DataFrame(_KPI_CATALOG)
    if search:
        mask = (
            df["Metric"].str.contains(search, case=False, na=False) |
            df["Definition"].str.contains(search, case=False, na=False) |
            df["Formula"].str.contains(search, case=False, na=False)
        )
        df = df[mask]
    if cat_filter != "All":
        df = df[df["Category"] == cat_filter]

    st.dataframe(df, use_container_width=True, hide_index=True)
    st.caption(f"{len(df)} of {len(_KPI_CATALOG)} metrics shown")

    st.divider()

    # ── Data tables section ──
    st.subheader("Data Tables Catalog")
    st.dataframe(pd.DataFrame(_TABLE_CATALOG), use_container_width=True, hide_index=True)


# ── Page 2: Data Lineage ──────────────────────────────────────────────

def render_data_lineage():
    """Medallion Architecture data lineage from CSV sources to the dashboard."""
    st.title("Data Lineage — Medallion Architecture")
    st.caption(
        "Three-layer medallion pipeline: "
        "**Bronze** (raw ingestion) → **Silver** (clean & typed) → "
        "**Gold** (pre-aggregated KPI views) → Dashboard."
    )

    # ── Layout constants ───────────────────────────────────────────────
    TABLE_ORDER = [
        "payers", "patients", "providers", "encounters", "charges",
        "claims", "payments", "denials", "adjustments", "operating_costs",
    ]
    Y_STEP = 1.25
    tbl_y = {t: i * Y_STEP for i, t in enumerate(TABLE_ORDER)}

    X_CSV, X_BRONZE, X_SILVER, X_GOLD, X_DASH = 0.7, 2.8, 5.2, 7.5, 9.5
    Y_TOP = tbl_y["operating_costs"] + Y_STEP * 0.8

    GOLD_VIEWS = [
        ("gold_monthly_kpis",    "gold_monthly_kpis",       tbl_y["patients"],           "Monthly KPIs: GCR, denial rate, clean claim rate by month"),
        ("gold_payer_perf",      "gold_payer_performance",  tbl_y["encounters"],          "Per-payer: denial rate, collection rate, total revenue"),
        ("gold_dept_perf",       "gold_dept_performance",   tbl_y["claims"],              "Per-dept: encounter count, charges, payments, rev/encounter"),
        ("gold_ar_aging",        "gold_ar_aging",           tbl_y["denials"],             "Open claims bucketed: 0-30, 31-60, 61-90, 91-120, 120+ days"),
        ("gold_denial_analysis", "gold_denial_analysis",    tbl_y["operating_costs"],    "Denial codes: count, denied $, recovered $, appeal win rate"),
    ]
    DASH_TABS = [
        ("tab_exec",   "Executive\nSummary",      tbl_y["payers"]),
        ("tab_rev",    "Collections &\nRevenue",  tbl_y["providers"]),
        ("tab_claims", "Claims &\nDenials",       tbl_y["charges"]),
        ("tab_ar",     "A/R Aging &\nCash Flow",  tbl_y["payments"]),
        ("tab_payer",  "Payer\nAnalysis",          tbl_y["adjustments"]),
        ("tab_dept",   "Department\nPerf.",        tbl_y["operating_costs"]),
    ]

    # ── Accumulate nodes and edges ─────────────────────────────────────
    nodes = []
    edges = []
    npos  = {}  # node_id → (x, y) for edge drawing

    def _n(nid, label, x, y, color, size, group, hover):
        nodes.append(dict(id=nid, label=label, x=x, y=y,
                          color=color, size=size, group=group, hover=hover))
        npos[nid] = (x, y)

    def _e(src, tgt):
        edges.append((src, tgt))

    # CSV source nodes
    for t in TABLE_ORDER:
        _n(f"csv_{t}", f"{t}.csv", X_CSV, tbl_y[t],
           "#5b8dee", 11, "CSV Source", f"Source file: data/{t}.csv")

    # Bronze table nodes
    for t in TABLE_ORDER:
        _n(f"bronze_{t}", f"bronze_{t}", X_BRONZE, tbl_y[t],
           "#CD7F32", 14, "Bronze Table",
           f"bronze_{t}: all TEXT columns + _loaded_at timestamp")

    # Silver table nodes
    for t in TABLE_ORDER:
        _n(f"silver_{t}", f"silver_{t}", X_SILVER, tbl_y[t],
           "#7a7a7a", 14, "Silver Table",
           f"silver_{t}: typed columns (REAL/INTEGER), FK constraints enforced")

    # Gold view nodes
    for gid, glabel, gy, ghover in GOLD_VIEWS:
        _n(gid, glabel, X_GOLD, gy, "#DAA520", 20, "Gold View", ghover)

    # Dashboard tab nodes
    for tid, tlabel, ty in DASH_TABS:
        _n(tid, tlabel, X_DASH, ty, "#f66d9b", 17, "Dashboard Tab",
           f"Dashboard tab: {tlabel.replace(chr(10), ' ')}")

    # CSV → Bronze edges (raw ingestion)
    for t in TABLE_ORDER:
        _e(f"csv_{t}", f"bronze_{t}")

    # Bronze → Silver edges (ETL: type casting & validation)
    for t in TABLE_ORDER:
        _e(f"bronze_{t}", f"silver_{t}")

    # Silver → Gold edges (SQL aggregation, representative joins)
    for src, tgt in [
        ("silver_claims",     "gold_monthly_kpis"),
        ("silver_payments",   "gold_monthly_kpis"),
        ("silver_payers",     "gold_payer_perf"),
        ("silver_claims",     "gold_payer_perf"),
        ("silver_encounters", "gold_dept_perf"),
        ("silver_charges",    "gold_dept_perf"),
        ("silver_claims",     "gold_ar_aging"),
        ("silver_denials",    "gold_denial_analysis"),
    ]:
        _e(src, tgt)

    # Gold → Dashboard edges
    for src, tgt in [
        ("gold_monthly_kpis",    "tab_exec"),
        ("gold_monthly_kpis",    "tab_rev"),
        ("gold_payer_perf",      "tab_payer"),
        ("gold_dept_perf",       "tab_dept"),
        ("gold_ar_aging",        "tab_ar"),
        ("gold_denial_analysis", "tab_claims"),
    ]:
        _e(src, tgt)

    # ── Build Plotly figure ────────────────────────────────────────────
    fig = go.Figure()

    # Zone background rectangles
    ZONE_Y0, ZONE_Y1 = -0.8, Y_TOP + 0.2
    zone_defs = [
        # x0,  x1,  fill,                          border,                        label,          tx,   color
        (-0.3,  4.0, "rgba(205,127,50,0.07)",  "rgba(205,127,50,0.30)",  "BRONZE LAYER",  1.85, "#8B4513"),
        ( 4.0,  6.8, "rgba(150,150,150,0.07)", "rgba(150,150,150,0.30)", "SILVER LAYER",  5.40, "#505050"),
        ( 6.8, 10.8, "rgba(255,215,  0,0.07)", "rgba(218,165, 32,0.30)", "GOLD LAYER",    8.80, "#7B5900"),
    ]
    for x0, x1, fill, border, zlabel, tx, tc in zone_defs:
        fig.add_shape(
            type="rect", x0=x0, y0=ZONE_Y0, x1=x1, y1=ZONE_Y1,
            fillcolor=fill, line=dict(color=border, width=1.5), layer="below",
        )
        fig.add_annotation(
            x=tx, y=Y_TOP + 0.1, text=f"<b>{zlabel}</b>",
            showarrow=False, font=dict(size=12, color=tc), xanchor="center",
        )

    # ETL step label
    fig.add_annotation(
        x=(X_BRONZE + X_SILVER) / 2, y=Y_TOP - 0.1,
        text="<i>ETL →</i>",
        showarrow=False, font=dict(size=9, color="#888"),
        bgcolor="rgba(255,255,255,0.8)",
    )

    # Draw edges
    for src_id, tgt_id in edges:
        sx, sy = npos[src_id]
        tx, ty = npos[tgt_id]
        fig.add_trace(go.Scatter(
            x=[sx, tx, None], y=[sy, ty, None],
            mode="lines",
            line=dict(width=0.8, color="#c8c8c8"),
            hoverinfo="none",
            showlegend=False,
        ))

    # Draw nodes, grouped by layer for the legend
    color_groups: dict = {}
    for n in nodes:
        color_groups.setdefault(n["group"], []).append(n)

    for group, gnodes in color_groups.items():
        fig.add_trace(go.Scatter(
            x=[n["x"] for n in gnodes],
            y=[n["y"] for n in gnodes],
            mode="markers+text",
            marker=dict(
                size=[n["size"] for n in gnodes],
                color=[n["color"] for n in gnodes],
                line=dict(width=1, color="white"),
            ),
            text=[n["label"] for n in gnodes],
            textposition="bottom center",
            textfont=dict(size=7),
            hovertext=[n.get("hover", n["label"]) for n in gnodes],
            hoverinfo="text",
            name=group,
            showlegend=True,
        ))

    fig.update_layout(
        title="Data Pipeline — Medallion Architecture (Bronze → Silver → Gold)",
        height=800,
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False,
                   range=[-0.6, 11.2]),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False,
                   range=[-1.3, Y_TOP + 0.8]),
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin=dict(l=20, r=20, t=55, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── Medallion pipeline stages table ───────────────────────────────
    st.subheader("Medallion Pipeline Stages")
    pipeline_table = [
        {
            "Layer":       "Source",
            "Stage":       "1. CSV Ingest",
            "Component":   "data/*.csv (10 files)",
            "Input":       "—",
            "Output":      "Raw CSV rows",
            "Description": "Original source files; loaded once per database init.",
        },
        {
            "Layer":       "Bronze",
            "Stage":       "2. Raw Landing",
            "Component":   "database.py → load_csv_to_bronze()",
            "Input":       "CSV files",
            "Output":      "bronze_* tables (all TEXT)",
            "Description": "Data lands as-is. All columns TEXT. _loaded_at timestamp records ingestion time.",
        },
        {
            "Layer":       "Silver",
            "Stage":       "3. ETL Transform",
            "Component":   "database.py → _etl_bronze_to_silver()",
            "Input":       "bronze_* tables",
            "Output":      "silver_* tables (typed)",
            "Description": "CAST to REAL/INTEGER, normalise booleans ('True'→1), enforce FK constraints, skip NULL PKs.",
        },
        {
            "Layer":       "Silver",
            "Stage":       "4. Load to DataFrames",
            "Component":   "data_loader.py → load_all_data()",
            "Input":       "silver_* tables",
            "Output":      "Dict of DataFrames",
            "Description": "Reads all 10 Silver tables; parses dates and booleans; cached by Streamlit @st.cache_data.",
        },
        {
            "Layer":       "Silver",
            "Stage":       "5. Validate",
            "Component":   "validators.py → validate_all()",
            "Input":       "DataFrames",
            "Output":      "Issue list",
            "Description": "17 integrity checks on Silver DataFrames; issues shown in sidebar Data Quality expander.",
        },
        {
            "Layer":       "Silver",
            "Stage":       "6. Filter",
            "Component":   "app.py sidebar widgets",
            "Input":       "DataFrames",
            "Output":      "Filtered DataFrames",
            "Description": "Date range, payer, department, encounter-type filters applied in-memory on Silver data.",
        },
        {
            "Layer":       "Gold",
            "Stage":       "7. Gold Views",
            "Component":   "database.py → gold_* SQL VIEWs (5 views)",
            "Input":       "silver_* tables",
            "Output":      "Pre-aggregated KPI tables",
            "Description": "SQL VIEWs join & aggregate Silver tables by month, payer, dept, aging bucket, denial code.",
        },
        {
            "Layer":       "Gold",
            "Stage":       "8. KPI Metrics",
            "Component":   "metrics.py (17 functions)",
            "Input":       "Filtered Silver DataFrames",
            "Output":      "Scalar / Series KPI results",
            "Description": "Each KPI function computes one metric from the filtered Silver data.",
        },
        {
            "Layer":       "Presentation",
            "Stage":       "9. Visualize",
            "Component":   "app.py tabs 1–6",
            "Input":       "Metric results",
            "Output":      "Plotly charts, KPI scorecards",
            "Description": "6 dashboard tabs render charts and KPI cards from metric outputs.",
        },
    ]
    st.dataframe(pd.DataFrame(pipeline_table), use_container_width=True, hide_index=True)


# ── Page 3: Knowledge Graph ───────────────────────────────────────────

def render_knowledge_graph():
    """Interactive entity-relationship diagram of the 10 data entities."""
    st.title("Knowledge Graph")
    st.caption("Entity relationships across the 10 data tables. Hover nodes for column details.")

    nodes = [
        # Reference entities (blue) — outer ring top
        {"id": "payers",    "label": "payers",    "x": 5.0, "y": 9.0, "color": "#5b8dee", "size": 30,
         "group": "Reference", "hover": "payer_id, payer_name, payer_type"},
        {"id": "patients",  "label": "patients",  "x": 1.5, "y": 7.0, "color": "#5b8dee", "size": 30,
         "group": "Reference", "hover": "patient_id, primary_payer_id"},
        {"id": "providers", "label": "providers", "x": 8.5, "y": 7.0, "color": "#5b8dee", "size": 30,
         "group": "Reference", "hover": "provider_id, department"},
        # Central hub
        {"id": "encounters", "label": "encounters", "x": 5.0, "y": 5.5, "color": "#38c172", "size": 36,
         "group": "Transactional", "hover": "encounter_id, patient_id, provider_id, date_of_service, department, encounter_type"},
        # Claims hub
        {"id": "claims", "label": "claims", "x": 5.0, "y": 3.0, "color": "#38c172", "size": 36,
         "group": "Transactional", "hover": "claim_id, encounter_id, patient_id, payer_id, date_of_service, submission_date, total_charge_amount, claim_status, is_clean_claim"},
        # Leaf transactional nodes
        {"id": "charges",     "label": "charges",     "x": 1.5, "y": 4.5, "color": "#38c172", "size": 26,
         "group": "Transactional", "hover": "charge_id, encounter_id, charge_amount, service_date, post_date"},
        {"id": "payments",    "label": "payments",    "x": 2.5, "y": 1.0, "color": "#38c172", "size": 26,
         "group": "Transactional", "hover": "payment_id, claim_id, payer_id, payment_amount, is_accurate_payment"},
        {"id": "denials",     "label": "denials",     "x": 5.0, "y": 0.5, "color": "#38c172", "size": 26,
         "group": "Transactional", "hover": "denial_id, claim_id, denial_reason_code, denied_amount, appeal_status, recovered_amount"},
        {"id": "adjustments", "label": "adjustments", "x": 7.5, "y": 1.0, "color": "#38c172", "size": 26,
         "group": "Transactional", "hover": "adjustment_id, claim_id, adjustment_type_code, adjustment_amount"},
        # Operational
        {"id": "operating_costs", "label": "operating\ncosts", "x": 9.0, "y": 4.5, "color": "#e8a838", "size": 26,
         "group": "Operational", "hover": "period, total_rcm_cost"},
    ]

    edges = [
        {"source": "payers",    "target": "patients",      "label": "1:N (primary_payer_id)"},
        {"source": "patients",  "target": "encounters",    "label": "1:N (patient_id)"},
        {"source": "providers", "target": "encounters",    "label": "1:N (provider_id)"},
        {"source": "encounters","target": "charges",       "label": "1:N (encounter_id)"},
        {"source": "encounters","target": "claims",        "label": "1:N (encounter_id)"},
        {"source": "payers",    "target": "claims",        "label": "1:N (payer_id)"},
        {"source": "claims",    "target": "payments",      "label": "1:N (claim_id)"},
        {"source": "claims",    "target": "denials",       "label": "1:N (claim_id)"},
        {"source": "claims",    "target": "adjustments",   "label": "1:N (claim_id)"},
    ]

    _draw_network_graph(nodes, edges, "Entity Relationship Diagram", height=620)

    # Legend
    st.markdown("""
| Color | Category | Tables |
|-------|----------|--------|
| 🔵 Blue | Reference / Master data | payers, patients, providers |
| 🟢 Green | Transactional | encounters, charges, claims, payments, denials, adjustments |
| 🟠 Orange | Operational | operating_costs |
""")

    st.subheader("Relationships")
    rel_table = [
        {"Parent Table": "payers",    "Child Table": "patients",     "Join Column": "primary_payer_id", "Cardinality": "1:N", "Business Meaning": "Each patient has one primary payer"},
        {"Parent Table": "payers",    "Child Table": "claims",       "Join Column": "payer_id",         "Cardinality": "1:N", "Business Meaning": "Claims are billed to one payer"},
        {"Parent Table": "patients",  "Child Table": "encounters",   "Join Column": "patient_id",       "Cardinality": "1:N", "Business Meaning": "A patient can have many visits"},
        {"Parent Table": "providers", "Child Table": "encounters",   "Join Column": "provider_id",      "Cardinality": "1:N", "Business Meaning": "A provider sees many patients"},
        {"Parent Table": "encounters","Child Table": "charges",      "Join Column": "encounter_id",     "Cardinality": "1:N", "Business Meaning": "Each visit generates line-item charges"},
        {"Parent Table": "encounters","Child Table": "claims",       "Join Column": "encounter_id",     "Cardinality": "1:N", "Business Meaning": "Each visit produces one or more insurance claims"},
        {"Parent Table": "claims",    "Child Table": "payments",     "Join Column": "claim_id",         "Cardinality": "1:N", "Business Meaning": "A claim may receive partial or split payments"},
        {"Parent Table": "claims",    "Child Table": "denials",      "Join Column": "claim_id",         "Cardinality": "1:N", "Business Meaning": "A claim can be denied once or multiple times"},
        {"Parent Table": "claims",    "Child Table": "adjustments",  "Join Column": "claim_id",         "Cardinality": "1:N", "Business Meaning": "Contractual write-offs are applied per claim"},
    ]
    st.dataframe(pd.DataFrame(rel_table), use_container_width=True, hide_index=True)


# ── Page 4: Semantic Layer ────────────────────────────────────────────

def render_semantic_layer():
    """Business concept → KPI → raw column mapping."""
    st.title("Semantic Layer")
    st.caption("How business questions map to KPIs and raw data columns.")

    # ── Business Concept → KPI graph ──
    st.subheader("Business Concept Map")

    concepts = [
        ("Revenue",              4.5, 8.0, "#e3342f"),
        ("Collections",          1.5, 6.0, "#e8a838"),
        ("Claims Quality",       4.5, 6.0, "#38c172"),
        ("A/R Health",           7.5, 6.0, "#5b8dee"),
        ("Payer Performance",    2.0, 3.5, "#9561e2"),
        ("Dept Performance",     7.0, 3.5, "#f66d9b"),
        ("Recovery & Appeals",   4.5, 3.5, "#20c997"),
    ]
    kpis = [
        ("NCR",           1.0, 4.5, "#e8a838", "Collections"),
        ("GCR",           2.0, 4.5, "#e8a838", "Collections"),
        ("Cost-to-Collect",3.0, 4.5, "#e8a838", "Collections"),
        ("DAR",           6.5, 4.5, "#5b8dee", "A/R Health"),
        ("A/R Aging",     7.5, 4.5, "#5b8dee", "A/R Health"),
        ("Pmt Accuracy",  8.5, 4.5, "#5b8dee", "A/R Health"),
        ("Clean Claim",   3.5, 4.5, "#38c172", "Claims Quality"),
        ("Denial Rate",   4.5, 4.5, "#38c172", "Claims Quality"),
        ("First-Pass",    5.5, 4.5, "#38c172", "Claims Quality"),
        ("Charge Lag",    4.5, 2.5, "#38c172", "Claims Quality"),
        ("Bad Debt",      4.5, 7.0, "#e3342f", "Revenue"),
        ("Avg Reimb",     5.5, 7.0, "#e3342f", "Revenue"),
        ("Appeal Success",3.5, 2.5, "#20c997", "Recovery & Appeals"),
        ("Denial Reasons",5.5, 2.5, "#20c997", "Recovery & Appeals"),
        ("Payer Mix",     1.5, 2.0, "#9561e2", "Payer Performance"),
        ("Denial/Payer",  2.5, 2.0, "#9561e2", "Payer Performance"),
        ("Dept Perf.",    7.0, 2.0, "#f66d9b", "Dept Performance"),
    ]

    nodes = []
    for name, x, y, color in concepts:
        nodes.append({"id": name, "label": name, "x": x, "y": y, "color": color,
                      "size": 34, "group": "Business Concept", "hover": f"Business Concept: {name}"})
    for name, x, y, color, parent in kpis:
        nodes.append({"id": name, "label": name, "x": x, "y": y, "color": color,
                      "size": 20, "group": "KPI", "hover": f"KPI: {name} → {parent}"})

    edges = [{"source": parent, "target": name} for name, x, y, color, parent in kpis]

    _draw_network_graph(nodes, edges, "Business Concepts → KPIs", height=580)

    st.divider()

    # ── Semantic mapping table ──
    st.subheader("Semantic Mapping")
    semantic = [
        {"Business Concept": "Revenue",     "KPI": "Gross Collection Rate",       "Raw Columns": "charges.charge_amount, payments.payment_amount",              "Transformation": "SUM(payments)/SUM(charges)×100",              "Business Rule": "Measures total collections vs. gross billed"},
        {"Business Concept": "Revenue",     "KPI": "Bad Debt Rate",               "Raw Columns": "adjustments.adjustment_type_code/amount, claims.total_charge_amount", "Transformation": "SUM(bad_debt_adj)/SUM(charges)×100",     "Business Rule": "Write-offs where type_code indicates bad debt"},
        {"Business Concept": "Revenue",     "KPI": "Avg Reimbursement/Encounter", "Raw Columns": "payments.payment_amount, encounters.encounter_id",             "Transformation": "SUM(payments)/COUNT(encounters)",             "Business Rule": "Revenue efficiency per patient visit"},
        {"Business Concept": "Collections", "KPI": "Net Collection Rate",         "Raw Columns": "payments.payment_amount, claims.total_charge_amount, adjustments.adjustment_amount", "Transformation": "Payments/(Charges−Adjustments)×100", "Business Rule": "Adjustments remove contractually non-collectible amounts"},
        {"Business Concept": "Collections", "KPI": "Cost to Collect",             "Raw Columns": "operating_costs.total_rcm_cost, payments.payment_amount",     "Transformation": "RCM Cost/Collections×100",                    "Business Rule": "Billing dept efficiency; target <3%"},
        {"Business Concept": "Claims Quality","KPI": "Clean Claim Rate",          "Raw Columns": "claims.is_clean_claim",                                        "Transformation": "SUM(is_clean_claim)/COUNT(claims)×100",       "Business Rule": "Claims passing payer edits on first submission"},
        {"Business Concept": "Claims Quality","KPI": "Denial Rate",               "Raw Columns": "claims.claim_status",                                          "Transformation": "COUNT(status='Denied')/COUNT(claims)×100",    "Business Rule": "Industry benchmark <5%"},
        {"Business Concept": "Claims Quality","KPI": "First-Pass Rate",           "Raw Columns": "claims.claim_status, claims.is_clean_claim",                  "Transformation": "Resolved on first pass/Total×100",            "Business Rule": "Resolved = Paid or legitimately Denied w/o rework"},
        {"Business Concept": "Claims Quality","KPI": "Charge Lag",                "Raw Columns": "claims.submission_date, claims.date_of_service",              "Transformation": "AVG(submission_date − date_of_service)",      "Business Rule": "Target <3 days; delays increase A/R balance"},
        {"Business Concept": "A/R Health",  "KPI": "Days in A/R",                "Raw Columns": "claims.total_charge_amount, payments.payment_amount",         "Transformation": "(Charges−Payments)/(Monthly Charges/30)",     "Business Rule": "Target <40 days; >50 is critical"},
        {"Business Concept": "A/R Health",  "KPI": "A/R Aging",                  "Raw Columns": "claims.date_of_service/claim_status, payments.payment_amount","Transformation": "Outstanding bucketed by age in days",          "Business Rule": "90+ day bucket should be <15% of total A/R"},
        {"Business Concept": "A/R Health",  "KPI": "Payment Accuracy Rate",      "Raw Columns": "payments.is_accurate_payment",                                "Transformation": "SUM(is_accurate_payment)/COUNT×100",          "Business Rule": "Inaccurate payments require follow-up with payer"},
        {"Business Concept": "Recovery",    "KPI": "Appeal Success Rate",         "Raw Columns": "denials.appeal_status",                                        "Transformation": "Successful/Total Appealed×100",               "Business Rule": "Target >50%; tracks ability to recover denied revenue"},
        {"Business Concept": "Payer Perf.", "KPI": "Payer Mix",                   "Raw Columns": "payments.payment_amount, payers.payer_type, claims.payer_id", "Transformation": "SUM(payments) GROUP BY payer_type",           "Business Rule": "High self-pay mix → higher collection risk"},
        {"Business Concept": "Payer Perf.", "KPI": "Denial Rate by Payer",        "Raw Columns": "claims.claim_status/payer_id, payers.payer_name",             "Transformation": "Denied/Total GROUP BY payer",                 "Business Rule": "Identifies payers with problematic contracts/edits"},
        {"Business Concept": "Dept Perf.", "KPI": "Department Performance",       "Raw Columns": "encounters.department, payments.payment_amount, claims.encounter_id", "Transformation": "SUM(payments), COUNT(encounters) GROUP BY dept", "Business Rule": "Revenue and volume by clinical department"},
    ]
    st.dataframe(pd.DataFrame(semantic), use_container_width=True, hide_index=True)

    st.divider()

    # ── Filter cascade diagram ──
    st.subheader("Sidebar Filter Cascade")
    st.markdown("""
How the sidebar filters propagate through the data model:

```
Date Range + Payer + Department + Encounter Type
        │
        ▼
    claims  (filtered directly)
        │
        ├──▶  payments    (claim_id ∈ filtered claims)
        ├──▶  denials     (claim_id ∈ filtered claims)
        ├──▶  adjustments (claim_id ∈ filtered claims)
        │
        └──▶  encounters  (encounter_id ∈ filtered claims)
                  │
                  └──▶  charges (encounter_id ∈ filtered encounters)

All 17 KPI functions receive the filtered DataFrames → metrics reflect current filter selection.
```
""")

