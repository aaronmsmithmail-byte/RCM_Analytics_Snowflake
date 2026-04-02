"""
Metadata Pages for Snowflake RCM Analytics Dashboard
=====================================================

Six sidebar metadata pages sourced from Snowflake METADATA schema
with static fallback constants. Replaces DuckDB/Cube/Neo4j sources.
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from snowflake.snowpark.context import get_active_session


# ===========================================================================
# Helper: query metadata tables from Snowflake
# ===========================================================================

def _query_meta(sql):
    """Execute a metadata query and return a DataFrame. Returns empty on error."""
    try:
        session = get_active_session()
        df = session.sql(sql).to_pandas()
        df.columns = [c.lower() if c == c.upper() else c for c in df.columns]
        return df
    except Exception:
        return pd.DataFrame()


# ===========================================================================
# Static data constants (fallback + layout)
# ===========================================================================

_KG_NODES = [
    {"id": "payers", "label": "payers", "x": 5.0, "y": 9.0, "color": "#5b8dee", "size": 30, "group": "Reference"},
    {"id": "patients", "label": "patients", "x": 1.5, "y": 7.0, "color": "#5b8dee", "size": 30, "group": "Reference"},
    {"id": "providers", "label": "providers", "x": 8.5, "y": 7.0, "color": "#5b8dee", "size": 30, "group": "Reference"},
    {"id": "encounters", "label": "encounters", "x": 5.0, "y": 5.5, "color": "#38c172", "size": 36, "group": "Transactional"},
    {"id": "claims", "label": "claims", "x": 5.0, "y": 3.0, "color": "#38c172", "size": 36, "group": "Transactional"},
    {"id": "charges", "label": "charges", "x": 1.5, "y": 4.5, "color": "#38c172", "size": 26, "group": "Transactional"},
    {"id": "payments", "label": "payments", "x": 2.5, "y": 1.0, "color": "#38c172", "size": 26, "group": "Transactional"},
    {"id": "denials", "label": "denials", "x": 5.0, "y": 0.5, "color": "#38c172", "size": 26, "group": "Transactional"},
    {"id": "adjustments", "label": "adjustments", "x": 7.5, "y": 1.0, "color": "#38c172", "size": 26, "group": "Transactional"},
    {"id": "operating_costs", "label": "operating\ncosts", "x": 9.0, "y": 4.5, "color": "#e8a838", "size": 26, "group": "Operational"},
]

_KG_EDGES = [
    {"source": "payers", "target": "patients", "label": "1:N (primary_payer_id)"},
    {"source": "patients", "target": "encounters", "label": "1:N (patient_id)"},
    {"source": "providers", "target": "encounters", "label": "1:N (provider_id)"},
    {"source": "encounters", "target": "charges", "label": "1:N (encounter_id)"},
    {"source": "encounters", "target": "claims", "label": "1:N (encounter_id)"},
    {"source": "payers", "target": "claims", "label": "1:N (payer_id)"},
    {"source": "patients", "target": "claims", "label": "1:N (patient_id)"},
    {"source": "payers", "target": "payments", "label": "1:N (payer_id)"},
    {"source": "claims", "target": "payments", "label": "1:N (claim_id)"},
    {"source": "claims", "target": "denials", "label": "1:N (claim_id)"},
    {"source": "claims", "target": "adjustments", "label": "1:N (claim_id)"},
]

_TABLE_CATALOG = [
    {"Layer": "Bronze", "Table": "BRONZE.PAYERS", "Source": "Payer Master", "Description": "Raw CSV — insurance payer list"},
    {"Layer": "Bronze", "Table": "BRONZE.PATIENTS", "Source": "EHR", "Description": "Raw CSV — patient demographics"},
    {"Layer": "Bronze", "Table": "BRONZE.PROVIDERS", "Source": "EHR", "Description": "Raw CSV — clinician roster"},
    {"Layer": "Bronze", "Table": "BRONZE.ENCOUNTERS", "Source": "EHR", "Description": "Raw CSV — patient visits"},
    {"Layer": "Bronze", "Table": "BRONZE.CHARGES", "Source": "EHR / Charge Capture", "Description": "Raw CSV — line-item charges"},
    {"Layer": "Bronze", "Table": "BRONZE.CLAIMS", "Source": "Clearinghouse", "Description": "Raw CSV — insurance claims"},
    {"Layer": "Bronze", "Table": "BRONZE.PAYMENTS", "Source": "Clearinghouse / ERA", "Description": "Raw CSV — payments received"},
    {"Layer": "Bronze", "Table": "BRONZE.DENIALS", "Source": "Clearinghouse / ERA", "Description": "Raw CSV — claim denials"},
    {"Layer": "Bronze", "Table": "BRONZE.ADJUSTMENTS", "Source": "Billing System", "Description": "Raw CSV — write-offs & adjustments"},
    {"Layer": "Bronze", "Table": "BRONZE.OPERATING_COSTS", "Source": "ERP / Finance", "Description": "Raw CSV — monthly RCM costs"},
    {"Layer": "Silver", "Table": "SILVER.PAYERS", "Source": "ETL", "Description": "Typed — insurance payer master"},
    {"Layer": "Silver", "Table": "SILVER.PATIENTS", "Source": "ETL", "Description": "Typed — patient demographics + FK"},
    {"Layer": "Silver", "Table": "SILVER.PROVIDERS", "Source": "ETL", "Description": "Typed — clinician roster"},
    {"Layer": "Silver", "Table": "SILVER.ENCOUNTERS", "Source": "ETL", "Description": "Typed — patient visits + FK"},
    {"Layer": "Silver", "Table": "SILVER.CHARGES", "Source": "ETL", "Description": "Typed — charges with FLOAT amounts"},
    {"Layer": "Silver", "Table": "SILVER.CLAIMS", "Source": "ETL", "Description": "Typed — claims with status + clean flag"},
    {"Layer": "Silver", "Table": "SILVER.PAYMENTS", "Source": "ETL", "Description": "Typed — payments with accuracy flag"},
    {"Layer": "Silver", "Table": "SILVER.DENIALS", "Source": "ETL", "Description": "Typed — denials with appeal tracking"},
    {"Layer": "Silver", "Table": "SILVER.ADJUSTMENTS", "Source": "ETL", "Description": "Typed — financial adjustments"},
    {"Layer": "Silver", "Table": "SILVER.OPERATING_COSTS", "Source": "ETL", "Description": "Typed — monthly RCM costs"},
    {"Layer": "Gold", "Table": "GOLD.MONTHLY_KPIS", "Source": "View", "Description": "Monthly KPI rollup"},
    {"Layer": "Gold", "Table": "GOLD.PAYER_PERFORMANCE", "Source": "View", "Description": "Payer performance summary"},
    {"Layer": "Gold", "Table": "GOLD.DEPARTMENT_PERFORMANCE", "Source": "View", "Description": "Department performance"},
    {"Layer": "Gold", "Table": "GOLD.AR_AGING", "Source": "View", "Description": "A/R aging buckets"},
    {"Layer": "Gold", "Table": "GOLD.DENIAL_ANALYSIS", "Source": "View", "Description": "Denial reason analysis"},
]

# Aliases for backward compatibility
_KPI_CATALOG = []
_SEMANTIC_LAYER = []
_KG_RELATIONSHIPS = [
    {"parent_table": "payers", "child_table": "patients", "join_column": "PRIMARY_PAYER_ID", "cardinality": "1:N", "business_meaning": "Each patient has one primary payer"},
    {"parent_table": "payers", "child_table": "claims", "join_column": "PAYER_ID", "cardinality": "1:N", "business_meaning": "Claims are billed to one payer"},
    {"parent_table": "patients", "child_table": "encounters", "join_column": "PATIENT_ID", "cardinality": "1:N", "business_meaning": "A patient can have many visits"},
    {"parent_table": "patients", "child_table": "claims", "join_column": "PATIENT_ID", "cardinality": "1:N", "business_meaning": "Claims track which patient received services"},
    {"parent_table": "payers", "child_table": "payments", "join_column": "PAYER_ID", "cardinality": "1:N", "business_meaning": "Payments remitted by a specific payer"},
    {"parent_table": "providers", "child_table": "encounters", "join_column": "PROVIDER_ID", "cardinality": "1:N", "business_meaning": "A provider sees many patients"},
    {"parent_table": "encounters", "child_table": "charges", "join_column": "ENCOUNTER_ID", "cardinality": "1:N", "business_meaning": "Each visit generates line-item charges"},
    {"parent_table": "encounters", "child_table": "claims", "join_column": "ENCOUNTER_ID", "cardinality": "1:N", "business_meaning": "Each visit produces insurance claims"},
    {"parent_table": "claims", "child_table": "payments", "join_column": "CLAIM_ID", "cardinality": "1:N", "business_meaning": "A claim may receive partial or split payments"},
    {"parent_table": "claims", "child_table": "denials", "join_column": "CLAIM_ID", "cardinality": "1:N", "business_meaning": "A claim can be denied one or more times"},
    {"parent_table": "claims", "child_table": "adjustments", "join_column": "CLAIM_ID", "cardinality": "1:N", "business_meaning": "Contractual write-offs applied per claim"},
]


# ===========================================================================
# Page 1: Data Catalog
# ===========================================================================

def render_data_catalog():
    st.title("Data Catalog")
    st.caption("KPI metrics and data tables reference. Sourced from METADATA.KPI_CATALOG.")

    st.subheader("KPI Metrics Catalog")
    raw = _query_meta("""
        SELECT METRIC_NAME AS "Metric", CATEGORY AS "Category",
               DEFINITION AS "Definition", FORMULA AS "Formula",
               COALESCE(BENCHMARK, '—') AS "Benchmark"
        FROM RCM_ANALYTICS.METADATA.KPI_CATALOG
        ORDER BY CATEGORY, METRIC_NAME
    """)
    if raw.empty:
        st.info("Metadata table not populated yet. Run seed_metadata.sql first.")
        return

    col1, col2 = st.columns([2, 1])
    with col1:
        search = st.text_input("Search metrics", placeholder="e.g. denial, collection, days...")
    with col2:
        categories = ["All"] + sorted(raw["Category"].dropna().unique().tolist())
        cat_filter = st.selectbox("Category", categories)

    df = raw.copy()
    if search:
        mask = (df["Metric"].str.contains(search, case=False, na=False) |
                df["Definition"].str.contains(search, case=False, na=False) |
                df["Formula"].str.contains(search, case=False, na=False))
        df = df[mask]
    if cat_filter != "All":
        df = df[df["Category"] == cat_filter]

    st.metric("Metrics shown", f"{len(df)} / {len(raw)}")
    st.dataframe(df, use_container_width=True, hide_index=True)

    st.subheader("Data Tables Reference")
    st.dataframe(pd.DataFrame(_TABLE_CATALOG), use_container_width=True, hide_index=True)


# ===========================================================================
# Page 2: Data Lineage
# ===========================================================================

def render_data_lineage():
    st.title("Data Lineage")
    st.caption("Medallion architecture data flow: CSV → Bronze → Silver → Gold → Dashboard")

    st.markdown("""
    ```
    CSV Files (10 sources)
        ↓  COPY INTO @RCM_STAGE
    ┌─────────────────────────────────────────┐
    │  BRONZE Schema  (10 tables, all VARCHAR) │
    └─────────────────┬───────────────────────┘
                      ↓  SP_BRONZE_TO_SILVER()
    ┌─────────────────────────────────────────┐
    │  SILVER Schema  (10 typed tables + FK)   │
    └─────────────────┬───────────────────────┘
                      ↓  SQL Views
    ┌─────────────────────────────────────────┐
    │  GOLD Schema    (5 aggregated views)     │
    └─────────────────┬───────────────────────┘
                      ↓
    ┌─────────────────────────────────────────┐
    │  Streamlit in Snowflake Dashboard        │
    │  + Cortex Analyst AI Chat                │
    └─────────────────────────────────────────┘
    ```
    """)

    st.subheader("Pipeline Components")
    components = pd.DataFrame([
        {"Stage": "1. File Upload", "Component": "@STAGING.RCM_STAGE", "Description": "Internal Snowflake stage for CSV files"},
        {"Stage": "2. Bronze Load", "Component": "COPY INTO", "Description": "Raw CSV → Bronze tables (all VARCHAR)"},
        {"Stage": "3. Silver ETL", "Component": "SP_BRONZE_TO_SILVER()", "Description": "Type casting, validation, FK enforcement"},
        {"Stage": "4. Gold Views", "Component": "SQL Views", "Description": "Pre-aggregated business KPIs"},
        {"Stage": "5. Dashboard", "Component": "Streamlit in Snowflake", "Description": "12-tab interactive analytics"},
        {"Stage": "6. AI Chat", "Component": "Cortex Analyst", "Description": "Natural language → SQL via semantic model"},
        {"Stage": "7. Scheduling", "Component": "DAILY_ETL_TASK", "Description": "Cron-based daily refresh at 6 AM ET"},
    ])
    st.dataframe(components, use_container_width=True, hide_index=True)


# ===========================================================================
# Page 3: Knowledge Graph
# ===========================================================================

def render_knowledge_graph():
    st.title("Knowledge Graph")
    st.caption("Entity-relationship diagram of the Silver layer data model.")

    # Query relationships from Snowflake
    edges_df = _query_meta("""
        SELECT PARENT_ENTITY, CHILD_ENTITY, JOIN_COLUMN, CARDINALITY, BUSINESS_MEANING
        FROM RCM_ANALYTICS.METADATA.KG_EDGES
    """)
    if edges_df.empty:
        edges_df = pd.DataFrame(_KG_RELATIONSHIPS)

    # Build Plotly network graph
    node_x, node_y, node_text, node_color, node_size = [], [], [], [], []
    for n in _KG_NODES:
        node_x.append(n["x"])
        node_y.append(n["y"])
        node_text.append(n["label"])
        node_color.append(n["color"])
        node_size.append(n["size"])

    node_map = {n["id"]: (n["x"], n["y"]) for n in _KG_NODES}
    edge_x, edge_y = [], []
    for e in _KG_EDGES:
        if e["source"] in node_map and e["target"] in node_map:
            x0, y0 = node_map[e["source"]]
            x1, y1 = node_map[e["target"]]
            edge_x += [x0, x1, None]
            edge_y += [y0, y1, None]

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=edge_x, y=edge_y, mode="lines",
                             line=dict(width=1.5, color="#888"), hoverinfo="none"))
    fig.add_trace(go.Scatter(x=node_x, y=node_y, mode="markers+text",
                             marker=dict(size=node_size, color=node_color, line=dict(width=1, color="white")),
                             text=node_text, textposition="top center",
                             hoverinfo="text"))
    fig.update_layout(showlegend=False, height=500,
                      xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                      yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                      margin=dict(l=20, r=20, t=20, b=20))
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Foreign Key Relationships")
    st.dataframe(edges_df, use_container_width=True, hide_index=True)


# ===========================================================================
# Page 4: Semantic Layer
# ===========================================================================

def render_semantic_layer():
    st.title("Semantic Layer")
    st.caption("Business concept → KPI → Silver column mappings for Cortex Analyst.")

    df = _query_meta("""
        SELECT BUSINESS_CONCEPT, KPI_NAME, SILVER_COLUMNS, FORMULA, BUSINESS_RULE
        FROM RCM_ANALYTICS.METADATA.SEMANTIC_LAYER
        ORDER BY BUSINESS_CONCEPT, KPI_NAME
    """)
    if df.empty:
        st.info("Semantic layer not populated yet. Run seed_metadata.sql first.")
        return

    concepts = ["All"] + sorted(df.iloc[:, 0].dropna().unique().tolist())
    selected = st.selectbox("Business Concept", concepts)
    if selected != "All":
        df = df[df.iloc[:, 0] == selected]

    st.dataframe(df, use_container_width=True, hide_index=True)

    st.subheader("Cortex Analyst Semantic Model")
    st.markdown("""
    The Cortex Analyst semantic model (`rcm_semantic_model.yaml`) is staged at
    `@RCM_ANALYTICS.STAGING.RCM_STAGE/cortex/`. It defines:

    - **10 tables** (all Silver-layer entities)
    - **Measures** (SUM, COUNT, computed rates)
    - **Time dimensions** (date_of_service, payment_date, etc.)
    - **Relationships** (11 foreign-key joins)
    - **Synonyms** for natural language understanding
    """)


# ===========================================================================
# Page 5: AI Architecture
# ===========================================================================

def render_ai_architecture():
    st.title("AI Architecture")
    st.caption("How Cortex Analyst powers the AI Assistant tab.")

    st.markdown("""
    ### Architecture Overview

    ```
    User Question (natural language)
         ↓
    Cortex Analyst API
         ↓
    Semantic Model (rcm_semantic_model.yaml)
         ↓
    SQL Generation → Snowflake Execution
         ↓
    Results + Explanation → User
    ```

    ### How It Works

    1. **User asks a question** in the AI Assistant tab
    2. **Cortex Analyst** receives the question + conversation history
    3. The **semantic model YAML** (staged in Snowflake) provides:
       - Table schemas and descriptions
       - Pre-defined measures and dimensions
       - Column synonyms for natural language understanding
       - Join relationships between tables
    4. Cortex Analyst **generates SQL** based on the semantic model
    5. The SQL is **executed against Snowflake** Silver tables
    6. Results are **returned with an explanation**

    ### Key Differences from Previous Architecture

    | Feature | Previous (OpenRouter) | Current (Cortex Analyst) |
    |---------|----------------------|-------------------------|
    | LLM Provider | OpenRouter (external) | Snowflake Cortex (native) |
    | Data Access | DuckDB local file | Snowflake warehouse |
    | Schema Context | Dynamic from meta_* tables | Semantic model YAML |
    | SQL Validation | Custom regex filter | Built-in (SELECT only) |
    | Authentication | API key in .env | Snowflake session (automatic) |
    """)


# ===========================================================================
# Page 6: Data Validation
# ===========================================================================

def render_data_validation(issues=None):
    st.title("Data Validation")
    st.caption("Data quality checks against the Silver layer.")

    if issues is None:
        issues = []

    total = len(issues)
    errors = sum(1 for i in issues if not i.get("passed", True))
    passed = total - errors

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Checks", total)
    col2.metric("Passed", passed)
    col3.metric("Failed", errors)

    if issues:
        df = pd.DataFrame(issues)
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No validation results available. Run validators to see results.")


# ===========================================================================
# Page 7: Feature Backlog
# ===========================================================================

def render_feature_backlog():
    st.title("Feature Backlog")
    st.caption("Planned features and enhancements.")

    df = _query_meta("""
        SELECT TITLE, DESCRIPTION, PRIORITY, STATUS, CREATED_AT
        FROM RCM_ANALYTICS.METADATA.FEATURE_BACKLOG
        ORDER BY
            CASE PRIORITY WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 WHEN 'Low' THEN 3 ELSE 4 END,
            CREATED_AT DESC
    """)
    if df.empty:
        st.info("No backlog items found.")
        return

    priorities = ["All"] + sorted(df.iloc[:, 2].dropna().unique().tolist())
    selected = st.selectbox("Priority Filter", priorities)
    if selected != "All":
        df = df[df.iloc[:, 2] == selected]

    st.dataframe(df, use_container_width=True, hide_index=True)
