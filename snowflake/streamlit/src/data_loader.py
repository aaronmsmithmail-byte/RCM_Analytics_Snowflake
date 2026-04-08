"""
Data Loader for Healthcare RCM Analytics Dashboard (Snowflake Edition)
======================================================================

Streamlit-in-Snowflake (SiS) data loader.  This module queries Snowflake
Silver and Gold tables via the Snowpark session that SiS provides automatically.

Medallion Architecture Data Flow (Snowflake):
    Staged CSV Files
        ↓ (raw ingestion)
    Bronze tables  (RCM_ANALYTICS.BRONZE.*, all VARCHAR, _loaded_at timestamp)
        ↓ (ETL — type casting, validation)
    Silver tables  (RCM_ANALYTICS.SILVER.*, typed + FK-constrained)  ← this module reads here
        ↓ (aggregation views)
    Gold views     (RCM_ANALYTICS.GOLD.*, pre-joined SQL aggregates)  ← load_gold_data() reads here
        ↓
    DataFrames  →  Metrics Engine  →  Dashboard

Usage:
    from src.data_loader import load_all_data, load_gold_data
    data = load_all_data()      # Silver layer → dict of DataFrames
    gold = load_gold_data()     # Gold layer  → dict of pre-aggregated DataFrames
"""

import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

# ---------------------------------------------------------------------------
# Snowpark session helper
# ---------------------------------------------------------------------------


def _get_session():
    """Return the active Snowpark session provided by Streamlit in Snowflake."""
    return get_active_session()


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _parse_dates(df, date_columns):
    """
    Convert string date columns to pandas Timestamp objects.

    Snowflake may return DATE columns as date objects or strings depending on
    the connector.  We normalise them to pandas Timestamps so that:
    - Date range filtering works with comparison operators (>=, <=).
    - We can extract year/month for trend analysis (dt.to_period("M")).
    - Plotly can render proper time-series axes.

    Args:
        df:            The DataFrame to modify (modified in-place for efficiency).
        date_columns:  List of column names that contain date strings.

    Returns:
        The same DataFrame with date columns converted.

    Notes:
        - errors="coerce" converts unparseable values to NaT (Not a Time)
          instead of raising an error. This handles empty/null date fields
          gracefully (e.g., appeal_date is empty for non-appealed denials).
    """
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def _parse_booleans(df, bool_columns):
    """
    Convert integer/boolean columns to Python booleans.

    Snowflake BOOLEAN columns may arrive as True/False, 1/0, or strings
    depending on the connector version.  We normalise to Python bool so
    that .sum() counts True values correctly.

    Args:
        df:            The DataFrame to modify.
        bool_columns:  List of column names that contain boolean values.

    Returns:
        The same DataFrame with boolean columns converted.
    """
    for col in bool_columns:
        if col in df.columns:
            df[col] = df[col].astype(bool)
    return df


# ---------------------------------------------------------------------------
# Required columns for validation
# ---------------------------------------------------------------------------

REQUIRED_COLUMNS = {
    "payers": ["payer_id", "payer_name", "payer_type"],
    "patients": ["patient_id", "primary_payer_id"],
    "providers": ["provider_id", "department"],
    "encounters": ["encounter_id", "patient_id", "provider_id", "date_of_service", "department", "encounter_type"],
    "charges": ["charge_id", "encounter_id", "charge_amount", "service_date", "post_date"],
    "claims": [
        "claim_id",
        "encounter_id",
        "patient_id",
        "payer_id",
        "date_of_service",
        "submission_date",
        "total_charge_amount",
        "claim_status",
        "is_clean_claim",
    ],
    "payments": ["payment_id", "claim_id", "payer_id", "payment_amount", "is_accurate_payment"],
    "denials": [
        "denial_id",
        "claim_id",
        "denial_reason_code",
        "denial_reason_description",
        "denied_amount",
        "appeal_status",
        "recovered_amount",
    ],
    "adjustments": ["adjustment_id", "claim_id", "adjustment_type_code", "adjustment_amount"],
    "operating_costs": ["period", "total_rcm_cost"],
}


def _validate_columns(df, key, table_ref):
    """Raise ValueError if any required columns are missing from the DataFrame."""
    required = REQUIRED_COLUMNS.get(key, [])
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Data source '{table_ref}' is missing required columns: {missing}")


def _normalise_columns(df):
    """Lowercase all column names so the rest of the app sees snake_case keys.

    Snowflake returns UPPER-CASE column names by default.  The metrics engine
    and dashboard code expect lower-case names (e.g. ``claim_id``), so we
    normalise here at the boundary.
    """
    df.columns = [c.lower() for c in df.columns]
    return df


# ---------------------------------------------------------------------------
# Schema-qualified table references
# ---------------------------------------------------------------------------

_SILVER_SCHEMA = "RCM_ANALYTICS.SILVER"
_GOLD_SCHEMA = "RCM_ANALYTICS.GOLD"


# ---------------------------------------------------------------------------
# Public loaders
# ---------------------------------------------------------------------------


@st.cache_data(ttl=300)
def load_all_data():
    """
    Load all 10 Silver-layer tables from Snowflake into a dict of DataFrames.

    This is the primary function called by the Streamlit dashboard at startup.
    It reads from the Silver tables (cleaned, typed, FK-validated data),
    parses dates and booleans, and returns everything in a single dict.

    Results are cached for 5 minutes via ``@st.cache_data(ttl=300)``.

    Returns:
        dict: Keys are logical table names (without the silver_ prefix).
            {
                "payers":          DataFrame,
                "patients":        DataFrame,
                "providers":       DataFrame,
                "encounters":      DataFrame,
                "charges":         DataFrame,
                "claims":          DataFrame,
                "payments":        DataFrame,
                "denials":         DataFrame,
                "adjustments":     DataFrame,
                "operating_costs": DataFrame,
            }
    """
    session = _get_session()

    # ------------------------------------------------------------------
    # Table configuration: logical name → Snowflake table + parse hints.
    # ------------------------------------------------------------------
    table_config = {
        "payers": {
            "table": f"{_SILVER_SCHEMA}.PAYERS",
            "date_cols": [],
            "bool_cols": [],
        },
        "patients": {
            "table": f"{_SILVER_SCHEMA}.PATIENTS",
            "date_cols": ["date_of_birth"],
            "bool_cols": [],
        },
        "providers": {
            "table": f"{_SILVER_SCHEMA}.PROVIDERS",
            "date_cols": [],
            "bool_cols": [],
        },
        "encounters": {
            "table": f"{_SILVER_SCHEMA}.ENCOUNTERS",
            "date_cols": ["date_of_service", "discharge_date"],
            "bool_cols": [],
        },
        "charges": {
            "table": f"{_SILVER_SCHEMA}.CHARGES",
            "date_cols": ["service_date", "post_date"],
            "bool_cols": [],
        },
        "claims": {
            "table": f"{_SILVER_SCHEMA}.CLAIMS",
            "date_cols": ["date_of_service", "submission_date"],
            "bool_cols": ["is_clean_claim"],
        },
        "payments": {
            "table": f"{_SILVER_SCHEMA}.PAYMENTS",
            "date_cols": ["payment_date"],
            "bool_cols": ["is_accurate_payment"],
        },
        "denials": {
            "table": f"{_SILVER_SCHEMA}.DENIALS",
            "date_cols": ["denial_date", "appeal_date"],
            "bool_cols": [],
        },
        "adjustments": {
            "table": f"{_SILVER_SCHEMA}.ADJUSTMENTS",
            "date_cols": ["adjustment_date"],
            "bool_cols": [],
        },
        "operating_costs": {
            "table": f"{_SILVER_SCHEMA}.OPERATING_COSTS",
            "date_cols": [],  # 'period' handled specially below
            "bool_cols": [],
        },
    }

    # ------------------------------------------------------------------
    # Load each Silver table into a DataFrame
    # ------------------------------------------------------------------
    data = {}
    for table_name, config in table_config.items():
        table_ref = config["table"]
        df = session.sql(f"SELECT * FROM {table_ref}").to_pandas()  # noqa: S608
        _normalise_columns(df)
        _parse_dates(df, config["date_cols"])
        _parse_booleans(df, config["bool_cols"])

        # 'period' in operating_costs is "YYYY-MM"; parse to datetime
        if table_name == "operating_costs" and "period" in df.columns:
            df["period"] = pd.to_datetime(df["period"], format="%Y-%m")

        _validate_columns(df, table_name, table_ref)
        data[table_name] = df

    return data


@st.cache_data(ttl=300)
def load_gold_data():
    """
    Load all five Gold-layer views from Snowflake into a dict of DataFrames.

    Gold views are pre-aggregated and business-ready.  They are useful for
    displaying summary KPIs directly from SQL without applying pandas filters.

    Results are cached for 5 minutes via ``@st.cache_data(ttl=300)``.

    Returns:
        dict with keys:
            "monthly_kpis"           — monthly claim counts, collection rates, etc.
            "payer_performance"      — per-payer denial rates and collection rates
            "department_performance" — per-department encounter counts and revenue
            "ar_aging"               — outstanding balance by aging bucket
            "denial_analysis"        — denial counts and appeal success by reason code
    """
    session = _get_session()

    gold_tables = {
        "monthly_kpis": f"{_GOLD_SCHEMA}.MONTHLY_KPIS",
        "payer_performance": f"{_GOLD_SCHEMA}.PAYER_PERFORMANCE",
        "department_performance": f"{_GOLD_SCHEMA}.DEPARTMENT_PERFORMANCE",
        "ar_aging": f"{_GOLD_SCHEMA}.AR_AGING",
        "denial_analysis": f"{_GOLD_SCHEMA}.DENIAL_ANALYSIS",
    }

    result = {}
    for key, table_ref in gold_tables.items():
        df = session.sql(f"SELECT * FROM {table_ref}").to_pandas()  # noqa: S608
        _normalise_columns(df)
        result[key] = df

    return result
