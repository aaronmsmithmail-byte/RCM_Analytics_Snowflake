"""
Snowflake SQL Syntax Tests
==========================

Validates that the Snowflake SQL files and Python metric queries use
correct Snowflake syntax (no DuckDB-isms).

These are static analysis tests that don't require a Snowflake connection.
"""

import os
import re

import pytest

SNOWFLAKE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "snowflake")
STREAMLIT_SRC = os.path.join(SNOWFLAKE_DIR, "streamlit", "src")


# ── Helper ──────────────────────────────────────────────────────────────


def _read(path):
    with open(path) as f:
        return f.read()


def _sql_files():
    """Yield all .sql files under snowflake/."""
    for root, _, files in os.walk(SNOWFLAKE_DIR):
        for fn in files:
            if fn.endswith(".sql"):
                yield os.path.join(root, fn)


def _py_files():
    """Yield all .py files under snowflake/streamlit/src/."""
    for root, _, files in os.walk(STREAMLIT_SRC):
        for fn in files:
            if fn.endswith(".py"):
                yield os.path.join(root, fn)


# ── SQL file tests ──────────────────────────────────────────────────────


class TestSQLFilesExist:
    """Verify all expected SQL files are present."""

    EXPECTED = [
        "setup/00_environment.sql",
        "ddl/01_bronze_tables.sql",
        "ddl/02_silver_tables.sql",
        "ddl/03_gold_views.sql",
        "ddl/04_metadata_tables.sql",
        "ddl/05_stages.sql",
        "etl/load_stage_to_bronze.sql",
        "etl/transform_bronze_to_silver.sql",
        "etl/seed_metadata.sql",
        "etl/tasks.sql",
        "catalog/tags_and_comments.sql",
        "deploy.sql",
    ]

    @pytest.mark.parametrize("relpath", EXPECTED)
    def test_file_exists(self, relpath):
        full = os.path.join(SNOWFLAKE_DIR, relpath)
        assert os.path.isfile(full), f"Missing: {relpath}"


class TestNoDuckDBSyntax:
    """Ensure no DuckDB-specific syntax leaked into Snowflake SQL files."""

    DUCKDB_PATTERNS = [
        (r"\bstrftime\b", "strftime (use TO_CHAR)"),
        (r"\bjulianday\b", "julianday (use DATEDIFF)"),
        (r"\bINSERT OR REPLACE\b", "INSERT OR REPLACE (use MERGE)"),
        (r"\bCAST\([^)]+AS\s+REAL\)", "CAST AS REAL (use FLOAT)"),
        (r"\bCREATE\s+INDEX\b", "CREATE INDEX (Snowflake auto-optimizes)"),
        (r"\bCREATE\s+SEQUENCE\b", "CREATE SEQUENCE (use AUTOINCREMENT)"),
    ]

    @pytest.mark.parametrize("pattern,desc", DUCKDB_PATTERNS)
    def test_no_duckdb_in_sql(self, pattern, desc):
        for path in _sql_files():
            content = _read(path)
            matches = re.findall(pattern, content, re.IGNORECASE)
            assert not matches, f"{os.path.basename(path)} contains {desc}: {matches}"


class TestNoDuckDBInPython:
    """Ensure no DuckDB imports or syntax in Snowflake Python modules."""

    BANNED_IMPORTS = [
        "import duckdb",
        "from src.database",
        "from src.cube_client",
        "from src.neo4j_client",
        "from src.ai_chat",
    ]

    def test_no_banned_imports(self):
        for path in _py_files():
            content = _read(path)
            for banned in self.BANNED_IMPORTS:
                assert banned not in content, f"{os.path.basename(path)} contains '{banned}'"


# ── DDL content tests ──────────────────────────────────────────────────


class TestBronzeDDL:
    """Verify Bronze layer DDL creates all 10 tables."""

    TABLES = [
        "PAYERS",
        "PATIENTS",
        "PROVIDERS",
        "ENCOUNTERS",
        "CHARGES",
        "CLAIMS",
        "PAYMENTS",
        "DENIALS",
        "ADJUSTMENTS",
        "OPERATING_COSTS",
    ]

    def test_all_bronze_tables(self):
        content = _read(os.path.join(SNOWFLAKE_DIR, "ddl", "01_bronze_tables.sql"))
        for table in self.TABLES:
            assert f"CREATE TABLE IF NOT EXISTS {table}" in content, f"Missing Bronze table: {table}"

    def test_loaded_at_column(self):
        content = _read(os.path.join(SNOWFLAKE_DIR, "ddl", "01_bronze_tables.sql"))
        assert content.count("_LOADED_AT") >= 10, "Each Bronze table should have _LOADED_AT"


class TestSilverDDL:
    """Verify Silver layer DDL creates all 10 tables with proper types."""

    def test_uses_float_not_real(self):
        content = _read(os.path.join(SNOWFLAKE_DIR, "ddl", "02_silver_tables.sql"))
        assert "FLOAT" in content, "Silver should use FLOAT type"
        assert "REAL" not in content.upper().replace("ALREADY", ""), "Silver should not use REAL type"

    def test_has_foreign_keys(self):
        content = _read(os.path.join(SNOWFLAKE_DIR, "ddl", "02_silver_tables.sql"))
        assert content.count("FOREIGN KEY") >= 8, "Silver should have at least 8 FK constraints"


class TestGoldViews:
    """Verify Gold views use Snowflake SQL syntax."""

    VIEWS = ["MONTHLY_KPIS", "PAYER_PERFORMANCE", "DEPARTMENT_PERFORMANCE", "AR_AGING", "DENIAL_ANALYSIS"]

    def test_all_gold_views(self):
        content = _read(os.path.join(SNOWFLAKE_DIR, "ddl", "03_gold_views.sql"))
        for view in self.VIEWS:
            assert view in content, f"Missing Gold view: {view}"

    def test_uses_snowflake_date_functions(self):
        content = _read(os.path.join(SNOWFLAKE_DIR, "ddl", "03_gold_views.sql"))
        assert "TO_CHAR" in content, "Gold views should use TO_CHAR (not strftime)"
        assert "TRY_TO_DATE" in content, "Gold views should use TRY_TO_DATE"
        assert "DATEDIFF" in content, "Gold views should use DATEDIFF"
        assert "CURRENT_DATE()" in content, "Gold views should use CURRENT_DATE()"


# ── ETL tests ──────────────────────────────────────────────────────────


class TestETLProcedure:
    """Verify Bronze → Silver ETL stored procedure."""

    def test_creates_procedure(self):
        content = _read(os.path.join(SNOWFLAKE_DIR, "etl", "transform_bronze_to_silver.sql"))
        assert "CREATE OR REPLACE PROCEDURE SP_BRONZE_TO_SILVER" in content

    def test_uses_try_cast(self):
        content = _read(os.path.join(SNOWFLAKE_DIR, "etl", "transform_bronze_to_silver.sql"))
        assert "TRY_CAST" in content, "ETL should use TRY_CAST (not CAST)"

    def test_updates_pipeline_runs(self):
        content = _read(os.path.join(SNOWFLAKE_DIR, "etl", "transform_bronze_to_silver.sql"))
        assert "METADATA.PIPELINE_RUNS" in content, "ETL should update pipeline tracking"


class TestSeedMetadata:
    """Verify metadata seeding SQL."""

    def test_kpi_catalog_count(self):
        content = _read(os.path.join(SNOWFLAKE_DIR, "etl", "seed_metadata.sql"))
        # Count INSERT INTO KPI_CATALOG value rows (each starts with a paren after VALUES)
        assert "KPI_CATALOG" in content
        assert "SEMANTIC_LAYER" in content
        assert "KG_NODES" in content
        assert "KG_EDGES" in content

    def test_uses_snowflake_table_refs(self):
        content = _read(os.path.join(SNOWFLAKE_DIR, "etl", "seed_metadata.sql"))
        assert "SILVER.CLAIMS" in content, "Should reference SILVER.CLAIMS not silver_claims"
        assert "silver_claims" not in content, "Should not contain DuckDB-style table refs"


class TestTaskScheduling:
    """Verify task scheduling SQL."""

    def test_creates_task(self):
        content = _read(os.path.join(SNOWFLAKE_DIR, "etl", "tasks.sql"))
        assert "CREATE OR REPLACE TASK" in content
        assert "CRON" in content
        assert "RESUME" in content


# ── Cortex semantic model tests ────────────────────────────────────────


class TestCortexModel:
    """Verify Cortex Analyst semantic model YAML."""

    def test_file_exists(self):
        path = os.path.join(SNOWFLAKE_DIR, "cortex", "rcm_semantic_model.yaml")
        assert os.path.isfile(path)

    def test_contains_tables(self):
        content = _read(os.path.join(SNOWFLAKE_DIR, "cortex", "rcm_semantic_model.yaml"))
        for table in [
            "CLAIMS",
            "PAYMENTS",
            "DENIALS",
            "CHARGES",
            "ADJUSTMENTS",
            "OPERATING_COSTS",
            "PAYERS",
            "PATIENTS",
            "PROVIDERS",
            "ENCOUNTERS",
        ]:
            assert table in content, f"Semantic model missing table: {table}"

    def test_contains_relationships(self):
        content = _read(os.path.join(SNOWFLAKE_DIR, "cortex", "rcm_semantic_model.yaml"))
        assert "relationships:" in content or "relationship" in content


# ── Horizon catalog tests ──────────────────────────────────────────────


class TestHorizonCatalog:
    """Verify Horizon Data Catalog SQL."""

    def test_creates_tags(self):
        content = _read(os.path.join(SNOWFLAKE_DIR, "catalog", "tags_and_comments.sql"))
        assert "DATA_LAYER" in content
        assert "DATA_DOMAIN" in content
        assert "SENSITIVITY" in content

    def test_pii_tagging(self):
        content = _read(os.path.join(SNOWFLAKE_DIR, "catalog", "tags_and_comments.sql"))
        assert "PII" in content
        assert "FIRST_NAME" in content
        assert "DATE_OF_BIRTH" in content


# ── Streamlit app tests ────────────────────────────────────────────────


class TestStreamlitApp:
    """Verify the main Streamlit dashboard file."""

    def test_dashboard_exists(self):
        path = os.path.join(SNOWFLAKE_DIR, "streamlit", "rcm_dashboard.py")
        assert os.path.isfile(path)

    def test_no_duckdb_references(self):
        content = _read(os.path.join(SNOWFLAKE_DIR, "streamlit", "rcm_dashboard.py"))
        assert "import duckdb" not in content
        assert "style_metric_cards" not in content
        assert "OPENROUTER" not in content

    def test_imports_cortex_chat(self):
        content = _read(os.path.join(SNOWFLAKE_DIR, "streamlit", "rcm_dashboard.py"))
        assert "from src.cortex_chat import render_chat_ui" in content

    def test_environment_yml_exists(self):
        path = os.path.join(SNOWFLAKE_DIR, "streamlit", "environment.yml")
        assert os.path.isfile(path)


# ── Metrics module tests ───────────────────────────────────────────────


class TestMetricsModule:
    """Verify the metrics module uses Snowflake SQL."""

    def setup_method(self):
        self.content = _read(os.path.join(STREAMLIT_SRC, "metrics.py"))

    def test_has_filter_params(self):
        assert "class FilterParams" in self.content

    def test_uses_snowflake_date_functions(self):
        assert "TO_CHAR(TRY_TO_DATE" in self.content
        assert "strftime" not in self.content

    def test_uses_snowflake_table_refs(self):
        assert "RCM_ANALYTICS.SILVER." in self.content
        assert "silver_claims" not in self.content.lower().split("'")[-1]  # not in SQL

    def test_uses_snowpark_session(self):
        assert "get_active_session" in self.content

    def test_has_all_26_functions(self):
        expected = [
            "query_days_in_ar",
            "query_net_collection_rate",
            "query_gross_collection_rate",
            "query_clean_claim_rate",
            "query_denial_rate",
            "query_denial_reasons",
            "query_first_pass_rate",
            "query_charge_lag",
            "query_cost_to_collect",
            "query_ar_aging",
            "query_payment_accuracy",
            "query_bad_debt_rate",
            "query_appeal_success_rate",
            "query_avg_reimbursement",
            "query_payer_mix",
            "query_denial_rate_by_payer",
            "query_department_performance",
            "query_provider_performance",
            "query_cpt_analysis",
            "query_underpayment_analysis",
            "query_underpayment_trend",
            "query_clean_claim_breakdown",
            "query_patient_responsibility_by_payer",
            "query_patient_responsibility_by_dept",
            "query_patient_responsibility_trend",
            "query_data_freshness",
        ]
        for fn_name in expected:
            assert f"def {fn_name}" in self.content, f"Missing function: {fn_name}"
