---
name: standards
description: >
  Project standards and conventions for the Healthcare RCM Analytics codebase.
  Reference this document when reviewing code, creating new modules, or validating
  changes. Use when the user says "check standards", "validate conventions",
  "naming check", "does this follow our standards", or during any review phase.
---

# Project Standards & Conventions

All code changes must conform to these standards. Review agents should validate
changes against each applicable section.

---

## 1. Data Modeling Standards

### Table Naming (Medallion Architecture)
| Layer | Schema | Example | Purpose |
|-------|--------|---------|---------|
| Bronze | `BRONZE` | `BRONZE.CLAIMS` | Raw CSV ingestion, all VARCHAR columns |
| Silver | `SILVER` | `SILVER.CLAIMS` | Typed (FLOAT/INTEGER), FK-constrained, validated |
| Gold | `GOLD` | `GOLD.MONTHLY_KPIS` | SQL VIEWs — pre-aggregated KPIs |
| Metadata | `METADATA` | `METADATA.KPI_CATALOG` | KPI definitions, semantic layer, knowledge graph |
| Staging | `STAGING` | `STAGING.RCM_STAGE` | Internal stages, ETL procedures, tasks |

### Column Naming
| Pattern | Convention | Examples |
|---------|-----------|----------|
| Primary keys | `{entity}_id` | `claim_id`, `patient_id`, `payer_id` |
| Foreign keys | Same as PK it references | `payer_id` in `silver_claims` → `silver_payers.payer_id` |
| Dates | `{action}_date` or `date_of_{noun}` | `submission_date`, `date_of_service`, `payment_date` |
| Money | `{descriptor}_amount` | `charge_amount`, `payment_amount`, `denied_amount` |
| Booleans | `is_{property}` | `is_clean_claim`, `is_accurate_payment` |
| Percentages | `{property}_pct` or `{property}_rate` | `avg_reimbursement_pct` |
| Metadata | `_{name}` (underscore prefix) | `_loaded_at` |
| All columns | `snake_case` | Never camelCase or PascalCase |

### Type Conventions
| Layer | Column type | Snowflake type |
|-------|------------|----------------|
| Bronze | Everything | `VARCHAR` |
| Silver | IDs, codes, dates | `VARCHAR` |
| Silver | Money, percentages | `FLOAT` |
| Silver | Counts, booleans | `INTEGER` |
| Silver | Timestamps | `TIMESTAMP` |

### SQL Conventions
- All Snowflake SQL uses **uppercase identifiers** (e.g. `SILVER.CLAIMS`, not `silver_claims`)
- All metric queries use a shared `WITH filtered_claims AS (...)` CTE via `FilterParams` for consistent filtering
- Date formatting: `TO_CHAR(TRY_TO_DATE(col, 'YYYY-MM-DD'), 'YYYY-MM')` — always specify format
- Date arithmetic: `DATEDIFF('day', start, end)` — not `date_diff` or `julianday`
- Type casting: `TRY_CAST(col AS FLOAT)` — not `CAST AS REAL`
- Current date: `CURRENT_DATE()` — not `CURRENT_DATE`
- Boolean conversion from text: `CASE UPPER(TRIM(col)) WHEN 'TRUE' THEN 1 WHEN '1' THEN 1 WHEN 'YES' THEN 1 ELSE 0 END`
- Empty string → NULL: `NULLIF(TRIM(COALESCE(col, '')), '')`
- NULL PKs filtered: `WHERE pk IS NOT NULL AND pk != ''`

---

## 2. Python Naming Standards

### Modules
- Lowercase with underscores: `cube_client.py`, `data_loader.py`, `metadata_pages.py`
- One module per logical domain — don't overload a single file

### Functions
| Prefix | Purpose | Example |
|--------|---------|---------|
| `query_*` | KPI metric queries | `query_denial_rate(p)` |
| `get_*` | Data retrieval (non-metric) | `get_connection()`, `get_kg_nodes()` |
| `is_*` / `has_*` | Boolean checks | `is_cube_available()`, `has_medallion_schema()` |
| `build_*` | Construct complex objects | `build_filter_cte()`, `build_system_prompt()` |
| `load_*` | Load data from storage | `load_all_data()`, `load_csv_to_bronze()` |
| `render_*` | Streamlit page rendering | `render_knowledge_graph()` |
| `_private` | Module-internal helpers | `_cte()`, `_empty_trend()`, `_try_cube_query()` |
| `seed_*` | Initialize external stores | `seed_knowledge_graph()` |

### Classes
- PascalCase: `FilterParams`, `TestQueryDenialRate`
- Dataclasses for value objects: `@dataclass class FilterParams`
- Test classes: `Test{ComponentName}`

### Constants
- Module-level public: `SCREAMING_SNAKE_CASE` — `DB_PATH`, `CUBE_API_URL`, `TOOL_SCHEMA`
- Module-level private: `_SCREAMING_SNAKE_CASE` — `_MAX_ROWS`, `_HEALTH_TTL`, `_KG_NODES`

### Variables
- Local: `snake_case`
- Domain abbreviations OK: `df` (DataFrame), `conn` (connection), `p` (FilterParams), `sql`
- Loop variables: `i`, `row`, `t` (table), `n` (node)

---

## 3. Package & Import Standards

### Import Order (enforced by ruff isort)
```python
# 1. Standard library
import os
import time
from dataclasses import dataclass

# 2. Third-party packages
import numpy as np
import pandas as pd
import streamlit as st

# 3. Local modules (first-party = src)
from src.metrics import FilterParams, query_days_in_ar
from src.data_loader import load_silver_data
```

### Optional Dependencies
```python
try:
    import graphviz
    _HAS_GRAPHVIZ = True
except ImportError:
    _HAS_GRAPHVIZ = False
```

### Environment Variables
- Load `.env` at module level before constants:
  ```python
  try:
      from dotenv import load_dotenv
      load_dotenv()
  except ImportError:
      pass
  ```
- Read with defaults: `VAR = os.environ.get("VAR_NAME", "default_value")`
- Type-safe parsing:
  ```python
  try:
      _MAX_ROWS = max(10, int(os.environ.get("AI_MAX_ROWS", "100")))
  except ValueError:
      _MAX_ROWS = 100
  ```

---

## 4. Module Standards

### Snowpark Session Access
All data access goes through the Snowpark session (provided by SiS or created from env vars):

```python
from snowflake.snowpark.context import get_active_session

session = get_active_session()
df = session.sql("SELECT * FROM RCM_ANALYTICS.SILVER.CLAIMS").to_pandas()
```

### Caching
Use `@st.cache_data(ttl=300)` for data loading functions (5-minute TTL).
Use uncached queries for pages that perform writes (e.g. Feature Backlog).

### Graceful Degradation
When a Snowflake query or feature is unavailable (e.g. Cortex functions, metadata tables),
return empty results or show a user-friendly message — never crash the page.

---

## 5. Testing Standards

### File & Naming
- One test file per source module: `tests/test_{module}.py`
- Test classes: `Test{Component}` — group related tests
- Test methods: `test_{action}_{expectation}`
  - Good: `test_missing_csv_skips_gracefully`, `test_denial_rate_non_negative`
  - Bad: `test_1`, `test_function`, `test_it_works`

### Test Patterns
Tests use static analysis of SQL and Python files (no live Snowflake connection required):

```python
class TestBronzeTables:
    def test_all_bronze_tables(self):
        sql = Path("snowflake/ddl/01_bronze_tables.sql").read_text()
        for table in EXPECTED_BRONZE_TABLES:
            assert table in sql

    def test_loaded_at_column(self):
        sql = Path("snowflake/ddl/01_bronze_tables.sql").read_text()
        assert sql.count("_LOADED_AT") >= 10
```

### Required Coverage
- Every `query_*` function: minimum 2 tests (happy path + empty data)
- Every new public function: at least 1 test
- New SQL files: add static analysis tests verifying structure and conventions

### Assertions
- Use pytest `assert` (not unittest methods)
- Floats: `assert val == pytest.approx(expected, abs=0.1)`
- DataFrames: `assert df.empty`, `assert "column" in df.columns`
- Dicts: `assert "error" in result`, `assert result["key"] == expected`

---

## 6. Documentation Standards

### Docstrings (Google style)
```python
def query_denial_rate(p: FilterParams):
    """Calculate Claim Denial Rate.

    Denial Rate = (Denied + Appealed Claims) / Total Claims * 100

    Args:
        p: FilterParams with date range and optional filters.

    Returns:
        tuple: (denial_rate_percentage, trend_dataframe)
    """
```

### Section Headers
```python
# ── Section Name ──────────────────────────────────────────────
# ===========================================================================
# MAJOR SECTION TITLE
# ===========================================================================
```

### Comments
- Comment the **why**, not the **what**
- Good: `# TTL-cached to avoid hammering unavailable services`
- Bad: `# increment counter by 1`
- No commented-out code in production files

### .env.example
Every env var the code reads must appear in `.env.example` with:
- A comment explaining what it does
- The default value
- Valid value range or format

---

## 7. Linting Standards (ruff)

### Configuration: `ruff.toml`
- Target: Python 3.11, line-length 120
- Rules: E, W, F, I, UP, B, SIM, T20
- Run: `ruff check snowflake/streamlit/ tests/ generate_sample_data.py`
- Must pass with zero violations before every commit

### Key Rules
- **F401**: No unused imports (use `# noqa: F401` only when import is needed by monkeypatch/re-export)
- **I001**: Imports must be sorted (stdlib → third-party → local)
- **T201**: No `print()` in `src/` modules (use proper logging or return values)
- **B**: No mutable default arguments, no bare `except:`

---

## 8. Error Handling Standards

Two error handling patterns, depending on context:

| Context | On error, return | Example |
|---------|-----------------|---------|
| **Metadata queries** (`_query_meta`) | Empty `pd.DataFrame()` | Page shows empty table instead of crashing |
| **Cortex AI** (`cortex_chat.py`) | Fallback from REST to SQL, then error message | Chat shows user-friendly error |
| **Data loading** (`data_loader.py`) | Empty DataFrame with warning | Dashboard shows partial data |
| **ETL stored procedures** | Log error in PIPELINE_RUNS | Pipeline continues with next table |

Rules:
- External service calls (API, database, file I/O) must be wrapped in `try/except`
- Never crash a Streamlit page — always degrade gracefully
- Never silently swallow errors — log or return a meaningful empty result
- Client modules check `is_*_available()` before attempting connections

---

## 9. Security Standards

### Credential & Secret Management
- **Never** hardcode credentials, API keys, or account identifiers in code
- `.env` files: Listed in `.gitignore`, never committed
- Snowflake credentials: Stored in GitHub Secrets for CI/CD, or `snowflake.yml` (gitignored) for CLI
- Snowflake Git integration: Credentials stored as Snowflake SECRET objects

### SQL Injection Prevention
- Use `_esc()` helper for string literals in dynamic SQL
- Cortex Analyst generates SQL from natural language — users never execute SQL directly
- All metric queries use controlled `FilterParams` values, not raw user input

### PII / PHI Data Handling
- Patient name, DOB, ZIP code, and member ID are classified as **PII** in the Horizon Data Catalog
- Never log, print, or include PII values in error messages or debug output
- Never use real patient data in tests — use `generate_sample_data.py` for fully synthetic data
- All PII columns are tagged with `SENSITIVITY = 'PII'` via `tags_and_comments.sql`

### Runtime Safety
- Row limits: AI queries capped at `_MAX_ROWS` (default 100) to prevent context overflow
- Iteration limits: Tool-calling loop capped at `_MAX_ITERATIONS` (default 8) to prevent runaway loops
- Warehouse auto-suspend: 60 seconds to prevent idle compute costs

---

## 10. SQL Formatting Standards

### Keyword Casing
All SQL keywords UPPERCASE, all identifiers UPPERCASE:

```sql
-- Good
SELECT C.CLAIM_ID, C.TOTAL_CHARGE_AMOUNT
FROM RCM_ANALYTICS.SILVER.CLAIMS C
LEFT JOIN RCM_ANALYTICS.SILVER.ENCOUNTERS E
    ON C.ENCOUNTER_ID = E.ENCOUNTER_ID
WHERE C.DATE_OF_SERVICE BETWEEN '2024-01-01' AND '2024-12-31';

-- Bad
select c.claim_id from silver.claims c;
```

### CTE Naming
CTEs should use descriptive `snake_case` names prefixed by purpose:

```sql
WITH filtered_claims AS (
    -- Base filter applied to all downstream queries
    SELECT ...
),
monthly_totals AS (
    -- Aggregate filtered claims by month
    SELECT ...
)
SELECT * FROM monthly_totals;
```

### JOIN Formatting
- Always use explicit `JOIN` syntax (never comma-join)
- Put `ON` clause on a new indented line
- Qualify all column references with table aliases

```sql
SELECT C.CLAIM_ID, P.PAYMENT_AMOUNT
FROM SILVER.CLAIMS C
LEFT JOIN SILVER.PAYMENTS P
    ON C.CLAIM_ID = P.CLAIM_ID
LEFT JOIN SILVER.ENCOUNTERS E
    ON C.ENCOUNTER_ID = E.ENCOUNTER_ID;
```

### Stored Procedure Standards
- Name with `SP_` prefix: `SP_BRONZE_TO_SILVER`, `SP_LOAD_STAGE_TO_BRONZE`
- Include a header comment with purpose, inputs, outputs
- Use `EXECUTE AS CALLER` for transparency
- Log execution to `METADATA.PIPELINE_RUNS` with row counts
- Handle errors gracefully — log and continue, don't crash the pipeline

### Gold View Standards
- Gold views are **read-only aggregations** — never write to Gold
- Use `COALESCE()` for NULL safety in all aggregations
- Use `NULLIF(denominator, 0)` to prevent division-by-zero
- Include `TO_CHAR(TRY_TO_DATE(...), 'YYYY-MM')` for time-series grouping

---

## 11. Change Management Standards

### DDL Changes (Schema Modifications)
All DDL changes must follow this process:

1. **Branch** — Create a `ddl/` prefixed branch
2. **Modify DDL files** — Update the numbered DDL scripts in `snowflake/ddl/`
3. **Update ETL** — Modify stored procedures if column mappings change
4. **Update downstream** — Gold views, metrics.py, semantic model, catalog
5. **Test locally** — Run `make verify` to confirm no regressions
6. **PR with DDL checklist** — Use the DDL checklist in the PR template
7. **Document rollback** — Include how to undo the change

### Backwards Compatibility
- **Adding columns**: Always safe — existing queries are unaffected
- **Changing column types**: Use `TRY_CAST()` in migration; never change in-place
- **Dropping columns**: Verify no downstream dependencies first; remove in reverse order
- **Renaming columns**: Add new column, migrate data, update references, then drop old

### Semantic Model Changes
When modifying `rcm_semantic_model.yaml`:
- Test with Cortex Analyst before merging (ask sample questions)
- Re-stage the YAML to `@RCM_STAGE/cortex/` after deployment
- Verify existing sample questions still return correct results

---

## 12. CI/CD Standards

### Pipeline Structure
```
Push to any branch  →  CI (lint + test + security)
Push to main        →  CI → CD (DDL → ETL → Cortex → Streamlit)
```

### CI Gates (must all pass)
| Gate | Tool | Config |
|------|------|--------|
| Lint | `ruff check` | `ruff.toml` |
| Format | `ruff format --check` | `ruff.toml` |
| Tests | `pytest` | `tests/` |
| Security (static) | `bandit` | `bandit.toml` |
| Security (deps) | `pip-audit` | `requirements.txt` |

### CD Requirements
- Deployment to Snowflake requires the `snowflake-prod` environment approval
- DDL deploys before ETL, ETL before Cortex/Streamlit (dependency order)
- All deployment steps are idempotent (`CREATE OR REPLACE`, `IF NOT EXISTS`)
