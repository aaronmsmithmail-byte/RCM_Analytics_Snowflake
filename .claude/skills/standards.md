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
| Layer | Prefix | Example | Purpose |
|-------|--------|---------|---------|
| Bronze | `bronze_*` | `bronze_claims` | Raw CSV ingestion, all TEXT columns |
| Silver | `silver_*` | `silver_claims` | Typed, FK-constrained, validated |
| Gold | `gold_*` | `gold_monthly_kpis` | SQL VIEWs — pre-aggregated KPIs |
| Metadata | `meta_*` | `meta_kpi_catalog` | AI-queryable semantic/KG tables |
| Pipeline | `pipeline_*` | `pipeline_runs` | ETL tracking and data freshness |

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
| `query_*` | KPI metric queries | `query_denial_rate(p, db_path=None)` |
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
    from neo4j import GraphDatabase
    _HAS_NEO4J = True
except ImportError:
    _HAS_NEO4J = False
```

### Lazy Imports (when module may not be available)
```python
def _try_cube_query(...):
    try:
        from src.cube_client import query_cube, is_cube_available
        ...
    except Exception:
        return None
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

- SQL injection: Use `_esc()` helper for string literals in SQL; avoid f-strings with user input
- Cortex Analyst generates SQL from natural language — no direct user SQL execution
- API keys: Never hardcoded — always from env vars via `os.environ.get()`
- `.env` files: Listed in `.gitignore`, never committed
- Snowflake credentials: Stored in GitHub Secrets for CI/CD, or `snowflake.yml` (gitignored) for CLI
- Row limits: AI queries capped at `_MAX_ROWS` (default 100) to prevent context overflow
- Iteration limits: Tool-calling loop capped at `_MAX_ITERATIONS` (default 8) to prevent runaway loops
