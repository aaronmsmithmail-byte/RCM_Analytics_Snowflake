# CLAUDE.md — Project Guide for Healthcare RCM Analytics (Snowflake)

This file is automatically read by Claude Code at the start of every session.
Follow these rules whenever modifying this project.

---

## Architecture Overview

```
CSV files → Snowflake Internal Stage (@RCM_STAGE)
         → Bronze schema (raw VARCHAR tables via COPY INTO)
         → Silver schema (typed + FK tables via stored procedure)
         → Gold schema (aggregated views)
         → Streamlit in Snowflake (SiS) dashboard
         → Cortex Analyst (AI chatbot via semantic model YAML)
```

**Snowflake Objects:**
- Database: `RCM_ANALYTICS`
- Schemas: `BRONZE`, `SILVER`, `GOLD`, `METADATA`, `STAGING`
- Warehouse: `RCM_WH` (X-Small, auto-suspend 60s)

**Key files:**
| File | Responsibility |
|------|----------------|
| `snowflake/ddl/01-05_*.sql` | Snowflake schema definitions (Bronze, Silver, Gold, Metadata, Stages) |
| `snowflake/etl/transform_bronze_to_silver.sql` | SP_BRONZE_TO_SILVER() stored procedure |
| `snowflake/etl/tasks.sql` | DAILY_ETL_TASK (cron-scheduled ETL) |
| `snowflake/cortex/rcm_semantic_model.yaml` | Cortex Analyst semantic model (10 tables, measures, joins) |
| `snowflake/catalog/tags_and_comments.sql` | Horizon Data Catalog (tags, comments, PII classification) |
| `snowflake/streamlit/rcm_dashboard.py` | Main SiS app: 12 tabs + sidebar + metadata page router |
| `snowflake/streamlit/src/metrics.py` | All 26 `query_*` KPI functions + `FilterParams` dataclass |
| `snowflake/streamlit/src/data_loader.py` | Snowpark session-based data loading |
| `snowflake/streamlit/src/cortex_chat.py` | Cortex Analyst chat UI (replaces OpenRouter) |
| `snowflake/streamlit/src/metadata_pages.py` | Eight sidebar metadata pages |
| `snowflake/streamlit/src/validators.py` | SQL COUNT-based data quality assertions |
| `generate_sample_data.py` | Creates CSV files for upload to Snowflake stage |
| `snowflake/deploy.sql` | Master deployment script |

---

## Single Source of Truth

| Information | Single source | Do NOT also edit |
|-------------|---------------|-----------------|
| KPI definitions, formulas, benchmarks | `METADATA.KPI_CATALOG` table (seeded by `seed_metadata.sql`) | `README.md` (update count only) |
| Business concept → KPI → column mappings | Cortex semantic model YAML (`snowflake/cortex/`) → `METADATA.SEMANTIC_LAYER` table | — |
| Silver-layer entity descriptions | `METADATA.KG_NODES` table | `_KG_NODES` in `metadata_pages.py` (layout only) |
| Entity relationships / foreign keys | `METADATA.KG_EDGES` table | — |
| Knowledge graph node positions (x, y) | `_KG_NODES` list in `metadata_pages.py` | — (layout only) |
| Table catalog (Bronze / Silver / Gold) | `_TABLE_CATALOG` list in `metadata_pages.py` | — |
| Dashboard tab list and descriptions | `README.md` | — |

---

## Common Change Recipes

### Adding a New KPI

1. Add a row to `METADATA.KPI_CATALOG` in `snowflake/etl/seed_metadata.sql`
2. Add the matching row to `METADATA.SEMANTIC_LAYER` in `seed_metadata.sql`
3. Write a `query_*` function in `snowflake/streamlit/src/metrics.py`
4. Call it in the relevant tab in `snowflake/streamlit/rcm_dashboard.py`
5. Update `README.md`: increment the KPI count
6. Add facts/metrics/dimensions to `snowflake/cortex/rcm_semantic_model.yaml`

### Adding a New Silver-Layer Data Entity

1. Add the DDL to `snowflake/ddl/02_silver_tables.sql`
2. Add Bronze DDL to `snowflake/ddl/01_bronze_tables.sql`
3. Add ETL logic to `snowflake/etl/transform_bronze_to_silver.sql`
4. Add COPY INTO to `snowflake/etl/load_stage_to_bronze.sql`
5. Add CSV generator section to `generate_sample_data.py`
6. Insert rows into `METADATA.KG_NODES` and `METADATA.KG_EDGES` in `seed_metadata.sql`
7. Add position entry to `_KG_NODES` in `metadata_pages.py`
8. Add entries to `_TABLE_CATALOG` in `metadata_pages.py`
9. Add table definition to `snowflake/cortex/rcm_semantic_model.yaml`
10. Add tags/comments to `snowflake/catalog/tags_and_comments.sql`

### Modifying the AI Assistant

The AI Assistant uses a two-stage pipeline:
1. **Cortex Analyst** (text-to-SQL) reads `rcm_semantic_model.yaml` to generate SQL
2. **Cortex Complete** (`mistral-large2`) interprets results for business users

- Update `snowflake/cortex/rcm_semantic_model.yaml` to change what Cortex Analyst understands
  - Semantic model uses `dimensions`, `facts`, `metrics`, `time_dimensions` (NOT `columns` or `measures`)
  - Tables used in joins need `primary_key` and relationships need `join_type: left_outer`
- Update `snowflake/streamlit/src/cortex_chat.py` for chat UI or LLM interpretation changes
- The semantic model YAML must be re-staged to `@RCM_STAGE/cortex/` after changes:
  ```sql
  ALTER GIT REPOSITORY RCM_ANALYTICS.STAGING.RCM_REPO FETCH;
  COPY FILES INTO @RCM_ANALYTICS.STAGING.RCM_STAGE/cortex/
    FROM @RCM_ANALYTICS.STAGING.RCM_REPO/branches/main/snowflake/cortex/
    PATTERN = '.*rcm_semantic_model\.yaml';
  ```

### Adding a New Metadata Page

1. Add `render_*()` function to `snowflake/streamlit/src/metadata_pages.py`
2. Import it in `snowflake/streamlit/rcm_dashboard.py`
3. Add a sidebar button and routing in the dashboard

---

## Environment Setup

### Snowflake Deployment
```bash
# 1. Generate sample CSV data locally
python generate_sample_data.py

# 2. Run DDL scripts in Snowsight (or via Snowflake CLI)
snow sql -f snowflake/setup/00_environment.sql
snow sql -f snowflake/ddl/01_bronze_tables.sql
# ... (see snowflake/deploy.sql for full order)

# 3. Upload CSVs to stage
# Via Snowsight UI: Data > Add Data > Load files into a Stage
# Or via SnowSQL: PUT file:///path/to/data/*.csv @RCM_ANALYTICS.STAGING.RCM_STAGE;

# 4. Run ETL
snow sql -f snowflake/etl/load_stage_to_bronze.sql
CALL RCM_ANALYTICS.STAGING.SP_BRONZE_TO_SILVER();

# 5. Seed metadata
snow sql -f snowflake/etl/seed_metadata.sql

# 6. Stage Cortex model
PUT file:///path/to/snowflake/cortex/rcm_semantic_model.yaml @RCM_ANALYTICS.STAGING.RCM_STAGE/cortex/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

# 7. Deploy Streamlit app via Snowsight or Snowflake CLI
```

### Local Development
```bash
pip install -r requirements.txt
python generate_sample_data.py   # generates data/ CSVs
```

---

## Code Conventions

See `.claude/skills/standards.md` for the complete reference.

- All Snowflake SQL uses uppercase identifiers (SILVER.CLAIMS, not silver_claims)
- Date formatting: `TO_CHAR(TRY_TO_DATE(x, 'YYYY-MM-DD'), 'YYYY-MM')` (always specify format; not strftime)
- Date arithmetic: `DATEDIFF('day', start, end)` (not date_diff or julianday)
- Type casting: `TRY_CAST(x AS FLOAT)` (not CAST AS REAL)
- Current date: `CURRENT_DATE()` (not CURRENT_DATE)

---

## Development Workflow

```
1. PLAN → 2. APPROVE → 3. CODE → 4. VERIFY → 5. REVIEW → 6. DEPLOY
```

**Quick commands:**
```bash
make test      # run pytest
make lint      # run ruff
make verify    # test + lint (fast gate)
make deploy    # deploy DDL to Snowflake
```

---

## CI/CD

- GitHub Actions workflow: `.github/workflows/deploy-snowflake.yml`
- Deploys on push to `main` when `snowflake/**` files change
- Requires GitHub Secrets: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`
- Snowflake CLI config template: `snowflake.yml.example`

---

## Governance & Standards

| Area | Document | Scope |
|------|----------|-------|
| Coding conventions | `.claude/skills/standards.md` | Naming, SQL formatting, imports, testing, security, CI/CD |
| Development workflow | `.claude/skills/feature-workflow.md` | 6-stage workflow (Plan → Approve → Code → Verify → Review → Deploy) |
| Contributing guide | `CONTRIBUTING.md` | Branching strategy, commit conventions, review process, SQL change management |
| PR template | `.github/pull_request_template.md` | Standardized PR format with type-specific checklists |
| Code ownership | `.github/CODEOWNERS` | Automated review routing by file area |
| Review agents | `.claude/agents/` | 5 specialized agents (code-reviewer, simplifier, comment-analyzer, test-analyzer, silent-failure-hunter) |
| Data classification | `snowflake/catalog/tags_and_comments.sql` | Horizon Data Catalog: PII tags, data domain tags, column descriptions |
| Security scanning | `bandit.toml` + CI pipeline | Bandit (static analysis) + pip-audit (dependency vulnerabilities) |

### Commit Convention
Use **Conventional Commits**: `<type>: <summary>` where type is one of:
`feat`, `fix`, `refactor`, `docs`, `test`, `ci`, `ddl`, `style`, `chore`

### Branch Naming
`<type>/<short-description>` — e.g. `feature/add-provider-tab`, `ddl/add-referral-column`, `fix/denial-rate-calc`

### DDL Change Process
Schema changes require the DDL checklist in the PR template. See `CONTRIBUTING.md` for the full process (backwards compatibility rules, rollback plan, downstream update order).
