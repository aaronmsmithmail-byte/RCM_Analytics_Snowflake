# Healthcare RCM Analytics Dashboard

A comprehensive Streamlit in Snowflake (SiS) application for monitoring and analyzing Healthcare Revenue Cycle Management (RCM) KPIs and metrics, built on a Snowflake-native medallion data architecture with Cortex Analyst AI.

## Overview

This dashboard provides healthcare organizations with interactive visualizations across twelve analytical tabs plus eight metadata pages:

| Tab | Description |
|-----|-------------|
| **Executive Summary** | High-level KPI scorecard with configurable alert thresholds |
| **Collections & Revenue** | Revenue waterfall, collection rate trends, cost to collect |
| **Claims & Denials** | Denial reasons, clean claim rate, first-pass resolution, charge lag, scrubbing rule breakdown |
| **A/R Aging & Cash Flow** | Aging buckets, days in A/R trend, monthly cash flow |
| **Payer Analysis** | Revenue/volume/denial rate breakdown by payer with claim-level drill-down |
| **Department Performance** | Revenue, collection rate, and encounter volume by department with encounter-level drill-down |
| **Provider Performance** | Provider scorecard with collection rate, denial rate, and clean claim rate outlier detection |
| **CPT Code Analysis** | Revenue, denial rate, and charge concentration by procedure code |
| **Underpayment Analysis** | ERA allowed vs. paid variance — contractual recovery opportunity by payer |
| **Forecasting** | Linear trend projections for cash flow, DAR, and denial rate + interactive what-if scenario modelling |
| **Patient Responsibility** | Patient-owed portion (co-pay/deductible/coinsurance) by payer, department, and encounter type |
| **AI Assistant** | Natural-language chat interface — asks questions, queries the database live via tool calling, and explains results in plain language |

**Metadata pages** (sidebar navigation): Data Catalog · Data Lineage · Knowledge Graph · Semantic Layer · AI Architecture · Business Process · Data Validation · Feature Backlog

Every data tab includes **CSV and Excel export buttons**. A **KPI alert system** in the sidebar flags threshold breaches in real time, and a **data pipeline freshness panel** shows the last ETL run time and status for each of the 10 data domains.

The **AI Assistant** tab uses a two-stage Snowflake-native AI pipeline — **Cortex Analyst** generates and executes SQL queries from natural-language questions using the staged semantic model YAML, then **Cortex Complete** (mistral-large2) interprets the results with business-friendly explanations and actionable recommendations. No external API keys required.

---

## Requirements

- A Snowflake account (free trial works — [signup](https://signup.snowflake.com/))
- Python 3.9+ (for generating sample CSV data locally)
- A GitHub account with a Personal Access Token (for Snowflake Git integration)

---

## Setup

### Quick Start (Recommended)

The fastest way to deploy is using the **comprehensive setup script** and Snowflake's native Git integration:

1. **Generate a GitHub Personal Access Token:**
   - Go to GitHub > Settings > Developer settings > Personal access tokens > Fine-grained tokens
   - Create a token with **Contents: Read-only** permission on this repository

2. **Open `snowflake/setup/full_setup.sql` in Snowsight** (or copy from the repo)

3. **Replace the two placeholders** (your GitHub username and PAT)

4. **Run each numbered section in order** (9 sections, ~10 minutes total)

The setup script handles everything: database/schemas/warehouse creation, Git repo connection, DDL deployment, ETL pipeline, metadata seeding, Horizon catalog, and Cortex Analyst model staging.

### Manual Setup (Step by Step)

If you prefer to run each step manually:

#### 1. Clone the repository

```bash
git clone https://github.com/aaronmsmithmail-byte/RCM_Analytics_Snowflake.git
cd RCM_Analytics_Snowflake
```

#### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

#### 3. Generate sample data

```bash
python generate_sample_data.py
```

This creates 10 CSV files in the `data/` directory covering a rolling 2-year window ending today:

| File | Rows | Source System | Description |
|------|------|---------------|-------------|
| `payers.csv` | 10 | Payer Master | Commercial, government, and self-pay payers |
| `patients.csv` | 500 | EHR | Patient demographics and insurance info |
| `providers.csv` | 25 | EHR | Providers across 10 departments |
| `encounters.csv` | 3,000 | EHR | Patient encounters (outpatient, inpatient, ED, telehealth) |
| `charges.csv` | ~5,900 | EHR / Charge Capture | Charge records with CPT and ICD-10 codes |
| `claims.csv` | 2,800 | Clearinghouse | Claims with status and scrubbing fail reason |
| `payments.csv` | ~3,200 | Clearinghouse / ERA | Payments with allowed amount and accuracy flags |
| `denials.csv` | ~400 | Clearinghouse / ERA | Denial records with appeal tracking |
| `adjustments.csv` | 600 | Billing System | Contractual, writeoff, and charity adjustments |
| `operating_costs.csv` | ~25 | ERP / Finance | Monthly RCM operational costs |

Upload these CSVs to the Snowflake internal stage `@RCM_ANALYTICS.STAGING.RCM_STAGE` via Snowsight UI or SnowSQL, then run the ETL stored procedure to load them through the medallion pipeline (Bronze → Silver → Gold).

### 5. Deploy to Snowflake

```bash
# Run DDL scripts to create all Snowflake objects
snow sql -f snowflake/setup/00_environment.sql
snow sql -f snowflake/ddl/01_bronze_tables.sql
snow sql -f snowflake/ddl/02_silver_tables.sql
snow sql -f snowflake/ddl/03_gold_views.sql
snow sql -f snowflake/ddl/04_metadata_tables.sql
snow sql -f snowflake/ddl/05_stages.sql

# Upload CSVs to stage (via SnowSQL)
PUT file:///path/to/data/*.csv @RCM_ANALYTICS.STAGING.RCM_STAGE;

# Run ETL
snow sql -f snowflake/etl/load_stage_to_bronze.sql
snow sql -q "CALL RCM_ANALYTICS.STAGING.SP_BRONZE_TO_SILVER();"

# Seed metadata (KPI definitions, semantic layer, knowledge graph)
snow sql -f snowflake/etl/seed_metadata.sql

# Apply Horizon Data Catalog (tags, column comments, PII classification)
snow sql -f snowflake/catalog/tags_and_comments.sql

# Stage Cortex Analyst semantic model
PUT file:///path/to/snowflake/cortex/rcm_semantic_model.yaml @RCM_ANALYTICS.STAGING.RCM_STAGE/cortex/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
```

### 6. Deploy the Streamlit dashboard

Create the Streamlit app directly from the Git repo (run in a SQL worksheet):

```sql
USE ROLE SYSADMIN;
USE DATABASE RCM_ANALYTICS;
USE SCHEMA STAGING;

CREATE OR REPLACE STREAMLIT RCM_DASHBOARD
    ROOT_LOCATION = '@RCM_ANALYTICS.STAGING.RCM_REPO/branches/main/snowflake/streamlit'
    MAIN_FILE = 'rcm_dashboard.py'
    QUERY_WAREHOUSE = RCM_WH
    COMMENT = 'Healthcare RCM Analytics Dashboard';
```

Then open the app: **Projects > Streamlit > RCM_DASHBOARD**

The app reads code directly from the Git repo, so pushing changes to `main` and running `ALTER GIT REPOSITORY RCM_REPO FETCH;` automatically updates the dashboard.

---

## Data Architecture — Medallion Layers

All data flows through a three-layer medallion architecture in Snowflake (`RCM_ANALYTICS` database):

| Layer | Schema | Tables/Views | Description |
|-------|--------|-------------|-------------|
| **Bronze** | `BRONZE` | 10 tables (e.g. `CLAIMS`, `PAYMENTS`) | Raw VARCHAR ingestion from CSV via `COPY INTO` — no type casting, full audit trail via `_LOADED_AT` timestamp |
| **Silver** | `SILVER` | 10 tables (e.g. `CLAIMS`, `PAYMENTS`) | Cleaned, typed (FLOAT/INTEGER/VARCHAR), FK-constrained — source of truth for all KPI computation |
| **Gold** | `GOLD` | 5 views (e.g. `MONTHLY_KPIS`, `PAYER_PERFORMANCE`) | Pre-aggregated business views computed at query time from Silver |

A `METADATA.PIPELINE_RUNS` table records the last load time, row count, and source file for each domain. The dashboard reads this table to power the data freshness sidebar panel.

### Gold Views

| View | Description |
|------|-------------|
| `GOLD.MONTHLY_KPIS` | Monthly claim counts, charges, payments, CCR, denial rate, GCR |
| `GOLD.PAYER_PERFORMANCE` | Revenue, volume, and collection metrics per payer |
| `GOLD.DEPARTMENT_PERFORMANCE` | Revenue, encounter count, and revenue-per-encounter by department |
| `GOLD.AR_AGING` | Outstanding AR grouped into 0–30, 31–60, 61–90, 91–120, 120+ day buckets |
| `GOLD.DENIAL_ANALYSIS` | Denial volume, dollars denied, and recovery rate by reason code |

### Horizon Data Catalog

The `snowflake/catalog/tags_and_comments.sql` script populates the Snowflake Horizon Data Catalog so all tables, views, and columns are discoverable in Snowsight — accessible to everyone in the organization, not just Streamlit app users.

| Feature | Coverage |
|---------|----------|
| **Database & schema comments** | All 5 schemas described with their medallion layer purpose |
| **Table/view comments** | All 25 objects (10 Bronze + 10 Silver + 5 Gold) with source system and relationship info |
| **Column comments** | Every column in the Silver layer has a description (data type, business meaning, FK references) |
| **Classification tags** | `DATA_LAYER` (bronze/silver/gold), `DATA_DOMAIN` (claims/payments/…), `SENSITIVITY` (PII/PHI/public) |
| **PII identification** | Patient name, DOB, ZIP code, and member ID tagged as PII in both Bronze and Silver |

Browse the catalog in Snowsight: **Data > Databases > RCM_ANALYTICS** — click any table to see column descriptions, tags, and lineage.

### Filtering with FilterParams

All metric functions accept a `FilterParams` dataclass that applies four filter dimensions at the Silver layer via parameterized SQL:

```python
FilterParams(
    start_date="2024-01-01",   # inclusive
    end_date="2024-12-31",     # inclusive
    payer_id="PYR001",         # optional — None for all payers
    department="Cardiology",   # optional — None for all departments
    encounter_type="Inpatient" # optional — None for all encounter types
)
```

A shared `WITH filtered_claims AS (...)` CTE joins `silver_claims` to `silver_encounters` and applies all active filter conditions, ensuring consistent filtering across every metric.

---

## Project Structure

```
RCM_Analytics_Snowflake/
├── generate_sample_data.py          # Synthetic CSV data generation (10 files, ~12K rows)
├── requirements.txt                 # Python dependencies
├── Makefile                         # Dev commands: test, lint, verify, deploy
├── CLAUDE.md                        # AI assistant project guide
├── LICENSE                          # MIT license
├── .env.example                     # Environment variable template
├── snowflake.yml.example            # Snowflake CLI config template
├── .streamlit/
│   └── config.toml                  # Streamlit server and theme settings
├── .github/
│   └── workflows/
│       ├── ci.yml                   # CI pipeline (lint, test, security — 3 parallel jobs)
│       └── deploy-snowflake.yml     # CD pipeline (DDL → ETL → Cortex → Streamlit)
├── data/                            # Generated CSV files (not committed — run generate_sample_data.py)
├── snowflake/
│   ├── setup/
│   │   ├── 00_environment.sql       # Database, schemas, warehouse creation
│   │   └── full_setup.sql           # One-script deployment via Git integration (recommended)
│   ├── ddl/
│   │   ├── 01_bronze_tables.sql     # Bronze layer — raw VARCHAR tables
│   │   ├── 02_silver_tables.sql     # Silver layer — typed tables with FKs
│   │   ├── 03_gold_views.sql        # Gold layer — aggregated views
│   │   ├── 04_metadata_tables.sql   # KPI catalog, semantic layer, knowledge graph tables
│   │   └── 05_stages.sql            # Internal stage and file format
│   ├── etl/
│   │   ├── load_stage_to_bronze.sql           # SP_LOAD_STAGE_TO_BRONZE() stored procedure
│   │   ├── transform_bronze_to_silver.sql     # SP_BRONZE_TO_SILVER() stored procedure
│   │   ├── seed_metadata.sql                  # KPI definitions, semantic layer, knowledge graph
│   │   └── tasks.sql                          # DAILY_ETL_TASK (cron-scheduled ETL)
│   ├── cortex/
│   │   └── rcm_semantic_model.yaml  # Cortex Analyst semantic model (10 tables, metrics, joins)
│   ├── catalog/
│   │   └── tags_and_comments.sql    # Horizon Data Catalog (tags, comments, PII classification)
│   ├── diag/
│   │   └── check_data.sql           # Diagnostic queries for verifying data loads
│   ├── deploy.sql                   # Master deployment script (runs DDL + ETL in order)
│   └── streamlit/
│       ├── rcm_dashboard.py         # Main SiS app: 12 tabs + sidebar + metadata page router
│       ├── environment.yml          # Snowflake Anaconda channel dependencies for SiS
│       └── src/
│           ├── metrics.py           # 26 query_* KPI functions + FilterParams dataclass
│           ├── data_loader.py       # Snowpark session-based data loading (Bronze/Silver/Gold)
│           ├── cortex_chat.py       # Cortex Analyst + Cortex Complete AI chat UI
│           ├── metadata_pages.py    # 8 metadata pages (catalog, lineage, KG, semantic, AI, etc.)
│           └── validators.py        # 25 SQL-based data quality validators
└── tests/
    ├── test_generate_data.py        # CSV generation tests (file existence, row counts)
    └── test_snowflake_sql.py        # SQL/Python static analysis (no DuckDB patterns, correct Snowflake syntax)
```

---

## Configuration Reference

**When running inside Streamlit in Snowflake (SiS):** No configuration is needed — the Snowpark session is provided automatically, and the AI Assistant uses Snowflake Cortex functions (no external API keys required).

**For local development only:** Copy `.env.example` to `.env` and configure your Snowflake connection:

| Variable | Default | Required | Description |
|----------|---------|----------|-------------|
| `SNOWFLAKE_ACCOUNT` | — | Local dev only | Snowflake account identifier (e.g. `xy12345.us-east-1`) |
| `SNOWFLAKE_USER` | — | Local dev only | Snowflake username |
| `SNOWFLAKE_PASSWORD` | — | Local dev only | Snowflake password |
| `SNOWFLAKE_DATABASE` | `RCM_ANALYTICS` | No | Target database |
| `SNOWFLAKE_SCHEMA` | `SILVER` | No | Default schema |
| `SNOWFLAKE_WAREHOUSE` | `RCM_WH` | No | Compute warehouse |
| `SNOWFLAKE_ROLE` | `SYSADMIN` | No | Snowflake role |
| `RCM_DATA_DIR` | `./data/` | No | Directory where `generate_sample_data.py` writes CSV files |
| `STREAMLIT_SERVER_PORT` | `8501` | No | Port the Streamlit server listens on (local dev) |
| `STREAMLIT_SERVER_ADDRESS` | `localhost` | No | Bind address (local dev) |

> **Adding a new variable?**  Add it to `.env.example` with a comment explaining the default, expected values, and impact.  Then update this table.

---

## Metrics Reference

### Core KPIs (pre-computed for the alert system)

| # | Metric | Formula | Benchmark |
|---|--------|---------|-----------|
| 1 | Days in A/R (DAR) | AR Balance / Avg Daily Charges | ≤ 35 days |
| 2 | Net Collection Rate (NCR) | Payments / (Charges − Contractual Adj) | ≥ 95% |
| 3 | Gross Collection Rate (GCR) | Total Payments / Total Charges | ≥ 70% |
| 4 | Clean Claim Rate (CCR) | Clean Claims / Total Claims | ≥ 90% |
| 5 | Denial Rate | Denied Claims / Total Claims | ≤ 10% |
| 6 | First-Pass Resolution Rate | Paid on First Submission / Total Claims | ≥ 85% |
| 7 | Payment Accuracy Rate | Accurate Payments / Total Payments | ≥ 95% |
| 8 | Bad Debt Rate | Bad Debt Write-offs / Total Charges | ≤ 3% |

### Additional Metrics

| # | Metric | Description |
|---|--------|-------------|
| 9 | Denial Reasons Breakdown | Grouped by reason code with recovery rate |
| 10 | Charge Lag | Avg days from service date to charge post date |
| 11 | Cost to Collect | Total RCM Costs / Total Collections |
| 12 | A/R Aging Buckets | Outstanding AR by 0–30, 31–60, 61–90, 91–120, 120+ days |
| 13 | Appeal Success Rate | Won Appeals / Total Appealed |
| 14 | Avg Reimbursement per Encounter | Total Payments / Total Encounters |
| 15 | Payer Mix Analysis | Revenue and volume by payer |
| 16 | Denial Rate by Payer | Denials per payer / Total payer claims |
| 17 | Department Performance | Revenue, collection rate, encounters by department |
| 18 | Provider Performance | Collection rate, denial rate, CCR, avg payment per provider |
| 19 | CPT Code Analysis | Revenue, denial rate, avg charge per unit by procedure code |
| 20 | Underpayment Analysis | ERA allowed_amount vs. payment_amount variance by payer |
| 21 | Clean Claim Scrubbing Breakdown | Dirty claim root causes by fail reason code |
| 22 | Patient Financial Responsibility | Patient-owed portion (co-pay/deductible) by payer, dept, trend |
| 23 | Data Freshness | Last ETL load time, row count, and staleness per domain |

All metrics are implemented as parameterized SQL queries against the Silver layer (`query_*` functions in `snowflake/streamlit/src/metrics.py`).

---

## Dashboard Tabs

### Tab 1 — Executive Summary
A single-screen scorecard for leadership. Eight color-coded KPI cards (green/amber/red vs. benchmarks) cover DAR, NCR, CCR, Denial Rate, GCR, First-Pass Rate, Payment Accuracy, and Bad Debt Rate. Trend lines for DAR and NCR give a month-over-month view; a grouped volume bar chart shows monthly encounters and claims.

**Alert banner:** If any KPI breaches its configured threshold, a banner appears at the top of this tab listing each breach with current value vs. threshold.

### Tab 2 — Collections & Revenue
Revenue waterfall from gross charges → contractual adjustments → denials → net collections. Supporting charts: gross vs. net collection rate trends, cost-to-collect trend vs. the 5% target, average reimbursement per claim. Financial summary table with CSV/Excel export.

### Tab 3 — Claims & Denials
Claim status distribution, top denial reasons by volume and dollar amount, denial/CCR/first-pass trend lines, and charge lag histogram.

**Claim Scrubbing Rules Breakdown:** A dedicated section below the denial analysis surfaces *why* dirty claims fail. Each dirty claim is tagged with one of six clearinghouse edit codes:

| Code | Description | Resolution Guidance |
|------|-------------|---------------------|
| `MISSING_AUTH` | Missing Prior Authorization | Automate PA check at scheduling |
| `ELIGIBILITY_FAIL` | Eligibility Not Verified | Real-time eligibility check 24-48h before visit |
| `CODING_ERROR` | Invalid CPT/ICD-10 Combination | Add coding edit rules; schedule coder training |
| `DUPLICATE_SUBMISSION` | Duplicate Claim | Enable duplicate detection in clearinghouse |
| `TIMELY_FILING` | Outside Filing Window | Automate timely-filing deadline alerts |
| `MISSING_INFO` | Missing Required Information | Front-desk registration checklist with required fields |

Charts show dirty claim volume and charges-at-risk by fail reason. An expandable table includes resolution guidance and is exportable.

### Tab 4 — A/R Aging & Cash Flow
Aging bucket bar/pie charts, dual-axis A/R balance vs. Days in A/R trend, monthly cash flow (charges vs. payments + net cash flow line).

### Tab 5 — Payer Analysis
Revenue and claim volume by payer, ranked collection rate and denial rate bars, payer comparison table. **Payer drill-down:** claim count, charges, payments, denied claims for any selected payer + claim status pie, denial reasons bar, and claim-level table with export.

### Tab 6 — Department Performance
Charges vs. payments by department, collection rate ranking, encounter volume pie, avg payment per encounter, encounter type breakdown. **Department drill-down:** encounter count, claim count, charges, payments for any selected department + encounter type pie, claim status bar, and encounter/claim table with export.

### Tab 7 — Provider Performance
Individual provider scorecard with collection rate, denial rate, and clean claim rate. Outlier detection flags providers with denial rate > 15%. Charts: revenue by provider, denial rate ranking, CCR ranking, avg payment per encounter. **Provider drill-down:** peer comparison deltas (vs. average), claim status mix, and top denial reasons.

### Tab 8 — CPT Code Analysis
Revenue and denial patterns by procedure code. Top CPT codes by revenue (colored by denial rate), highest-denial CPT codes (min 10 charges threshold), revenue concentration pie for top 12 codes, avg charge per unit. Full CPT detail table with export.

### Tab 9 — Underpayment Analysis
Compares ERA `allowed_amount` (contracted rate) vs. `payment_amount` (actual remittance). Identifies systematic payer underpayments — a direct contractual recovery opportunity. Summary KPIs: total recovery opportunity, underpayment rate, underpaid claim count, avg shortfall per claim. Charts: recovery opportunity by payer, underpayment rate by payer, monthly trend, allowed/paid/underpaid stacked waterfall. Payer summary table with export.

### Tab 10 — Forecasting
**Trend projections:** Linear extrapolation (degree-1 polynomial) with ±1 std-dev confidence band projected 3 months forward for cash flow (monthly collections), Days in A/R, and denial rate. Each projection includes a pass/fail callout against its benchmark.

**Anomaly detection:** IQR-based outlier detection automatically identifies and excludes anomalous months (e.g., incomplete data, processing errors) from the forecast model. Anomalies are flagged with red ✕ markers on the chart and listed in a warning callout.

**Seasonality detection:** Measures seasonal strength (0–1) by comparing month-of-year variance to total variance. Reported in the Model Details expander alongside train/test R², MAE, and MAPE.

**What-If Scenario Modelling** — three interactive sliders:

| Scenario | Output |
|----------|--------|
| Reduce denial rate by X pp | Monthly/annual claim recovery ($) + claims recovered/month |
| Reduce DAR by X days | One-time cash acceleration ($) + new projected DAR |
| Improve clean claim rate by X pp | Monthly/annual rework savings ($) + implied denial rate drop |

A **Combined Annual Impact** summary row aggregates all three scenarios.

### Tab 11 — Patient Responsibility
Analyses the patient-owed portion of the revenue cycle — co-pays, deductibles, and coinsurance — derived from `allowed_amount − payment_amount` in ERA payment data (no additional data needed).

Summary KPIs: total patient responsibility, patient responsibility rate (% of allowed), avg per claim, self-pay exposure. Charts: total patient responsibility by payer (colored by % of allowed), by payer type with rate labels, monthly trend, by department and encounter type. Payer detail table with export.

### Tab 12 — AI Assistant

A conversational interface powered by a **two-stage Snowflake-native AI pipeline**:

1. **Cortex Analyst (text-to-SQL)** -- The user's question and conversation history are sent to the Cortex Analyst REST API along with a staged semantic model YAML (`@RCM_STAGE/cortex/rcm_semantic_model.yaml`). Cortex Analyst generates a SQL query and executes it against the Silver layer.
2. **Cortex Complete (interpretation)** -- The SQL results and original question are sent to `SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', ...)` which generates a business-friendly explanation with key findings and actionable recommendations.
3. The generated SQL is shown in a collapsible expander, and the results are displayed in a scrollable data table below the interpretation.

**No external API keys required** -- everything runs within Snowflake using Cortex AI functions.

### Metadata Pages (sidebar navigation)

- **Data Catalog** — Searchable reference of all 23 KPIs and 10 data tables with descriptions, formulas, and source columns
- **Data Lineage** — DAG diagram showing the full pipeline: CSV files → Bronze → Silver → Gold → Dashboard
- **Knowledge Graph** — Entity-relationship diagram of the Silver-layer data model
- **Semantic Layer** — Business concept → KPI → source table/column mapping for every metric
- **AI Architecture** — Process flow diagram showing the two-stage pipeline: Cortex Analyst (text-to-SQL via semantic model) and Cortex Complete (business interpretation of results)
- **Business Process** — Revenue cycle process map with decision points (clean claim?, payer decision?, appeal?) and live KPI annotations at each step, linking business processes to the dashboard tabs that measure them
- **Data Validation** — Results from 25 automated quality checks run against the Silver layer on every app startup (row counts, referential integrity, data quality)
- **Feature Backlog** — Submit, prioritize, and track feature requests — persisted in Snowflake `METADATA.FEATURE_BACKLOG` table

---

## Sidebar Features

### Filters
All visualizations respond in real time to:
- **Date Range** — Filter by date of service
- **Payer** — Filter to a specific payer or view all
- **Department** — Filter to a specific clinical department
- **Encounter Type** — Outpatient, Inpatient, Emergency, Telehealth

Filters are applied at the database level via parameterized SQL — the Silver layer is queried directly with each selection.

### Alert Thresholds
An **⚙️ Alert Thresholds** expander lets users configure custom KPI targets for their organization. Defaults match industry benchmarks. Thresholds persist for the browser session.

When any KPI breaches its threshold, the sidebar shows a red alert badge with the count of breaching KPIs, and the Executive Summary tab displays an alert banner with value vs. threshold for each one.

### Data Pipeline Freshness
A **🟢/🟡/🔴 Data Pipeline** panel shows the last ETL run time, row count, and staleness status for all 10 data domains. Status is computed against the expected refresh cadence per domain (e.g. claims: 4h, encounters: 2h, operating costs: 720h).

### Data Quality
On startup, `snowflake/streamlit/src/validators.py` runs 25 SQL-based validation checks against the Silver layer:

| Category | Checks | Level |
|----------|--------|-------|
| Row count (all 10 tables non-empty) | 10 | Error |
| Referential integrity (FK relationships) | 8 | Error |
| No negative monetary amounts | 3 | Error |
| Valid claim status values | 1 | Error |
| Boolean column integrity | 2 | Error |
| No null primary keys | 1 | Error |

Issues appear in a collapsible **Data Quality** panel and on the dedicated **Data Validation** metadata page.

---

## CI/CD

### CI — Continuous Integration

A GitHub Actions workflow (`.github/workflows/ci.yml`) runs automatically on **every push and pull request**. It contains three parallel jobs:

| Job | What it checks | Key tools |
|-----|---------------|-----------|
| **Lint** | Code style + formatting | `ruff check`, `ruff format --check` |
| **Test** | Full test suite + coverage | `pytest --cov`, coverage PR comment |
| **Security** | Static analysis + dependency vulnerabilities | `bandit`, `pip-audit` |

### CD — Continuous Deployment

A second workflow (`.github/workflows/deploy-snowflake.yml`) deploys to Snowflake automatically when changes to `snowflake/**` files are pushed to `main`. It can also be triggered manually via the GitHub Actions UI (`workflow_dispatch`).

The pipeline runs in 5 sequential phases:

```
Phase 1: Lint & Test          ← CI gate (must pass before any deployment)
    │
Phase 2: Deploy DDL           ← Schemas, tables, views, stages (01-05_*.sql)
    │
Phase 3: Deploy ETL           ← Stored procedures, tasks, metadata, Horizon catalog
    │
Phase 4: Deploy Cortex        ← Semantic model YAML → @RCM_STAGE/cortex/
         Deploy Streamlit      ← Git repo fetch + SiS app deploy (parallel)
    │
Phase 5: Verify Deployment    ← Smoke tests (table counts, metadata, procedures)
```

**Prerequisites** (one-time setup):

1. **GitHub Secrets** — Add `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD` in repo Settings → Secrets
2. **GitHub Environment** — Create a `snowflake-prod` environment in Settings → Environments (optionally add required reviewers for manual approval before deploy)
3. **CSV data** — Upload generated CSVs to `@RCM_STAGE` once before the first ETL run

**Manual trigger:** Actions tab → Deploy to Snowflake → Run workflow → select `main`

### Running CI locally

```bash
make ci        # runs lint + test + security (mirrors GitHub Actions)
make verify    # quick gate: lint + test only
make coverage  # test suite with coverage report
make security  # bandit + pip-audit
```

---

## Running Tests

```bash
pytest tests/ -v
```

**52 tests** across 2 test files:

- **`test_generate_data.py`** — Validates CSV generation: verifies all 10 CSV files exist with expected row counts after running `generate_sample_data.py`.
- **`test_snowflake_sql.py`** — Static analysis of all SQL and Python files: verifies no DuckDB patterns remain, correct Snowflake syntax (FLOAT not REAL, TRY_CAST, DATEDIFF), DDL structure (Bronze tables, Silver FKs, Gold views), ETL stored procedures, semantic model structure, Horizon catalog tags, dashboard imports, and metrics module conventions (FilterParams, Snowpark session, all 26 query functions).

---

## Contributing

See **[CONTRIBUTING.md](CONTRIBUTING.md)** for the full contributor guide, including:

- **Branching strategy** — Trunk-based development with `feature/`, `fix/`, `ddl/` branch prefixes
- **Commit convention** — Conventional Commits format (`feat:`, `fix:`, `ddl:`, `docs:`, etc.)
- **Code review standards** — What reviewers check, response expectations
- **SQL change management** — Process for DDL changes with backwards-compatibility rules and rollback plans
- **Security standards** — PII/PHI handling, credential management, dependency review

Additional governance documents:

| Document | Purpose |
|----------|---------|
| [`.github/pull_request_template.md`](.github/pull_request_template.md) | Standardized PR format with quality and DDL checklists |
| [`.github/CODEOWNERS`](.github/CODEOWNERS) | Automated review routing by file area |
| [`.claude/skills/standards.md`](.claude/skills/standards.md) | Coding conventions (Python, SQL, testing, security, CI/CD) |

---

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| streamlit | ≥ 1.30.0 | Web framework and UI (provided by SiS runtime) |
| pandas | ≥ 2.0.0 | Data manipulation and DataFrame results |
| plotly | ≥ 5.18.0 | Interactive visualizations |
| numpy | ≥ 1.24.0 | Numerical calculations and trend extrapolation |
| snowflake-connector-python | ≥ 3.6.0 | Snowflake database connectivity |
| snowflake-snowpark-python | ≥ 1.11.0 | Snowpark DataFrame API and session management |
| openpyxl | ≥ 3.1.0 | Excel export support |
| python-dotenv | ≥ 1.0.0 | Loads environment variables from `.env` (local dev only) |
| pytest | ≥ 7.0.0 | Unit testing (dev) |
| pytest-cov | ≥ 4.0.0 | Coverage measurement in CI and locally (dev) |
| ruff | ≥ 0.8.0 | Linting and formatting (dev) |
