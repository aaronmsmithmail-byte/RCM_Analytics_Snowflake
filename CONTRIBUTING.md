# Contributing to RCM Analytics

This guide covers the development standards, branching strategy, and review process for contributing to this project. All contributors should read this before submitting changes.

---

## Quick Start

```bash
# 1. Clone and install
git clone https://github.com/aaronmsmithmail-byte/RCM_Analytics_Snowflake.git
cd RCM_Analytics_Snowflake
pip install -r requirements.txt

# 2. Generate sample data
python generate_sample_data.py

# 3. Run quality gates
make verify          # lint + tests (must pass before every commit)
```

---

## Branching Strategy

We use **trunk-based development** with short-lived feature branches:

```
main (protected)
  ├── feature/add-provider-scorecard
  ├── fix/denial-rate-calculation
  ├── refactor/metrics-cte-pattern
  └── docs/update-readme-kpi-count
```

### Branch Naming Convention

```
<type>/<short-description>
```

| Type | When to use | Example |
|------|-------------|---------|
| `feature/` | New functionality | `feature/add-underpayment-tab` |
| `fix/` | Bug fixes | `fix/ar-aging-date-range` |
| `refactor/` | Code restructuring (no behavior change) | `refactor/extract-filter-cte` |
| `docs/` | Documentation-only changes | `docs/update-setup-instructions` |
| `ddl/` | Schema changes (Bronze/Silver/Gold DDL) | `ddl/add-referral-source-column` |
| `etl/` | Pipeline logic changes | `etl/incremental-claims-load` |
| `ci/` | CI/CD and DevOps changes | `ci/add-sql-validation-step` |

### Rules

- **Never push directly to `main`** — all changes go through pull requests
- **Keep branches short-lived** — merge within 1-3 days; no long-running branches
- **One concern per branch** — don't mix features with refactoring
- **Delete branches after merge** — keep the repo clean

---

## Commit Message Convention

We use **Conventional Commits** for clear, parseable history:

```
<type>: <short summary in imperative mood>

<optional body — explain WHY, not WHAT>

<optional footer — breaking changes, issue refs>
```

### Types

| Type | Description | Example |
|------|-------------|---------|
| `feat` | New feature | `feat: add provider performance tab with outlier detection` |
| `fix` | Bug fix | `fix: correct denial rate calculation for partial months` |
| `refactor` | Code change that doesn't fix a bug or add a feature | `refactor: extract shared CTE builder into metrics.py` |
| `docs` | Documentation only | `docs: update README test count to 52` |
| `test` | Adding or updating tests | `test: add Silver FK constraint validation tests` |
| `ci` | CI/CD changes | `ci: add SQL convention checks to CI pipeline` |
| `ddl` | Schema/DDL changes | `ddl: add REFERRAL_SOURCE column to SILVER.ENCOUNTERS` |
| `style` | Formatting, whitespace (no logic change) | `style: fix ruff formatting violations` |
| `chore` | Maintenance tasks | `chore: update ruff to 0.9.0` |

### Rules

- **Imperative mood** — "add feature" not "added feature" or "adding feature"
- **No period at the end** of the summary line
- **72 character limit** on the summary line
- **Body explains WHY** — the diff shows what changed; the message explains why

### Examples

```
feat: add patient responsibility tab with payer breakdown

Adds Tab 11 showing co-pay, deductible, and coinsurance analysis
derived from ERA allowed_amount vs payment_amount. No additional
data sources required — uses existing SILVER.PAYMENTS table.

Closes #42
```

```
ddl: add REFERRAL_SOURCE to SILVER.ENCOUNTERS

New VARCHAR column tracking how patients were referred (physician,
self, ED transfer, etc.). Required for the upcoming referral
analytics tab.

BREAKING: Requires re-running SP_BRONZE_TO_SILVER() after deploy.
```

---

## Development Workflow

Follow the **6-stage workflow** defined in `.claude/skills/feature-workflow.md`:

```
1. PLAN  →  2. APPROVE  →  3. CODE  →  4. VERIFY  →  5. REVIEW  →  6. DEPLOY
```

### Stage 4: Verification Gates

All gates must pass before creating a PR:

| Gate | Command | What it checks |
|------|---------|----------------|
| 1. Tests | `make test` | All pytest tests pass (0 failures) |
| 2. Lint | `make lint` | ruff check passes (0 violations) |
| 3. Test coverage | Manual check | New `query_*` functions have 2+ tests |
| 4. Documentation | Manual check | README, CLAUDE.md counts are current |
| 5. Standards | Manual check | Naming, SQL conventions, no hardcoded secrets |

**Quick gate:** `make verify` runs gates 1 + 2 together.
**Full CI:** `make ci` runs gates 1 + 2 + security scanning.

---

## Code Review Standards

### What Reviewers Check

1. **Correctness** — Does the code do what it claims? Are edge cases handled?
2. **Standards compliance** — Follows `.claude/skills/standards.md` conventions
3. **Security** — No SQL injection, no hardcoded secrets, PII handled correctly
4. **Testing** — Adequate test coverage for new functionality
5. **Documentation** — Docstrings, README updates, CLAUDE.md consistency
6. **Snowflake conventions** — Uppercase identifiers, proper date functions, TRY_CAST

### Review Expectations

- **Respond within 1 business day** of being assigned
- **Be specific** — reference file:line, suggest exact code changes
- **Approve or request changes** — don't leave reviews in limbo
- **Approve when satisfied** — don't nitpick style if linter passes

---

## SQL Change Process

Schema changes (DDL) require extra care because they affect production data:

### Adding a Column

1. Add column to `snowflake/ddl/02_silver_tables.sql` (Silver) and `01_bronze_tables.sql` (Bronze)
2. Update `snowflake/etl/transform_bronze_to_silver.sql` to handle the new column
3. Update `snowflake/etl/load_stage_to_bronze.sql` with the new column mapping
4. Update `generate_sample_data.py` to generate data for the column
5. Add column comments to `snowflake/catalog/tags_and_comments.sql`
6. Update `snowflake/cortex/rcm_semantic_model.yaml` if the column is query-relevant
7. Update metadata tables (`seed_metadata.sql`) if KPI/semantic/KG affected
8. **Document rollback** — how to remove the column if needed

### Modifying a Column Type

- **Never** change a column type in-place on production data
- Add a new column with the correct type, migrate data, then drop the old column
- Always use `TRY_CAST()` in migration logic to handle conversion failures

### Dropping a Column

- Verify no downstream dependencies (Gold views, metrics.py queries, semantic model)
- Remove in reverse order: semantic model → metrics → Gold views → Silver DDL → Bronze DDL
- Keep the column in Bronze for audit trail (Bronze is append-only)

---

## Security Standards

### Secrets

- **Never** commit credentials, API keys, or account identifiers
- Use `.env` (gitignored) for local development
- Use GitHub Secrets for CI/CD
- Use Snowflake secrets for Git integration credentials

### PII / PHI Handling

This is a **healthcare application** — treat all patient data as protected:

- Patient name, DOB, ZIP code, and member ID are classified as **PII**
- All PII columns are tagged in the Horizon Data Catalog (`SENSITIVITY = 'PII'`)
- Never log or print PII values in error messages or debug output
- Never include real patient data in test fixtures — use synthetic data only
- The `generate_sample_data.py` script creates **fully synthetic** data with no real patients

### SQL Injection Prevention

- Use the `_esc()` helper for string literals in dynamic SQL
- Never interpolate user input directly into SQL strings
- Cortex Analyst generates SQL from natural language — users never execute SQL directly

### Dependency Security

- `pip-audit` checks for known vulnerabilities in CI
- `bandit` scans Python code for common security issues
- Review dependency updates before merging (check changelogs for breaking changes)

---

## File Organization

When adding new files, follow these conventions:

| File type | Location | Naming |
|-----------|----------|--------|
| Snowflake DDL | `snowflake/ddl/` | `NN_description.sql` (numbered for execution order) |
| ETL procedures | `snowflake/etl/` | `descriptive_name.sql` |
| Streamlit modules | `snowflake/streamlit/src/` | `snake_case.py` |
| Tests | `tests/` | `test_module_name.py` |
| GitHub workflows | `.github/workflows/` | `descriptive-name.yml` |
| Claude skills | `.claude/skills/` | `skill-name.md` |
| Claude agents | `.claude/agents/` | `agent-name.md` |

---

## Getting Help

- **Project guide:** Read `CLAUDE.md` for architecture overview and change recipes
- **Standards reference:** See `.claude/skills/standards.md` for coding conventions
- **Issues:** Report bugs or request features via GitHub Issues
- **Questions:** Open a Discussion on GitHub
