## Summary

<!-- What does this PR do and why? 1-3 bullet points. -->

-

## Type of Change

<!-- Check one: -->

- [ ] **Feature** — New functionality (new KPI, tab, metadata page, data entity)
- [ ] **Enhancement** — Improvement to existing functionality
- [ ] **Bug Fix** — Corrects incorrect behavior
- [ ] **Refactor** — Code restructuring with no behavior change
- [ ] **DDL / Schema Change** — Modifies Snowflake table structure (see checklist below)
- [ ] **ETL Change** — Modifies data pipeline logic
- [ ] **Documentation** — Updates to README, CLAUDE.md, or inline docs
- [ ] **CI/CD** — Changes to GitHub Actions workflows or Makefile

## Files Changed

<!-- List the key files modified and briefly describe each change. -->

| File | Change |
|------|--------|
|  |  |

## Testing

- [ ] `make verify` passes (lint + tests)
- [ ] New code has tests (every `query_*` function: 2+ tests; every public function: 1+ test)
- [ ] Tested manually in Snowsight / SiS (if applicable)

## Code Quality

- [ ] Follows naming conventions in `.claude/skills/standards.md`
- [ ] SQL uses uppercase identifiers and Snowflake functions (`TRY_CAST`, `DATEDIFF`, `TO_CHAR`)
- [ ] No hardcoded credentials, API keys, or account-specific values
- [ ] No `print()` statements in `snowflake/streamlit/src/` modules
- [ ] Error handling follows project patterns (graceful degradation, no crashes)

## Documentation

- [ ] README updated (if feature adds tabs, KPIs, metadata pages, or dependencies)
- [ ] CLAUDE.md updated (if architecture or file structure changed)
- [ ] `.env.example` updated (if new environment variables added)
- [ ] Inline docstrings added for new public functions

## DDL / Schema Change Checklist

<!-- Complete this section ONLY if this PR modifies Snowflake DDL. -->

- [ ] Migration is backwards-compatible (no breaking column drops or type changes)
- [ ] ETL stored procedure updated to handle new/changed columns
- [ ] `seed_metadata.sql` updated (KPI catalog, semantic layer, knowledge graph)
- [ ] `tags_and_comments.sql` updated (Horizon catalog tags and column comments)
- [ ] `rcm_semantic_model.yaml` updated (Cortex Analyst)
- [ ] `generate_sample_data.py` updated (if new data entities)
- [ ] Rollback plan documented (how to undo this DDL change)

## Cortex / AI Change Checklist

<!-- Complete this section ONLY if this PR modifies the AI Assistant. -->

- [ ] `rcm_semantic_model.yaml` changes tested via Cortex Analyst
- [ ] Semantic model re-staged to `@RCM_STAGE/cortex/`
- [ ] Sample questions tested end-to-end in AI Assistant tab
