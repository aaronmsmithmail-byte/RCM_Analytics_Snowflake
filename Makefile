# Healthcare RCM Analytics (Snowflake) — Development Commands
# ============================================================
# Usage: make <target>

.PHONY: test lint verify format coverage security ci setup data deploy

# ── Quality Gates ────────────────────────────────────────────
test:
	python -m pytest tests/ -q

lint:
	python -m ruff check snowflake/streamlit/ tests/ generate_sample_data.py

verify: lint test
	@echo ""
	@echo "All gates passed (lint + tests)"

# ── Coverage & Security ─────────────────────────────────────
coverage:
	python -m pytest tests/ -q --cov=snowflake/streamlit/src --cov-report=term-missing

security:
	bandit -r snowflake/streamlit/ -c bandit.toml --severity-level medium
	pip-audit -r requirements.txt

ci: lint test security
	@echo ""
	@echo "All CI gates passed (lint + tests + security)"

# ── Formatting ───────────────────────────────────────────────
format:
	python -m ruff check --fix snowflake/streamlit/ tests/ generate_sample_data.py
	python -m ruff format snowflake/streamlit/ tests/ generate_sample_data.py

# ── Setup ────────────────────────────────────────────────────
setup:
	pip install -r requirements.txt

data:
	python generate_sample_data.py

# ── Snowflake Deployment ────────────────────────────────────
deploy:
	snow sql -f snowflake/deploy.sql

deploy-streamlit:
	cd snowflake/streamlit && snow streamlit deploy --replace

deploy-cortex:
	snow stage copy snowflake/cortex/rcm_semantic_model.yaml @RCM_ANALYTICS.STAGING.RCM_STAGE/cortex/ --overwrite
