-- ============================================================================
-- RCM Analytics — Complete Snowflake Setup from Git Repository
-- ============================================================================
-- This single script sets up the entire RCM Analytics platform from scratch.
--
-- Prerequisites:
--   1. A Snowflake account (free trial works)
--   2. ACCOUNTADMIN role access
--   3. A GitHub Personal Access Token (PAT) with Contents: Read-only
--      Generate at: GitHub > Settings > Developer settings > Personal access tokens
--
-- Usage:
--   1. Open this file in Snowsight
--   2. Replace the two placeholders on lines marked with <REPLACE>
--   3. Run each numbered section in order (highlight + click play)
--
-- Time to complete: ~10 minutes
-- ============================================================================


-- ============================================================================
-- SECTION 1: Initial Setup (run as ACCOUNTADMIN)
-- ============================================================================
-- Creates the database, schemas, warehouse, and GitHub integration.
-- This section requires ACCOUNTADMIN and only needs to run once.
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- 1a. Create database and schemas
CREATE DATABASE IF NOT EXISTS RCM_ANALYTICS
    COMMENT = 'Healthcare Revenue Cycle Management Analytics Platform';

CREATE SCHEMA IF NOT EXISTS RCM_ANALYTICS.BRONZE
    COMMENT = 'Raw ingestion layer — data lands exactly as received from CSV sources';
CREATE SCHEMA IF NOT EXISTS RCM_ANALYTICS.SILVER
    COMMENT = 'Cleaned and conformed layer — typed columns, validated rows, FK constraints';
CREATE SCHEMA IF NOT EXISTS RCM_ANALYTICS.GOLD
    COMMENT = 'Aggregated business-ready layer — pre-joined KPI views for the dashboard';
CREATE SCHEMA IF NOT EXISTS RCM_ANALYTICS.METADATA
    COMMENT = 'Business metadata — KPI catalog, semantic layer, knowledge graph nodes/edges';
CREATE SCHEMA IF NOT EXISTS RCM_ANALYTICS.STAGING
    COMMENT = 'Data staging — internal stages, file formats, ETL stored procedures, tasks';

-- 1b. Create warehouse
CREATE WAREHOUSE IF NOT EXISTS RCM_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Compute warehouse for RCM Analytics ETL and dashboard queries';

-- 1c. Create GitHub secret for Git integration
-- <REPLACE> Update USERNAME and PASSWORD below with your GitHub credentials
CREATE OR REPLACE SECRET RCM_ANALYTICS.STAGING.GITHUB_SECRET
    TYPE = password
    USERNAME = '<your_github_username>'                -- <REPLACE> Your GitHub username
    PASSWORD = '<your_github_pat_here>';               -- <REPLACE> Your GitHub PAT (github_pat_...)

-- 1d. Create API integration for GitHub
CREATE OR REPLACE API INTEGRATION GITHUB_INTEGRATION
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/')
    ALLOWED_AUTHENTICATION_SECRETS = (RCM_ANALYTICS.STAGING.GITHUB_SECRET)
    ENABLED = TRUE;

-- 1e. Grant ownership to SYSADMIN for day-to-day operations
GRANT OWNERSHIP ON DATABASE RCM_ANALYTICS TO ROLE SYSADMIN COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL SCHEMAS IN DATABASE RCM_ANALYTICS TO ROLE SYSADMIN COPY CURRENT GRANTS;
GRANT USAGE ON SECRET RCM_ANALYTICS.STAGING.GITHUB_SECRET TO ROLE SYSADMIN;
GRANT USAGE ON INTEGRATION GITHUB_INTEGRATION TO ROLE SYSADMIN;
GRANT OWNERSHIP ON WAREHOUSE RCM_WH TO ROLE SYSADMIN COPY CURRENT GRANTS;


-- ============================================================================
-- SECTION 2: Connect Git Repository (run as SYSADMIN)
-- ============================================================================
-- Links the GitHub repo to Snowflake so all SQL/YAML/Python files are
-- accessible as a Snowflake stage.
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE RCM_ANALYTICS;
USE SCHEMA STAGING;
USE WAREHOUSE RCM_WH;

-- 2a. Create the Git repository connection
CREATE OR REPLACE GIT REPOSITORY RCM_REPO
    API_INTEGRATION = GITHUB_INTEGRATION
    GIT_CREDENTIALS = RCM_ANALYTICS.STAGING.GITHUB_SECRET
    -- NOTE: Update this URL if you forked the repository to your own GitHub account
    ORIGIN = 'https://github.com/aaronmsmithmail-byte/RCM_Analytics_Snowflake.git';

-- 2b. Fetch latest from GitHub
ALTER GIT REPOSITORY RCM_REPO FETCH;

-- 2c. Verify — you should see ~21 files listed
LS @RCM_ANALYTICS.STAGING.RCM_REPO/branches/main/snowflake/;


-- ============================================================================
-- SECTION 3: Deploy DDL from Git (run as SYSADMIN)
-- ============================================================================
-- Creates all Bronze, Silver, Gold tables/views, metadata tables,
-- internal stages, and file formats.
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE RCM_ANALYTICS;
USE SCHEMA STAGING;

-- 3a. Bronze tables (10 tables, all VARCHAR)
EXECUTE IMMEDIATE FROM @RCM_ANALYTICS.STAGING.RCM_REPO/branches/main/snowflake/ddl/01_bronze_tables.sql;

-- 3b. Silver tables (10 typed tables with FK constraints)
EXECUTE IMMEDIATE FROM @RCM_ANALYTICS.STAGING.RCM_REPO/branches/main/snowflake/ddl/02_silver_tables.sql;

-- 3c. Gold views (5 aggregated business views)
EXECUTE IMMEDIATE FROM @RCM_ANALYTICS.STAGING.RCM_REPO/branches/main/snowflake/ddl/03_gold_views.sql;

-- 3d. Metadata tables (KPI catalog, semantic layer, knowledge graph)
EXECUTE IMMEDIATE FROM @RCM_ANALYTICS.STAGING.RCM_REPO/branches/main/snowflake/ddl/04_metadata_tables.sql;

-- 3e. Internal stage and CSV file format
EXECUTE IMMEDIATE FROM @RCM_ANALYTICS.STAGING.RCM_REPO/branches/main/snowflake/ddl/05_stages.sql;


-- ============================================================================
-- SECTION 4: Deploy ETL Pipeline (run as SYSADMIN)
-- ============================================================================
-- Creates the stored procedure for Bronze→Silver ETL and the daily
-- scheduled task.
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE RCM_ANALYTICS;
USE SCHEMA STAGING;

-- 4a. Create Stage→Bronze stored procedure (loads all 10 CSVs)
EXECUTE IMMEDIATE FROM @RCM_ANALYTICS.STAGING.RCM_REPO/branches/main/snowflake/etl/load_stage_to_bronze.sql;

-- 4b. Create Bronze→Silver stored procedure
EXECUTE IMMEDIATE FROM @RCM_ANALYTICS.STAGING.RCM_REPO/branches/main/snowflake/etl/transform_bronze_to_silver.sql;

-- 4c. Create daily ETL task (runs at 6 AM ET)
EXECUTE IMMEDIATE FROM @RCM_ANALYTICS.STAGING.RCM_REPO/branches/main/snowflake/etl/tasks.sql;


-- ============================================================================
-- SECTION 5: Load Sample Data (run as SYSADMIN)
-- ============================================================================
-- Before running this section, you need to upload CSV files to the stage.
--
-- Option A (Snowsight UI):
--   1. Go to Data > Databases > RCM_ANALYTICS > STAGING > Stages > RCM_STAGE
--   2. Click "+ Files" and upload all 10 CSV files from the data/ directory
--
-- Option B (SnowSQL CLI):
--   PUT file:///path/to/data/*.csv @RCM_ANALYTICS.STAGING.RCM_STAGE;
--
-- Option C (Snowflake CLI):
--   snow stage copy data/payers.csv @RCM_ANALYTICS.STAGING.RCM_STAGE
--   (repeat for each CSV file)
--
-- Generate the CSVs first by running locally:
--   python generate_sample_data.py
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE RCM_ANALYTICS;
USE SCHEMA STAGING;

-- 5a. Load CSVs from stage into Bronze tables
CALL RCM_ANALYTICS.STAGING.SP_LOAD_STAGE_TO_BRONZE();

-- 5b. Run Bronze→Silver ETL
CALL RCM_ANALYTICS.STAGING.SP_BRONZE_TO_SILVER();

-- 5c. Verify row counts
SELECT 'BRONZE.PAYERS' AS table_name, COUNT(*) AS row_count FROM RCM_ANALYTICS.BRONZE.PAYERS
UNION ALL SELECT 'BRONZE.PATIENTS', COUNT(*) FROM RCM_ANALYTICS.BRONZE.PATIENTS
UNION ALL SELECT 'BRONZE.CLAIMS', COUNT(*) FROM RCM_ANALYTICS.BRONZE.CLAIMS
UNION ALL SELECT 'SILVER.PAYERS', COUNT(*) FROM RCM_ANALYTICS.SILVER.PAYERS
UNION ALL SELECT 'SILVER.PATIENTS', COUNT(*) FROM RCM_ANALYTICS.SILVER.PATIENTS
UNION ALL SELECT 'SILVER.CLAIMS', COUNT(*) FROM RCM_ANALYTICS.SILVER.CLAIMS
UNION ALL SELECT 'SILVER.PAYMENTS', COUNT(*) FROM RCM_ANALYTICS.SILVER.PAYMENTS
ORDER BY table_name;


-- ============================================================================
-- SECTION 6: Seed Metadata & Catalog (run as SYSADMIN)
-- ============================================================================
-- Populates KPI definitions, semantic layer mappings, knowledge graph,
-- and Horizon Data Catalog tags/comments.
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE RCM_ANALYTICS;
USE SCHEMA STAGING;

-- 6a. Seed metadata tables (23 KPIs, 21 semantic mappings, 10 KG nodes, 11 edges)
EXECUTE IMMEDIATE FROM @RCM_ANALYTICS.STAGING.RCM_REPO/branches/main/snowflake/etl/seed_metadata.sql;

-- 6b. Apply Horizon Data Catalog tags and comments
-- Note: Tag creation requires ACCOUNTADMIN or a role with CREATE TAG privilege
USE ROLE ACCOUNTADMIN;
EXECUTE IMMEDIATE FROM @RCM_ANALYTICS.STAGING.RCM_REPO/branches/main/snowflake/catalog/tags_and_comments.sql;


-- ============================================================================
-- SECTION 7: Stage Cortex Analyst Semantic Model (run as SYSADMIN)
-- ============================================================================
-- Copies the semantic model YAML from the Git repo to the data stage
-- where Cortex Analyst can read it.
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE RCM_ANALYTICS;
USE SCHEMA STAGING;

COPY FILES
    INTO @RCM_ANALYTICS.STAGING.RCM_STAGE/cortex/
    FROM @RCM_ANALYTICS.STAGING.RCM_REPO/branches/main/snowflake/cortex/
    FILES = ('rcm_semantic_model.yaml');

-- Verify the semantic model is staged
LS @RCM_ANALYTICS.STAGING.RCM_STAGE/cortex/;


-- ============================================================================
-- SECTION 8: Deploy Streamlit App (run as SYSADMIN)
-- ============================================================================
-- Creates the Streamlit app directly from the Git repo. The app reads
-- rcm_dashboard.py and src/ modules from the main branch automatically.
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE RCM_ANALYTICS;
USE SCHEMA STAGING;

CREATE OR REPLACE STREAMLIT RCM_DASHBOARD
    ROOT_LOCATION = '@RCM_ANALYTICS.STAGING.RCM_REPO/branches/main/snowflake/streamlit'
    MAIN_FILE = 'rcm_dashboard.py'
    QUERY_WAREHOUSE = RCM_WH
    COMMENT = 'Healthcare RCM Analytics Dashboard — 12 tabs + Cortex Analyst AI';

-- To open the app: Projects > Streamlit > RCM_DASHBOARD


-- ============================================================================
-- SECTION 9: Verify Deployment
-- ============================================================================
-- Run these queries to confirm everything is set up correctly.
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE RCM_ANALYTICS;
USE WAREHOUSE RCM_WH;

-- 9a. Check all schemas exist
SHOW SCHEMAS IN DATABASE RCM_ANALYTICS;

-- 9b. Check Bronze tables (should show 10)
SHOW TABLES IN SCHEMA RCM_ANALYTICS.BRONZE;

-- 9c. Check Silver tables (should show 10)
SHOW TABLES IN SCHEMA RCM_ANALYTICS.SILVER;

-- 9d. Check Gold views (should show 5)
SHOW VIEWS IN SCHEMA RCM_ANALYTICS.GOLD;

-- 9e. Check metadata tables (should show 6)
SHOW TABLES IN SCHEMA RCM_ANALYTICS.METADATA;

-- 9f. Preview a Gold view
SELECT * FROM RCM_ANALYTICS.GOLD.MONTHLY_KPIS LIMIT 5;

-- 9g. Check KPI catalog
SELECT COUNT(*) AS kpi_count FROM RCM_ANALYTICS.METADATA.KPI_CATALOG;

-- 9h. Check Cortex semantic model is staged
LS @RCM_ANALYTICS.STAGING.RCM_STAGE/cortex/;


-- ============================================================================
-- DONE! Your RCM Analytics platform is deployed.
-- ============================================================================
-- Next steps:
--   1. Upload CSV data files to @RCM_STAGE (Section 5)
--   2. Create the Streamlit app (Section 8)
--   3. Start exploring your data!
--
-- To redeploy after code changes:
--   ALTER GIT REPOSITORY RCM_ANALYTICS.STAGING.RCM_REPO FETCH;
--   EXECUTE IMMEDIATE FROM @RCM_ANALYTICS.STAGING.RCM_REPO/branches/main/snowflake/ddl/...;
-- ============================================================================
