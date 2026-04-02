-- ============================================================================
-- RCM Analytics — Snowflake Master Deployment Script
-- ============================================================================
-- Runs all DDL, ETL, catalog, and metadata scripts in the correct order.
--
-- Usage (via Snowflake CLI):
--   snow sql -f snowflake/deploy.sql
--
-- Usage (via Snowsight):
--   Open this file and run all statements
--
-- Prerequisites:
--   - Snowflake account with SYSADMIN role access
--   - CSV data files uploaded to @RCM_ANALYTICS.STAGING.RCM_STAGE
-- ============================================================================

-- Phase 1: Environment & DDL
!source snowflake/setup/00_environment.sql;
!source snowflake/ddl/01_bronze_tables.sql;
!source snowflake/ddl/02_silver_tables.sql;
!source snowflake/ddl/03_gold_views.sql;
!source snowflake/ddl/04_metadata_tables.sql;
!source snowflake/ddl/05_stages.sql;

-- Phase 2: ETL
-- Note: CSV files must be uploaded to @RCM_STAGE before running these.
-- Use SnowSQL: PUT file:///path/to/data/*.csv @RCM_ANALYTICS.STAGING.RCM_STAGE;
-- Or use Snowsight: Data > Databases > RCM_ANALYTICS > STAGING > RCM_STAGE > Upload
!source snowflake/etl/load_stage_to_bronze.sql;

-- Create and run the Bronze → Silver stored procedure
!source snowflake/etl/transform_bronze_to_silver.sql;
CALL RCM_ANALYTICS.STAGING.SP_BRONZE_TO_SILVER();

-- Seed metadata
!source snowflake/etl/seed_metadata.sql;

-- Horizon Data Catalog
!source snowflake/catalog/tags_and_comments.sql;

-- Task scheduling
!source snowflake/etl/tasks.sql;

-- Stage Cortex Analyst semantic model
-- Note: Run separately via SnowSQL:
--   PUT file:///path/to/snowflake/cortex/rcm_semantic_model.yaml
--       @RCM_ANALYTICS.STAGING.RCM_STAGE/cortex/
--       AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Streamlit app deployment
-- Note: Deploy via Snowflake CLI:
--   snow streamlit deploy --replace

SELECT 'Deployment complete!' AS STATUS;
