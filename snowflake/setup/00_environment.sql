-- ============================================================================
-- Snowflake Environment Setup
-- ============================================================================
-- Creates the database, schemas, warehouse, and roles for the RCM Analytics
-- Snowflake deployment. Run this first before any other DDL scripts.
-- ============================================================================

USE ROLE SYSADMIN;

-- Database
CREATE DATABASE IF NOT EXISTS RCM_ANALYTICS
    COMMENT = 'Healthcare Revenue Cycle Management Analytics Platform';

USE DATABASE RCM_ANALYTICS;

-- Schemas (Medallion Architecture)
CREATE SCHEMA IF NOT EXISTS BRONZE
    COMMENT = 'Raw ingestion layer — data lands exactly as received from CSV sources';

CREATE SCHEMA IF NOT EXISTS SILVER
    COMMENT = 'Cleaned and conformed layer — typed columns, validated rows, FK constraints';

CREATE SCHEMA IF NOT EXISTS GOLD
    COMMENT = 'Aggregated business-ready layer — pre-joined KPI views for the dashboard';

CREATE SCHEMA IF NOT EXISTS METADATA
    COMMENT = 'Business metadata — KPI catalog, semantic layer, knowledge graph nodes/edges';

CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Data staging — internal stages, file formats, ETL stored procedures, tasks';

-- Warehouse
CREATE WAREHOUSE IF NOT EXISTS RCM_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Compute warehouse for RCM Analytics ETL and dashboard queries';

USE WAREHOUSE RCM_WH;
