-- ============================================================================
-- Task Scheduling — Daily ETL automation
-- ============================================================================
-- Wraps the ETL stored procedures into a Snowflake Task that runs daily.
-- ============================================================================

USE DATABASE RCM_ANALYTICS;
USE SCHEMA STAGING;

-- Full ETL wrapper: stage → bronze → silver
CREATE OR REPLACE PROCEDURE SP_FULL_ETL()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Note: COPY INTO is idempotent when files haven't changed (Snowflake
    -- tracks loaded files). For a full refresh, run load_stage_to_bronze.sql
    -- manually first, then this procedure handles bronze → silver.
    CALL SP_BRONZE_TO_SILVER();
    RETURN 'Full ETL pipeline completed successfully';
END;
$$;

-- Scheduled task: runs daily at 6 AM Eastern
CREATE OR REPLACE TASK DAILY_ETL_TASK
    WAREHOUSE = RCM_WH
    SCHEDULE = 'USING CRON 0 6 * * * America/New_York'
    COMMENT = 'Daily ETL: refresh Silver layer from Bronze data'
AS
    CALL SP_FULL_ETL();

-- Tasks are created in SUSPENDED state — resume to activate
ALTER TASK DAILY_ETL_TASK RESUME;
