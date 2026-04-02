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
    CALL SP_LOAD_STAGE_TO_BRONZE();
    CALL SP_BRONZE_TO_SILVER();
    RETURN 'Full ETL pipeline completed successfully (stage → bronze → silver)';
END;
$$;

-- Scheduled task: runs daily at 6 AM Eastern
CREATE OR REPLACE TASK DAILY_ETL_TASK
    WAREHOUSE = RCM_WH
    SCHEDULE = 'USING CRON 0 6 * * * America/New_York'
    COMMENT = 'Daily ETL: reload stage → bronze → silver'
AS
    CALL SP_FULL_ETL();

-- Tasks are created in SUSPENDED state — resume to activate
ALTER TASK DAILY_ETL_TASK RESUME;
