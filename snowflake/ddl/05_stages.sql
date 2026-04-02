-- ============================================================================
-- Stages & File Formats
-- ============================================================================
-- Internal named stage for uploading CSV files and the Cortex Analyst
-- semantic model YAML.
-- ============================================================================

USE DATABASE RCM_ANALYTICS;
USE SCHEMA STAGING;

-- CSV file format matching the generated sample data
CREATE FILE FORMAT IF NOT EXISTS CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    COMMENT = 'CSV format for RCM Analytics source data files';

-- Internal stage for all data files and semantic model
CREATE STAGE IF NOT EXISTS RCM_STAGE
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'Internal stage for RCM CSV data files and Cortex Analyst semantic model';
