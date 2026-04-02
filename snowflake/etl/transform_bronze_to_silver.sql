-- ============================================================================
-- Bronze → Silver: Type-cast and validate data into the Silver layer
-- ============================================================================
-- Stored procedure for idempotent full refresh of all Silver tables.
-- Reference tables are loaded first to satisfy FK constraints.
-- ============================================================================

USE DATABASE RCM_ANALYTICS;
USE SCHEMA STAGING;

CREATE OR REPLACE PROCEDURE SP_BRONZE_TO_SILVER()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- =====================================================================
    -- Reference tables (no FK dependencies)
    -- =====================================================================

    TRUNCATE TABLE SILVER.PAYERS;
    INSERT INTO SILVER.PAYERS
    SELECT PAYER_ID, PAYER_NAME, PAYER_TYPE,
           TRY_CAST(AVG_REIMBURSEMENT_PCT AS FLOAT),
           CONTRACT_ID
    FROM BRONZE.PAYERS
    WHERE PAYER_ID IS NOT NULL AND PAYER_ID != '';

    TRUNCATE TABLE SILVER.PATIENTS;
    INSERT INTO SILVER.PATIENTS
    SELECT PATIENT_ID, FIRST_NAME, LAST_NAME, DATE_OF_BIRTH, GENDER,
           PRIMARY_PAYER_ID, MEMBER_ID, ZIP_CODE
    FROM BRONZE.PATIENTS
    WHERE PATIENT_ID IS NOT NULL AND PATIENT_ID != '';

    TRUNCATE TABLE SILVER.PROVIDERS;
    INSERT INTO SILVER.PROVIDERS
    SELECT PROVIDER_ID, PROVIDER_NAME, NPI, DEPARTMENT, SPECIALTY
    FROM BRONZE.PROVIDERS
    WHERE PROVIDER_ID IS NOT NULL AND PROVIDER_ID != '';

    -- =====================================================================
    -- Transactional tables (depend on reference tables above)
    -- =====================================================================

    TRUNCATE TABLE SILVER.ENCOUNTERS;
    INSERT INTO SILVER.ENCOUNTERS
    SELECT ENCOUNTER_ID, PATIENT_ID, PROVIDER_ID, DATE_OF_SERVICE,
           DISCHARGE_DATE, ENCOUNTER_TYPE, DEPARTMENT
    FROM BRONZE.ENCOUNTERS
    WHERE ENCOUNTER_ID IS NOT NULL AND ENCOUNTER_ID != '';

    TRUNCATE TABLE SILVER.CHARGES;
    INSERT INTO SILVER.CHARGES
    SELECT CHARGE_ID, ENCOUNTER_ID, CPT_CODE, CPT_DESCRIPTION,
           TRY_CAST(UNITS AS INTEGER),
           TRY_CAST(CHARGE_AMOUNT AS FLOAT),
           SERVICE_DATE, POST_DATE, ICD10_CODE
    FROM BRONZE.CHARGES
    WHERE CHARGE_ID IS NOT NULL AND CHARGE_ID != '';

    TRUNCATE TABLE SILVER.CLAIMS;
    INSERT INTO SILVER.CLAIMS
    SELECT CLAIM_ID, ENCOUNTER_ID, PATIENT_ID, PAYER_ID,
           DATE_OF_SERVICE, SUBMISSION_DATE,
           TRY_CAST(TOTAL_CHARGE_AMOUNT AS FLOAT),
           CLAIM_STATUS,
           CASE UPPER(TRIM(IS_CLEAN_CLAIM))
               WHEN 'TRUE'  THEN 1
               WHEN '1'     THEN 1
               WHEN 'YES'   THEN 1
               ELSE 0
           END,
           SUBMISSION_METHOD,
           NULLIF(TRIM(COALESCE(FAIL_REASON, '')), '')
    FROM BRONZE.CLAIMS
    WHERE CLAIM_ID IS NOT NULL AND CLAIM_ID != '';

    TRUNCATE TABLE SILVER.PAYMENTS;
    INSERT INTO SILVER.PAYMENTS
    SELECT PAYMENT_ID, CLAIM_ID, PAYER_ID,
           TRY_CAST(PAYMENT_AMOUNT AS FLOAT),
           TRY_CAST(ALLOWED_AMOUNT AS FLOAT),
           PAYMENT_DATE, PAYMENT_METHOD,
           CASE UPPER(TRIM(IS_ACCURATE_PAYMENT))
               WHEN 'TRUE'  THEN 1
               WHEN '1'     THEN 1
               WHEN 'YES'   THEN 1
               ELSE 0
           END
    FROM BRONZE.PAYMENTS
    WHERE PAYMENT_ID IS NOT NULL AND PAYMENT_ID != '';

    TRUNCATE TABLE SILVER.DENIALS;
    INSERT INTO SILVER.DENIALS
    SELECT DENIAL_ID, CLAIM_ID, DENIAL_REASON_CODE, DENIAL_REASON_DESCRIPTION,
           DENIAL_DATE,
           TRY_CAST(DENIED_AMOUNT AS FLOAT),
           APPEAL_STATUS, APPEAL_DATE,
           TRY_CAST(COALESCE(NULLIF(RECOVERED_AMOUNT, ''), '0') AS FLOAT)
    FROM BRONZE.DENIALS
    WHERE DENIAL_ID IS NOT NULL AND DENIAL_ID != '';

    TRUNCATE TABLE SILVER.ADJUSTMENTS;
    INSERT INTO SILVER.ADJUSTMENTS
    SELECT ADJUSTMENT_ID, CLAIM_ID, ADJUSTMENT_TYPE_CODE, ADJUSTMENT_TYPE_DESCRIPTION,
           TRY_CAST(ADJUSTMENT_AMOUNT AS FLOAT),
           ADJUSTMENT_DATE
    FROM BRONZE.ADJUSTMENTS
    WHERE ADJUSTMENT_ID IS NOT NULL AND ADJUSTMENT_ID != '';

    -- =====================================================================
    -- Operational tables
    -- =====================================================================

    TRUNCATE TABLE SILVER.OPERATING_COSTS;
    INSERT INTO SILVER.OPERATING_COSTS
    SELECT PERIOD,
           TRY_CAST(BILLING_STAFF_COST AS FLOAT),
           TRY_CAST(SOFTWARE_COST      AS FLOAT),
           TRY_CAST(OUTSOURCING_COST   AS FLOAT),
           TRY_CAST(SUPPLIES_OVERHEAD  AS FLOAT),
           TRY_CAST(TOTAL_RCM_COST     AS FLOAT)
    FROM BRONZE.OPERATING_COSTS
    WHERE PERIOD IS NOT NULL AND PERIOD != '';

    -- =====================================================================
    -- Update pipeline tracking
    -- =====================================================================

    MERGE INTO METADATA.PIPELINE_RUNS t
    USING (
        SELECT 'payers' AS DOMAIN, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS LAST_LOADED_AT, COUNT(*) AS ROW_COUNT, 'payers.csv' AS SOURCE_FILE FROM SILVER.PAYERS
        UNION ALL SELECT 'patients', CURRENT_TIMESTAMP(), COUNT(*), 'patients.csv' FROM SILVER.PATIENTS
        UNION ALL SELECT 'providers', CURRENT_TIMESTAMP(), COUNT(*), 'providers.csv' FROM SILVER.PROVIDERS
        UNION ALL SELECT 'encounters', CURRENT_TIMESTAMP(), COUNT(*), 'encounters.csv' FROM SILVER.ENCOUNTERS
        UNION ALL SELECT 'charges', CURRENT_TIMESTAMP(), COUNT(*), 'charges.csv' FROM SILVER.CHARGES
        UNION ALL SELECT 'claims', CURRENT_TIMESTAMP(), COUNT(*), 'claims.csv' FROM SILVER.CLAIMS
        UNION ALL SELECT 'payments', CURRENT_TIMESTAMP(), COUNT(*), 'payments.csv' FROM SILVER.PAYMENTS
        UNION ALL SELECT 'denials', CURRENT_TIMESTAMP(), COUNT(*), 'denials.csv' FROM SILVER.DENIALS
        UNION ALL SELECT 'adjustments', CURRENT_TIMESTAMP(), COUNT(*), 'adjustments.csv' FROM SILVER.ADJUSTMENTS
        UNION ALL SELECT 'operating_costs', CURRENT_TIMESTAMP(), COUNT(*), 'operating_costs.csv' FROM SILVER.OPERATING_COSTS
    ) s ON t.DOMAIN = s.DOMAIN
    WHEN MATCHED THEN UPDATE SET
        t.LAST_LOADED_AT = s.LAST_LOADED_AT,
        t.ROW_COUNT = s.ROW_COUNT,
        t.SOURCE_FILE = s.SOURCE_FILE
    WHEN NOT MATCHED THEN INSERT (DOMAIN, LAST_LOADED_AT, ROW_COUNT, SOURCE_FILE)
        VALUES (s.DOMAIN, s.LAST_LOADED_AT, s.ROW_COUNT, s.SOURCE_FILE);

    RETURN 'Bronze → Silver ETL completed successfully';
END;
$$;
