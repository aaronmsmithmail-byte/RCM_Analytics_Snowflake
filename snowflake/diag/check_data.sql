-- ============================================================================
-- Data Diagnostic Script — Run in Snowsight to verify data loaded correctly
-- ============================================================================
-- Checks row counts across Bronze, Silver, and Gold layers, validates
-- payment-to-claim joins, and spot-checks the GCR calculation.
--
-- EXPECTED RESULTS (from generate_sample_data.py defaults):
--   Payers: 10 | Patients: 500 | Providers: 25 | Encounters: 3,000
--   Charges: ~5,900 | Claims: 2,800 | Payments: ~3,100 | Denials: ~385
--   Adjustments: 600 | Operating Costs: 24
-- ============================================================================

USE DATABASE RCM_ANALYTICS;

-- =========================================================================
-- 1. BRONZE LAYER — Row counts (should match CSV file row counts)
-- =========================================================================
SELECT 'BRONZE' AS LAYER, 'PAYERS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM BRONZE.PAYERS
UNION ALL SELECT 'BRONZE', 'PATIENTS', COUNT(*) FROM BRONZE.PATIENTS
UNION ALL SELECT 'BRONZE', 'PROVIDERS', COUNT(*) FROM BRONZE.PROVIDERS
UNION ALL SELECT 'BRONZE', 'ENCOUNTERS', COUNT(*) FROM BRONZE.ENCOUNTERS
UNION ALL SELECT 'BRONZE', 'CHARGES', COUNT(*) FROM BRONZE.CHARGES
UNION ALL SELECT 'BRONZE', 'CLAIMS', COUNT(*) FROM BRONZE.CLAIMS
UNION ALL SELECT 'BRONZE', 'PAYMENTS', COUNT(*) FROM BRONZE.PAYMENTS
UNION ALL SELECT 'BRONZE', 'DENIALS', COUNT(*) FROM BRONZE.DENIALS
UNION ALL SELECT 'BRONZE', 'ADJUSTMENTS', COUNT(*) FROM BRONZE.ADJUSTMENTS
UNION ALL SELECT 'BRONZE', 'OPERATING_COSTS', COUNT(*) FROM BRONZE.OPERATING_COSTS
ORDER BY LAYER, TABLE_NAME;

-- =========================================================================
-- 2. SILVER LAYER — Row counts (should match Bronze after ETL)
-- =========================================================================
SELECT 'SILVER' AS LAYER, 'PAYERS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM SILVER.PAYERS
UNION ALL SELECT 'SILVER', 'PATIENTS', COUNT(*) FROM SILVER.PATIENTS
UNION ALL SELECT 'SILVER', 'PROVIDERS', COUNT(*) FROM SILVER.PROVIDERS
UNION ALL SELECT 'SILVER', 'ENCOUNTERS', COUNT(*) FROM SILVER.ENCOUNTERS
UNION ALL SELECT 'SILVER', 'CHARGES', COUNT(*) FROM SILVER.CHARGES
UNION ALL SELECT 'SILVER', 'CLAIMS', COUNT(*) FROM SILVER.CLAIMS
UNION ALL SELECT 'SILVER', 'PAYMENTS', COUNT(*) FROM SILVER.PAYMENTS
UNION ALL SELECT 'SILVER', 'DENIALS', COUNT(*) FROM SILVER.DENIALS
UNION ALL SELECT 'SILVER', 'ADJUSTMENTS', COUNT(*) FROM SILVER.ADJUSTMENTS
UNION ALL SELECT 'SILVER', 'OPERATING_COSTS', COUNT(*) FROM SILVER.OPERATING_COSTS
ORDER BY LAYER, TABLE_NAME;

-- =========================================================================
-- 3. GOLD LAYER — Row counts (views, so computed on the fly)
-- =========================================================================
SELECT 'GOLD' AS LAYER, 'MONTHLY_KPIS' AS VIEW_NAME, COUNT(*) AS ROW_COUNT FROM GOLD.MONTHLY_KPIS
UNION ALL SELECT 'GOLD', 'PAYER_PERFORMANCE', COUNT(*) FROM GOLD.PAYER_PERFORMANCE
UNION ALL SELECT 'GOLD', 'DEPARTMENT_PERFORMANCE', COUNT(*) FROM GOLD.DEPARTMENT_PERFORMANCE
UNION ALL SELECT 'GOLD', 'AR_AGING', COUNT(*) FROM GOLD.AR_AGING
UNION ALL SELECT 'GOLD', 'DENIAL_ANALYSIS', COUNT(*) FROM GOLD.DENIAL_ANALYSIS
ORDER BY LAYER, VIEW_NAME;

-- =========================================================================
-- 4. PAYMENT DATA CHECK — Are payments populated with real amounts?
-- =========================================================================
SELECT
    COUNT(*)                    AS TOTAL_PAYMENTS,
    MIN(PAYMENT_AMOUNT)         AS MIN_AMOUNT,
    MAX(PAYMENT_AMOUNT)         AS MAX_AMOUNT,
    ROUND(AVG(PAYMENT_AMOUNT), 2) AS AVG_AMOUNT,
    ROUND(SUM(PAYMENT_AMOUNT), 2) AS TOTAL_AMOUNT
FROM SILVER.PAYMENTS;

-- =========================================================================
-- 5. CLAIMS-TO-PAYMENTS JOIN — Do payments link to claims correctly?
-- =========================================================================
SELECT
    COUNT(DISTINCT c.CLAIM_ID)  AS TOTAL_CLAIMS,
    COUNT(DISTINCT p.CLAIM_ID)  AS CLAIMS_WITH_PAYMENTS,
    COUNT(DISTINCT c.CLAIM_ID) - COUNT(DISTINCT p.CLAIM_ID) AS CLAIMS_WITHOUT_PAYMENTS
FROM SILVER.CLAIMS c
LEFT JOIN SILVER.PAYMENTS p ON c.CLAIM_ID = p.CLAIM_ID;

-- =========================================================================
-- 6. ORPHAN CHECK — Payments referencing non-existent claims
-- =========================================================================
SELECT COUNT(*) AS ORPHAN_PAYMENTS
FROM SILVER.PAYMENTS p
LEFT JOIN SILVER.CLAIMS c ON p.CLAIM_ID = c.CLAIM_ID
WHERE c.CLAIM_ID IS NULL;

-- =========================================================================
-- 7. SPOT-CHECK GCR — Monthly Gross Collection Rate (first 6 months)
--    GCR should be 50-80% per month if data is loaded correctly.
--    If payments = 0, the data load or ETL has a problem.
-- =========================================================================
SELECT
    TO_CHAR(TRY_TO_DATE(c.DATE_OF_SERVICE, 'YYYY-MM-DD'), 'YYYY-MM') AS PERIOD,
    ROUND(SUM(c.TOTAL_CHARGE_AMOUNT), 2)                              AS CHARGES,
    ROUND(COALESCE(SUM(p.PAYMENT_AMOUNT), 0), 2)                      AS PAYMENTS,
    ROUND(COALESCE(SUM(p.PAYMENT_AMOUNT), 0)
          / NULLIF(SUM(c.TOTAL_CHARGE_AMOUNT), 0) * 100, 1)           AS GCR_PCT
FROM SILVER.CLAIMS c
LEFT JOIN SILVER.PAYMENTS p ON c.CLAIM_ID = p.CLAIM_ID
GROUP BY PERIOD
ORDER BY PERIOD
LIMIT 6;

-- =========================================================================
-- 8. DATE FORMAT CHECK — Verify TRY_TO_DATE parses correctly
--    If PARSED_DATE is NULL, the date format is wrong.
-- =========================================================================
SELECT
    DATE_OF_SERVICE                                          AS RAW_VALUE,
    TRY_TO_DATE(DATE_OF_SERVICE, 'YYYY-MM-DD')              AS PARSED_DATE,
    TO_CHAR(TRY_TO_DATE(DATE_OF_SERVICE, 'YYYY-MM-DD'), 'YYYY-MM') AS PERIOD
FROM SILVER.CLAIMS
LIMIT 5;

-- =========================================================================
-- 9. CLAIM STATUS DISTRIBUTION — Verify realistic distribution
-- =========================================================================
SELECT CLAIM_STATUS, COUNT(*) AS CNT,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS PCT
FROM SILVER.CLAIMS
GROUP BY CLAIM_STATUS
ORDER BY CNT DESC;
