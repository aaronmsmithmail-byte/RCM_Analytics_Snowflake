-- ============================================================================
-- GOLD LAYER — Aggregated Business-Ready Views
-- ============================================================================
-- SQL VIEWs that join and aggregate Silver tables into the five KPI domains
-- used by the dashboard. Always up-to-date (computed at query time).
-- ============================================================================

USE DATABASE RCM_ANALYTICS;
USE SCHEMA GOLD;

-- Gold: Monthly KPI rollup
CREATE OR REPLACE VIEW MONTHLY_KPIS AS
SELECT
    TO_CHAR(TRY_TO_DATE(c.DATE_OF_SERVICE), 'YYYY-MM')              AS PERIOD,
    COUNT(DISTINCT c.CLAIM_ID)                                        AS TOTAL_CLAIMS,
    SUM(c.TOTAL_CHARGE_AMOUNT)                                        AS TOTAL_CHARGES,
    COALESCE(SUM(p.PAYMENT_AMOUNT), 0)                                AS TOTAL_PAYMENTS,
    SUM(CASE WHEN c.IS_CLEAN_CLAIM = 1 THEN 1 ELSE 0 END) * 1.0
        / NULLIF(COUNT(c.CLAIM_ID), 0)                                AS CLEAN_CLAIM_RATE,
    SUM(CASE WHEN c.CLAIM_STATUS = 'Denied' THEN 1 ELSE 0 END) * 1.0
        / NULLIF(COUNT(c.CLAIM_ID), 0)                                AS DENIAL_RATE,
    COALESCE(SUM(p.PAYMENT_AMOUNT), 0)::FLOAT
        / NULLIF(SUM(c.TOTAL_CHARGE_AMOUNT), 0)                      AS GROSS_COLLECTION_RATE
FROM SILVER.CLAIMS c
LEFT JOIN SILVER.PAYMENTS p ON c.CLAIM_ID = p.CLAIM_ID
GROUP BY TO_CHAR(TRY_TO_DATE(c.DATE_OF_SERVICE), 'YYYY-MM')
ORDER BY PERIOD;

-- Gold: Payer performance summary
CREATE OR REPLACE VIEW PAYER_PERFORMANCE AS
SELECT
    py.PAYER_ID,
    py.PAYER_NAME,
    py.PAYER_TYPE,
    COUNT(DISTINCT c.CLAIM_ID)                                        AS TOTAL_CLAIMS,
    SUM(c.TOTAL_CHARGE_AMOUNT)                                        AS TOTAL_CHARGES,
    COALESCE(SUM(p.PAYMENT_AMOUNT), 0)                                AS TOTAL_PAYMENTS,
    SUM(CASE WHEN c.CLAIM_STATUS = 'Denied' THEN 1 ELSE 0 END) * 1.0
        / NULLIF(COUNT(c.CLAIM_ID), 0)                                AS DENIAL_RATE,
    COALESCE(SUM(p.PAYMENT_AMOUNT), 0)::FLOAT
        / NULLIF(SUM(c.TOTAL_CHARGE_AMOUNT), 0)                      AS GROSS_COLLECTION_RATE
FROM SILVER.PAYERS py
LEFT JOIN SILVER.CLAIMS c   ON py.PAYER_ID = c.PAYER_ID
LEFT JOIN SILVER.PAYMENTS p ON c.CLAIM_ID  = p.CLAIM_ID
GROUP BY py.PAYER_ID, py.PAYER_NAME, py.PAYER_TYPE
ORDER BY TOTAL_PAYMENTS DESC;

-- Gold: Department performance summary
CREATE OR REPLACE VIEW DEPARTMENT_PERFORMANCE AS
SELECT
    e.DEPARTMENT,
    COUNT(DISTINCT e.ENCOUNTER_ID)                                    AS TOTAL_ENCOUNTERS,
    COALESCE(SUM(ch.CHARGE_AMOUNT), 0)                                AS TOTAL_CHARGES,
    COALESCE(SUM(p.PAYMENT_AMOUNT), 0)                                AS TOTAL_PAYMENTS,
    COALESCE(SUM(p.PAYMENT_AMOUNT), 0)::FLOAT
        / NULLIF(COUNT(DISTINCT e.ENCOUNTER_ID), 0)                   AS REVENUE_PER_ENCOUNTER
FROM SILVER.ENCOUNTERS e
LEFT JOIN SILVER.CHARGES   ch ON e.ENCOUNTER_ID = ch.ENCOUNTER_ID
LEFT JOIN SILVER.CLAIMS     c ON e.ENCOUNTER_ID = c.ENCOUNTER_ID
LEFT JOIN SILVER.PAYMENTS   p ON c.CLAIM_ID     = p.CLAIM_ID
GROUP BY e.DEPARTMENT
ORDER BY TOTAL_PAYMENTS DESC;

-- Gold: A/R aging buckets
CREATE OR REPLACE VIEW AR_AGING AS
SELECT
    CASE
        WHEN DATEDIFF('day', TRY_TO_DATE(c.DATE_OF_SERVICE), CURRENT_DATE()) <=  30 THEN '0-30 days'
        WHEN DATEDIFF('day', TRY_TO_DATE(c.DATE_OF_SERVICE), CURRENT_DATE()) <=  60 THEN '31-60 days'
        WHEN DATEDIFF('day', TRY_TO_DATE(c.DATE_OF_SERVICE), CURRENT_DATE()) <=  90 THEN '61-90 days'
        WHEN DATEDIFF('day', TRY_TO_DATE(c.DATE_OF_SERVICE), CURRENT_DATE()) <= 120 THEN '91-120 days'
        ELSE '120+ days'
    END                                                               AS AGING_BUCKET,
    COUNT(c.CLAIM_ID)                                                 AS CLAIM_COUNT,
    SUM(c.TOTAL_CHARGE_AMOUNT)                                        AS TOTAL_BILLED,
    COALESCE(SUM(p.PAYMENT_AMOUNT), 0)                                AS TOTAL_COLLECTED,
    SUM(c.TOTAL_CHARGE_AMOUNT) - COALESCE(SUM(p.PAYMENT_AMOUNT), 0)  AS OUTSTANDING_BALANCE
FROM SILVER.CLAIMS c
LEFT JOIN SILVER.PAYMENTS p ON c.CLAIM_ID = p.CLAIM_ID
WHERE c.CLAIM_STATUS NOT IN ('Paid')
GROUP BY AGING_BUCKET;

-- Gold: Denial reason analysis
CREATE OR REPLACE VIEW DENIAL_ANALYSIS AS
SELECT
    d.DENIAL_REASON_CODE,
    d.DENIAL_REASON_DESCRIPTION,
    COUNT(d.DENIAL_ID)                                                AS DENIAL_COUNT,
    SUM(d.DENIED_AMOUNT)                                              AS TOTAL_DENIED,
    COALESCE(SUM(d.RECOVERED_AMOUNT), 0)                              AS TOTAL_RECOVERED,
    SUM(CASE WHEN d.APPEAL_STATUS = 'Won' THEN 1 ELSE 0 END)::FLOAT
        / NULLIF(COUNT(d.DENIAL_ID), 0)                               AS APPEAL_SUCCESS_RATE
FROM SILVER.DENIALS d
GROUP BY d.DENIAL_REASON_CODE, d.DENIAL_REASON_DESCRIPTION
ORDER BY DENIAL_COUNT DESC;
