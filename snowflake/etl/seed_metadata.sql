-- ============================================================================
-- Seed Metadata Tables
-- ============================================================================
-- Populates KPI catalog, semantic layer, knowledge graph, and feature backlog.
-- Idempotent: deletes existing rows before inserting.
-- ============================================================================

USE DATABASE RCM_ANALYTICS;
USE SCHEMA METADATA;

-- =========================================================================
-- KPI Catalog (23 metrics)
-- =========================================================================

DELETE FROM KPI_CATALOG;

INSERT INTO KPI_CATALOG (METRIC_NAME, CATEGORY, DEFINITION, FORMULA, DATA_SOURCES, DASHBOARD_TAB, BENCHMARK) VALUES
('Days in A/R (DAR)', 'Financial Performance', 'How many days of charges are sitting unpaid. The single most important cash-flow metric in RCM.', 'A/R Balance / Avg Daily Charges', 'SILVER.CLAIMS.TOTAL_CHARGE_AMOUNT, SILVER.PAYMENTS.PAYMENT_AMOUNT', 'Executive Summary, A/R Aging & Cash Flow', '≤ 35 days'),
('Net Collection Rate (NCR)', 'Financial Performance', 'Percentage of collectible revenue actually collected. Adjustments remove contractually non-collectible amounts.', 'Payments / (Charges − Adjustments) × 100', 'SILVER.PAYMENTS.PAYMENT_AMOUNT, SILVER.CLAIMS.TOTAL_CHARGE_AMOUNT, SILVER.ADJUSTMENTS.ADJUSTMENT_AMOUNT', 'Executive Summary, Collections & Revenue', '≥ 95%'),
('Gross Collection Rate (GCR)', 'Financial Performance', 'Total collections as a percentage of gross charges billed, before adjustments.', 'SUM(payments) / SUM(charges) × 100', 'SILVER.CLAIMS.TOTAL_CHARGE_AMOUNT, SILVER.PAYMENTS.PAYMENT_AMOUNT', 'Executive Summary, Collections & Revenue', '≥ 70%'),
('Cost to Collect', 'Financial Performance', 'RCM operating cost per dollar collected — measures billing department efficiency.', 'Total RCM Cost / Total Collections × 100', 'SILVER.OPERATING_COSTS.TOTAL_RCM_COST, SILVER.PAYMENTS.PAYMENT_AMOUNT', 'Executive Summary, Collections & Revenue', '≤ 3%'),
('Bad Debt Rate', 'Financial Performance', 'Percentage of charges written off as uncollectable bad debt.', 'Bad Debt Write-offs / Total Charges × 100', 'SILVER.ADJUSTMENTS.ADJUSTMENT_TYPE_CODE, SILVER.ADJUSTMENTS.ADJUSTMENT_AMOUNT, SILVER.CLAIMS.TOTAL_CHARGE_AMOUNT', 'Executive Summary', '≤ 3%'),
('Average Reimbursement per Encounter', 'Financial Performance', 'Average payment received per patient encounter — tracks revenue per visit.', 'Total Payments / Number of Encounters', 'SILVER.PAYMENTS.PAYMENT_AMOUNT, SILVER.ENCOUNTERS.ENCOUNTER_ID', 'Executive Summary', NULL),
('Clean Claim Rate', 'Claims Quality', 'Percentage of claims submitted without errors that are accepted on first pass.', 'Clean Claims / Total Claims × 100', 'SILVER.CLAIMS.IS_CLEAN_CLAIM', 'Executive Summary, Claims & Denials', '≥ 90%'),
('Denial Rate', 'Claims Quality', 'Percentage of submitted claims denied by payers.', 'Denied Claims / Total Claims × 100', 'SILVER.CLAIMS.CLAIM_STATUS', 'Executive Summary, Claims & Denials', '≤ 10%'),
('First-Pass Resolution Rate', 'Claims Quality', 'Percentage of claims resolved (paid or denied) on first submission without rework.', 'Claims Resolved on First Pass / Total Claims × 100', 'SILVER.CLAIMS.CLAIM_STATUS, SILVER.CLAIMS.IS_CLEAN_CLAIM', 'Claims & Denials', '≥ 85%'),
('Charge Lag', 'Claims Quality', 'Average days between date of service and claim submission — delays increase A/R.', 'AVG(submission_date − date_of_service) in days', 'SILVER.CLAIMS.SUBMISSION_DATE, SILVER.CLAIMS.DATE_OF_SERVICE', 'Claims & Denials', NULL),
('Denial Reasons', 'Claims Quality', 'Distribution of denial reason codes — identifies root causes for process improvement.', 'COUNT(*) GROUP BY denial_reason_code', 'SILVER.DENIALS.DENIAL_REASON_CODE, SILVER.DENIALS.DENIAL_REASON_DESCRIPTION, SILVER.DENIALS.DENIED_AMOUNT', 'Claims & Denials', NULL),
('Appeal Success Rate', 'Recovery & Appeals', 'Percentage of appealed denials that are successfully overturned.', 'Successful Appeals / Total Appealed Denials × 100', 'SILVER.DENIALS.APPEAL_STATUS', 'Claims & Denials', NULL),
('A/R Aging', 'Recovery & Appeals', 'Dollar value of unpaid claims bucketed by age (0-30, 31-60, 61-90, 91-120, 120+ days).', 'SUM(outstanding_amount) GROUP BY age_bucket', 'SILVER.CLAIMS.DATE_OF_SERVICE, SILVER.CLAIMS.TOTAL_CHARGE_AMOUNT, SILVER.CLAIMS.CLAIM_STATUS, SILVER.PAYMENTS.PAYMENT_AMOUNT', 'A/R Aging & Cash Flow', NULL),
('Payment Accuracy Rate', 'Recovery & Appeals', 'Percentage of payments received that match the contracted reimbursement amount.', 'Accurate Payments / Total Payments × 100', 'SILVER.PAYMENTS.IS_ACCURATE_PAYMENT', 'Executive Summary', '≥ 95%'),
('Payer Mix', 'Segmentation', 'Revenue distribution across payer types (Medicare, Medicaid, Commercial, Self-pay).', 'SUM(payments) GROUP BY payer_type', 'SILVER.PAYMENTS.PAYMENT_AMOUNT, SILVER.PAYERS.PAYER_TYPE, SILVER.CLAIMS.PAYER_ID', 'Payer Analysis', NULL),
('Denial Rate by Payer', 'Segmentation', 'Denial rate broken down per payer — identifies problematic payer relationships.', 'Denied Claims / Total Claims GROUP BY payer_id', 'SILVER.CLAIMS.CLAIM_STATUS, SILVER.CLAIMS.PAYER_ID, SILVER.PAYERS.PAYER_NAME', 'Payer Analysis', NULL),
('Department Performance', 'Segmentation', 'Revenue and encounter volume broken down by clinical department.', 'SUM(payments), COUNT(encounters) GROUP BY department', 'SILVER.ENCOUNTERS.DEPARTMENT, SILVER.PAYMENTS.PAYMENT_AMOUNT, SILVER.CLAIMS.ENCOUNTER_ID', 'Department Performance', NULL),
('Provider Performance', 'Segmentation', 'Collection rate, denial rate, and clean claim rate scored per provider — highlights outliers.', 'SUM(payments)/SUM(charges), Denied/Total, Clean/Total GROUP BY provider_id', 'SILVER.PROVIDERS.PROVIDER_ID, SILVER.CLAIMS.*, SILVER.PAYMENTS.PAYMENT_AMOUNT, SILVER.ENCOUNTERS.PROVIDER_ID', 'Provider Performance', NULL),
('CPT Code Analysis', 'Segmentation', 'Revenue, denial rate, and charge concentration by procedure code — identifies high-value and high-risk CPTs.', 'SUM(charge_amount), COUNT(*), denial_rate GROUP BY cpt_code', 'SILVER.CHARGES.CPT_CODE, SILVER.CHARGES.CHARGE_AMOUNT, SILVER.CLAIMS.CLAIM_STATUS, SILVER.PAYMENTS.PAYMENT_AMOUNT', 'CPT Code Analysis', NULL),
('Underpayment Rate', 'Recovery & Appeals', 'Percentage of payments where the payer paid less than the contracted allowed amount.', 'COUNT(payment_amount < allowed_amount) / COUNT(payments) × 100', 'SILVER.PAYMENTS.ALLOWED_AMOUNT, SILVER.PAYMENTS.PAYMENT_AMOUNT, SILVER.PAYERS.PAYER_NAME', 'Underpayment Analysis', '≤ 5%'),
('Clean Claim Scrubbing Breakdown', 'Claims Quality', 'Distribution of scrubbing fail reasons for dirty claims — root cause analysis.', 'COUNT(*) GROUP BY fail_reason WHERE is_clean_claim = 0', 'SILVER.CLAIMS.FAIL_REASON, SILVER.CLAIMS.IS_CLEAN_CLAIM', 'Claims & Denials', NULL),
('Patient Financial Responsibility', 'Collections', 'Patient-owed portion (co-pay, deductible, coinsurance) by payer, department, and encounter type.', 'SUM(total_charge_amount − payment_amount) GROUP BY payer/dept', 'SILVER.PAYMENTS.ALLOWED_AMOUNT, SILVER.PAYMENTS.PAYMENT_AMOUNT, SILVER.CLAIMS.TOTAL_CHARGE_AMOUNT, SILVER.PAYERS.PAYER_TYPE', 'Patient Responsibility', NULL),
('Data Freshness', 'Operational', 'Last ETL load time, row count, and staleness status per data domain — monitors pipeline health.', 'NOW() − last_loaded_at per domain', 'METADATA.PIPELINE_RUNS.DOMAIN, METADATA.PIPELINE_RUNS.LAST_LOADED_AT, METADATA.PIPELINE_RUNS.ROW_COUNT', 'Executive Summary (sidebar)', NULL);

-- =========================================================================
-- Semantic Layer (21 mappings)
-- =========================================================================

DELETE FROM SEMANTIC_LAYER;

INSERT INTO SEMANTIC_LAYER (BUSINESS_CONCEPT, KPI_NAME, SILVER_COLUMNS, FORMULA, BUSINESS_RULE) VALUES
('Revenue', 'Gross Collection Rate', 'SILVER.CLAIMS.TOTAL_CHARGE_AMOUNT, SILVER.PAYMENTS.PAYMENT_AMOUNT', 'SUM(payments)/SUM(charges)×100', 'Measures total collections vs. gross billed'),
('Revenue', 'Bad Debt Rate', 'SILVER.ADJUSTMENTS.ADJUSTMENT_TYPE_CODE, SILVER.ADJUSTMENTS.ADJUSTMENT_AMOUNT, SILVER.CLAIMS.TOTAL_CHARGE_AMOUNT', 'SUM(bad_debt_adj)/SUM(charges)×100', 'Write-offs where type_code indicates bad debt'),
('Revenue', 'Avg Reimbursement/Encounter', 'SILVER.PAYMENTS.PAYMENT_AMOUNT, SILVER.ENCOUNTERS.ENCOUNTER_ID', 'SUM(payments)/COUNT(encounters)', 'Revenue efficiency per patient visit'),
('Collections', 'Net Collection Rate', 'SILVER.PAYMENTS.PAYMENT_AMOUNT, SILVER.CLAIMS.TOTAL_CHARGE_AMOUNT, SILVER.ADJUSTMENTS.ADJUSTMENT_AMOUNT', 'Payments/(Charges−Adjustments)×100', 'Adjustments remove contractually non-collectible amounts'),
('Collections', 'Cost to Collect', 'SILVER.OPERATING_COSTS.TOTAL_RCM_COST, SILVER.PAYMENTS.PAYMENT_AMOUNT', 'RCM Cost/Collections×100', 'Billing dept efficiency; target <3%'),
('Claims Quality', 'Clean Claim Rate', 'SILVER.CLAIMS.IS_CLEAN_CLAIM', 'SUM(is_clean_claim)/COUNT(claims)×100', 'Claims passing payer edits on first submission'),
('Claims Quality', 'Denial Rate', 'SILVER.CLAIMS.CLAIM_STATUS', 'COUNT(status=''Denied'')/COUNT(claims)×100', 'Industry benchmark <5%'),
('Claims Quality', 'First-Pass Rate', 'SILVER.CLAIMS.CLAIM_STATUS, SILVER.CLAIMS.IS_CLEAN_CLAIM', 'Resolved on first pass/Total×100', 'Resolved = Paid or legitimately Denied w/o rework'),
('Claims Quality', 'Charge Lag', 'SILVER.CLAIMS.SUBMISSION_DATE, SILVER.CLAIMS.DATE_OF_SERVICE', 'AVG(submission_date − date_of_service)', 'Target <3 days; delays increase A/R balance'),
('A/R Health', 'Days in A/R', 'SILVER.CLAIMS.TOTAL_CHARGE_AMOUNT, SILVER.PAYMENTS.PAYMENT_AMOUNT', '(Charges−Payments)/(Monthly Charges/30)', 'Target <40 days; >50 is critical'),
('A/R Health', 'A/R Aging', 'SILVER.CLAIMS.DATE_OF_SERVICE, SILVER.CLAIMS.CLAIM_STATUS, SILVER.PAYMENTS.PAYMENT_AMOUNT', 'Outstanding bucketed by age in days', '90+ day bucket should be <15% of total A/R'),
('A/R Health', 'Payment Accuracy Rate', 'SILVER.PAYMENTS.IS_ACCURATE_PAYMENT', 'SUM(is_accurate_payment)/COUNT×100', 'Inaccurate payments require follow-up with payer'),
('Recovery', 'Appeal Success Rate', 'SILVER.DENIALS.APPEAL_STATUS', 'Successful/Total Appealed×100', 'Target >50%; tracks ability to recover denied revenue'),
('Payer Perf.', 'Payer Mix', 'SILVER.PAYMENTS.PAYMENT_AMOUNT, SILVER.PAYERS.PAYER_TYPE, SILVER.CLAIMS.PAYER_ID', 'SUM(payments) GROUP BY payer_type', 'High self-pay mix → higher collection risk'),
('Payer Perf.', 'Denial Rate by Payer', 'SILVER.CLAIMS.CLAIM_STATUS, SILVER.CLAIMS.PAYER_ID, SILVER.PAYERS.PAYER_NAME', 'Denied/Total GROUP BY payer', 'Identifies payers with problematic contracts/edits'),
('Dept Perf.', 'Department Performance', 'SILVER.ENCOUNTERS.DEPARTMENT, SILVER.PAYMENTS.PAYMENT_AMOUNT, SILVER.CLAIMS.ENCOUNTER_ID', 'SUM(payments), COUNT(encounters) GROUP BY dept', 'Revenue and volume by clinical department'),
('Provider Perf.', 'Provider Performance', 'SILVER.PROVIDERS.PROVIDER_ID, SILVER.ENCOUNTERS.PROVIDER_ID, SILVER.CLAIMS.*, SILVER.PAYMENTS.PAYMENT_AMOUNT', 'SUM(payments)/SUM(charges), Denied/Total GROUP BY provider', 'Identifies high-performing and underperforming providers'),
('Procedure Perf.', 'CPT Code Analysis', 'SILVER.CHARGES.CPT_CODE, SILVER.CHARGES.CHARGE_AMOUNT, SILVER.CLAIMS.CLAIM_STATUS, SILVER.PAYMENTS.PAYMENT_AMOUNT', 'SUM(charge_amount), denial_rate GROUP BY cpt_code', 'High-volume CPTs with high denial rates need coding review'),
('Recovery', 'Underpayment Rate', 'SILVER.PAYMENTS.ALLOWED_AMOUNT, SILVER.PAYMENTS.PAYMENT_AMOUNT, SILVER.PAYERS.PAYER_NAME', 'COUNT(paid < allowed)/COUNT(payments)×100', 'Target ≤5%; systematic underpayment signals contract issues'),
('Claims Quality', 'Clean Claim Breakdown', 'SILVER.CLAIMS.FAIL_REASON, SILVER.CLAIMS.IS_CLEAN_CLAIM', 'COUNT(*) GROUP BY fail_reason WHERE NOT clean', 'Top fail reasons drive scrubbing rule improvements'),
('Patient Resp.', 'Patient Financial Responsibility', 'SILVER.PAYMENTS.ALLOWED_AMOUNT, SILVER.PAYMENTS.PAYMENT_AMOUNT, SILVER.CLAIMS.TOTAL_CHARGE_AMOUNT, SILVER.PAYERS.PAYER_TYPE', 'SUM(charge − payment) GROUP BY payer/dept', 'High patient responsibility with low collection = bad debt risk');

-- =========================================================================
-- Knowledge Graph Nodes (10 entities)
-- =========================================================================

DELETE FROM KG_NODES;

INSERT INTO KG_NODES (ENTITY_ID, ENTITY_NAME, ENTITY_GROUP, SILVER_TABLE, DESCRIPTION, SOURCE_SYSTEM) VALUES
('payers', 'payers', 'Reference', 'SILVER.PAYERS', 'SILVER.PAYERS: PAYER_ID PK, PAYER_NAME, PAYER_TYPE, AVG_REIMBURSEMENT_PCT FLOAT', 'Payer Master'),
('patients', 'patients', 'Reference', 'SILVER.PATIENTS', 'SILVER.PATIENTS: PATIENT_ID PK, PRIMARY_PAYER_ID FK → SILVER.PAYERS', 'EHR'),
('providers', 'providers', 'Reference', 'SILVER.PROVIDERS', 'SILVER.PROVIDERS: PROVIDER_ID PK, DEPARTMENT, SPECIALTY', 'EHR'),
('encounters', 'encounters', 'Transactional', 'SILVER.ENCOUNTERS', 'SILVER.ENCOUNTERS: ENCOUNTER_ID PK, PATIENT_ID FK, PROVIDER_ID FK, DATE_OF_SERVICE, DEPARTMENT, ENCOUNTER_TYPE', 'EHR'),
('claims', 'claims', 'Transactional', 'SILVER.CLAIMS', 'SILVER.CLAIMS: CLAIM_ID PK, ENCOUNTER_ID FK, PATIENT_ID FK, PAYER_ID FK, DATE_OF_SERVICE, SUBMISSION_DATE, TOTAL_CHARGE_AMOUNT FLOAT, CLAIM_STATUS, IS_CLEAN_CLAIM INTEGER', 'Clearinghouse'),
('charges', 'charges', 'Transactional', 'SILVER.CHARGES', 'SILVER.CHARGES: CHARGE_ID PK, ENCOUNTER_ID FK, CHARGE_AMOUNT FLOAT, UNITS INTEGER, SERVICE_DATE, POST_DATE', 'EHR / Charge Capture'),
('payments', 'payments', 'Transactional', 'SILVER.PAYMENTS', 'SILVER.PAYMENTS: PAYMENT_ID PK, CLAIM_ID FK, PAYER_ID FK → SILVER.PAYERS, PAYMENT_AMOUNT FLOAT, ALLOWED_AMOUNT FLOAT, IS_ACCURATE_PAYMENT INTEGER', 'Clearinghouse / ERA'),
('denials', 'denials', 'Transactional', 'SILVER.DENIALS', 'SILVER.DENIALS: DENIAL_ID PK, CLAIM_ID FK, DENIAL_REASON_CODE, DENIED_AMOUNT FLOAT, APPEAL_STATUS, RECOVERED_AMOUNT FLOAT', 'Clearinghouse / ERA'),
('adjustments', 'adjustments', 'Transactional', 'SILVER.ADJUSTMENTS', 'SILVER.ADJUSTMENTS: ADJUSTMENT_ID PK, CLAIM_ID FK, ADJUSTMENT_TYPE_CODE, ADJUSTMENT_AMOUNT FLOAT', 'Billing System'),
('operating_costs', 'operating costs', 'Operational', 'SILVER.OPERATING_COSTS', 'SILVER.OPERATING_COSTS: PERIOD PK, TOTAL_RCM_COST FLOAT', 'ERP / Finance');

-- =========================================================================
-- Knowledge Graph Edges (11 relationships)
-- =========================================================================

DELETE FROM KG_EDGES;

INSERT INTO KG_EDGES (PARENT_ENTITY, CHILD_ENTITY, JOIN_COLUMN, CARDINALITY, BUSINESS_MEANING) VALUES
('payers', 'patients', 'PRIMARY_PAYER_ID', '1:N', 'Each patient has one primary payer'),
('payers', 'claims', 'PAYER_ID', '1:N', 'Claims are billed to one payer'),
('patients', 'encounters', 'PATIENT_ID', '1:N', 'A patient can have many visits'),
('patients', 'claims', 'PATIENT_ID', '1:N', 'Claims track which patient received services'),
('payers', 'payments', 'PAYER_ID', '1:N', 'Payments are remitted by a specific payer'),
('providers', 'encounters', 'PROVIDER_ID', '1:N', 'A provider sees many patients'),
('encounters', 'charges', 'ENCOUNTER_ID', '1:N', 'Each visit generates line-item charges'),
('encounters', 'claims', 'ENCOUNTER_ID', '1:N', 'Each visit produces one or more insurance claims'),
('claims', 'payments', 'CLAIM_ID', '1:N', 'A claim may receive partial or split payments'),
('claims', 'denials', 'CLAIM_ID', '1:N', 'A claim can be denied once or multiple times'),
('claims', 'adjustments', 'CLAIM_ID', '1:N', 'Contractual write-offs are applied per claim');

-- =========================================================================
-- Feature Backlog (3 seed examples)
-- =========================================================================

INSERT INTO FEATURE_BACKLOG (TITLE, DESCRIPTION, PRIORITY, ACCEPTANCE_CRITERIA, BENEFITS, STATUS)
SELECT 'Automated Email Alerts for KPI Threshold Breaches',
       'Send automated email notifications when KPIs exceed configurable thresholds.',
       'High',
       '1. Admin can configure per-KPI thresholds\n2. Emails sent within 5 minutes of breach\n3. Includes KPI name, value, threshold, and link\n4. Duplicate alerts suppressed for 24 hours',
       'Proactive issue detection reduces time-to-action on revenue cycle problems.',
       'Not Started'
WHERE NOT EXISTS (SELECT 1 FROM FEATURE_BACKLOG WHERE TITLE = 'Automated Email Alerts for KPI Threshold Breaches');

INSERT INTO FEATURE_BACKLOG (TITLE, DESCRIPTION, PRIORITY, ACCEPTANCE_CRITERIA, BENEFITS, STATUS)
SELECT 'Payer Contract Rate Comparison',
       'Dashboard tab comparing actual reimbursement rates against contracted rates by payer.',
       'Medium',
       '1. Upload contracted rates per CPT per payer\n2. Expected vs actual side-by-side\n3. Underpayment flag with dollar amount\n4. Summary: total underpayment by payer',
       'Identifies 2-5% revenue leakage from undetected payer underpayments.',
       'Not Started'
WHERE NOT EXISTS (SELECT 1 FROM FEATURE_BACKLOG WHERE TITLE = 'Payer Contract Rate Comparison');

INSERT INTO FEATURE_BACKLOG (TITLE, DESCRIPTION, PRIORITY, ACCEPTANCE_CRITERIA, BENEFITS, STATUS)
SELECT 'Historical Trend Forecasting with ML',
       'Time-series ML models to forecast A/R, denial rates, and revenue 3-6 months ahead.',
       'Low',
       '1. Forecast for 4+ core KPIs\n2. Confidence intervals on charts\n3. Auto-update on new data\n4. Accuracy metrics (MAPE, MAE)',
       'Enables proactive financial planning and early warning of negative trends.',
       'Not Started'
WHERE NOT EXISTS (SELECT 1 FROM FEATURE_BACKLOG WHERE TITLE = 'Historical Trend Forecasting with ML');
