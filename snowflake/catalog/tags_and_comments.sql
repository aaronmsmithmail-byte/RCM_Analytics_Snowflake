-- ============================================================================
-- Horizon Data Catalog: Tags, Comments, and Classification
-- ============================================================================
-- Comprehensive Snowflake Horizon catalog for the RCM_ANALYTICS database.
-- Applies tags, table comments, and column-level descriptions so every object
-- is discoverable in Snowsight's data catalog — accessible to all users,
-- not just Streamlit app users.
--
-- Run after DDL scripts:
--   EXECUTE IMMEDIATE FROM @RCM_ANALYTICS.STAGING.RCM_REPO/branches/main/snowflake/catalog/tags_and_comments.sql;
-- ============================================================================

USE DATABASE RCM_ANALYTICS;
USE SCHEMA METADATA;

-- =========================================================================
-- 1. CREATE TAGS
-- =========================================================================

CREATE TAG IF NOT EXISTS DATA_LAYER
    ALLOWED_VALUES 'bronze', 'silver', 'gold', 'metadata'
    COMMENT = 'Medallion architecture layer (bronze/silver/gold/metadata)';

CREATE TAG IF NOT EXISTS DATA_DOMAIN
    ALLOWED_VALUES 'claims', 'payments', 'denials', 'encounters', 'charges',
                   'adjustments', 'payers', 'patients', 'providers', 'operations',
                   'kpi', 'semantic', 'knowledge_graph', 'pipeline'
    COMMENT = 'Business domain classification for RCM data';

CREATE TAG IF NOT EXISTS SENSITIVITY
    ALLOWED_VALUES 'PHI', 'PII', 'public'
    COMMENT = 'Data sensitivity classification (PHI/PII/public)';

-- =========================================================================
-- 2. DATABASE & SCHEMA COMMENTS
-- =========================================================================

COMMENT ON DATABASE RCM_ANALYTICS IS
    'Healthcare Revenue Cycle Management Analytics — Medallion architecture '
    '(Bronze -> Silver -> Gold) with 10 data entities, 23 KPIs, and Cortex Analyst integration.';

COMMENT ON SCHEMA RCM_ANALYTICS.BRONZE IS
    'Raw ingestion layer — all VARCHAR columns loaded from staged CSV files via COPY INTO.';

COMMENT ON SCHEMA RCM_ANALYTICS.SILVER IS
    'Typed and FK-constrained layer — cleaned data with proper data types and referential integrity.';

COMMENT ON SCHEMA RCM_ANALYTICS.GOLD IS
    'Business-ready aggregation layer — pre-joined SQL views for KPI dashboards and reporting.';

COMMENT ON SCHEMA RCM_ANALYTICS.METADATA IS
    'Catalog and governance — KPI definitions, semantic layer mappings, knowledge graph, and pipeline tracking.';

COMMENT ON SCHEMA RCM_ANALYTICS.STAGING IS
    'Internal stage, file formats, stored procedures, and Git repository integration.';

-- =========================================================================
-- 3. BRONZE LAYER — Table Tags & Comments
-- =========================================================================

ALTER TABLE BRONZE.PAYERS SET TAG DATA_LAYER = 'bronze', DATA_DOMAIN = 'payers';
COMMENT ON TABLE BRONZE.PAYERS IS
    'Raw CSV ingestion — insurance payer master list (10 payers). '
    'Source: Payer Master / Credentialing system.';

ALTER TABLE BRONZE.PATIENTS SET TAG DATA_LAYER = 'bronze', DATA_DOMAIN = 'patients';
COMMENT ON TABLE BRONZE.PATIENTS IS
    'Raw CSV ingestion — patient demographics and payer assignment. '
    'Source: EHR (Electronic Health Record). Contains PII.';

ALTER TABLE BRONZE.PROVIDERS SET TAG DATA_LAYER = 'bronze', DATA_DOMAIN = 'providers';
COMMENT ON TABLE BRONZE.PROVIDERS IS
    'Raw CSV ingestion — clinician roster with department and specialty. '
    'Source: EHR / Credentialing system.';

ALTER TABLE BRONZE.ENCOUNTERS SET TAG DATA_LAYER = 'bronze', DATA_DOMAIN = 'encounters';
COMMENT ON TABLE BRONZE.ENCOUNTERS IS
    'Raw CSV ingestion — individual patient visits (outpatient, ED, inpatient, telehealth). '
    'Source: EHR scheduling/registration.';

ALTER TABLE BRONZE.CHARGES SET TAG DATA_LAYER = 'bronze', DATA_DOMAIN = 'charges';
COMMENT ON TABLE BRONZE.CHARGES IS
    'Raw CSV ingestion — line-item charges per encounter with CPT/ICD-10 codes. '
    'Source: EHR charge capture / coding.';

ALTER TABLE BRONZE.CLAIMS SET TAG DATA_LAYER = 'bronze', DATA_DOMAIN = 'claims';
COMMENT ON TABLE BRONZE.CLAIMS IS
    'Raw CSV ingestion — insurance claims submitted for payment. '
    'Source: Clearinghouse (ANSI 837 transactions).';

ALTER TABLE BRONZE.PAYMENTS SET TAG DATA_LAYER = 'bronze', DATA_DOMAIN = 'payments';
COMMENT ON TABLE BRONZE.PAYMENTS IS
    'Raw CSV ingestion — payments received against claims (payer + patient). '
    'Source: Clearinghouse / ERA (ANSI 835 transactions).';

ALTER TABLE BRONZE.DENIALS SET TAG DATA_LAYER = 'bronze', DATA_DOMAIN = 'denials';
COMMENT ON TABLE BRONZE.DENIALS IS
    'Raw CSV ingestion — claim denials with reason codes and appeal outcomes. '
    'Source: Clearinghouse / ERA (ANSI 835 transactions).';

ALTER TABLE BRONZE.ADJUSTMENTS SET TAG DATA_LAYER = 'bronze', DATA_DOMAIN = 'adjustments';
COMMENT ON TABLE BRONZE.ADJUSTMENTS IS
    'Raw CSV ingestion — contractual write-offs and financial adjustments. '
    'Source: Billing system.';

ALTER TABLE BRONZE.OPERATING_COSTS SET TAG DATA_LAYER = 'bronze', DATA_DOMAIN = 'operations';
COMMENT ON TABLE BRONZE.OPERATING_COSTS IS
    'Raw CSV ingestion — monthly RCM department operating costs (staff, software, outsourcing). '
    'Source: ERP / Finance general ledger.';

-- =========================================================================
-- 4. SILVER LAYER — Table Tags & Comments
-- =========================================================================

-- ── Reference Tables ────────────────────────────────────────────────────

ALTER TABLE SILVER.PAYERS SET TAG DATA_LAYER = 'silver', DATA_DOMAIN = 'payers';
COMMENT ON TABLE SILVER.PAYERS IS
    'Insurance payer master — 10 payers with contract terms. '
    'FK target for CLAIMS.PAYER_ID and PATIENTS.PRIMARY_PAYER_ID.';

COMMENT ON COLUMN SILVER.PAYERS.PAYER_ID IS 'Unique payer identifier (e.g. PAY001). Primary key.';
COMMENT ON COLUMN SILVER.PAYERS.PAYER_NAME IS 'Full payer name (e.g. Blue Cross Blue Shield).';
COMMENT ON COLUMN SILVER.PAYERS.PAYER_TYPE IS 'Payer category: Commercial, Medicare, Medicaid, or Self-Pay.';
COMMENT ON COLUMN SILVER.PAYERS.AVG_REIMBURSEMENT_PCT IS 'Average reimbursement as % of billed charges per contract.';
COMMENT ON COLUMN SILVER.PAYERS.CONTRACT_ID IS 'Reference to the payer contract on file.';

ALTER TABLE SILVER.PATIENTS SET TAG DATA_LAYER = 'silver', DATA_DOMAIN = 'patients';
COMMENT ON TABLE SILVER.PATIENTS IS
    'Patient demographics and primary payer assignment. Contains PII. '
    'FK target for ENCOUNTERS.PATIENT_ID and CLAIMS.PATIENT_ID.';

COMMENT ON COLUMN SILVER.PATIENTS.PATIENT_ID IS 'Unique patient identifier. Primary key.';
COMMENT ON COLUMN SILVER.PATIENTS.FIRST_NAME IS 'Patient first name. PII — access restricted.';
COMMENT ON COLUMN SILVER.PATIENTS.LAST_NAME IS 'Patient last name. PII — access restricted.';
COMMENT ON COLUMN SILVER.PATIENTS.DATE_OF_BIRTH IS 'Patient date of birth (YYYY-MM-DD). PII.';
COMMENT ON COLUMN SILVER.PATIENTS.GENDER IS 'Patient gender (M/F/Other).';
COMMENT ON COLUMN SILVER.PATIENTS.PRIMARY_PAYER_ID IS 'FK -> SILVER.PAYERS. Primary insurance payer.';
COMMENT ON COLUMN SILVER.PATIENTS.MEMBER_ID IS 'Insurance member/subscriber ID. PII.';
COMMENT ON COLUMN SILVER.PATIENTS.ZIP_CODE IS 'Patient ZIP code. PII — geographic identifier.';

ALTER TABLE SILVER.PROVIDERS SET TAG DATA_LAYER = 'silver', DATA_DOMAIN = 'providers';
COMMENT ON TABLE SILVER.PROVIDERS IS
    'Clinician roster with department and specialty. '
    'FK target for ENCOUNTERS.PROVIDER_ID.';

COMMENT ON COLUMN SILVER.PROVIDERS.PROVIDER_ID IS 'Unique provider identifier. Primary key.';
COMMENT ON COLUMN SILVER.PROVIDERS.PROVIDER_NAME IS 'Full name of the clinician.';
COMMENT ON COLUMN SILVER.PROVIDERS.NPI IS 'National Provider Identifier (10-digit CMS number).';
COMMENT ON COLUMN SILVER.PROVIDERS.DEPARTMENT IS 'Clinical department (e.g. Cardiology, Emergency Medicine).';
COMMENT ON COLUMN SILVER.PROVIDERS.SPECIALTY IS 'Medical specialty (e.g. Internal Medicine, Orthopedics).';

-- ── Transactional Tables ────────────────────────────────────────────────

ALTER TABLE SILVER.ENCOUNTERS SET TAG DATA_LAYER = 'silver', DATA_DOMAIN = 'encounters';
COMMENT ON TABLE SILVER.ENCOUNTERS IS
    'Individual patient visits — the starting point of the revenue cycle. '
    'Each encounter generates charges and claims. FK: PATIENT_ID, PROVIDER_ID.';

COMMENT ON COLUMN SILVER.ENCOUNTERS.ENCOUNTER_ID IS 'Unique visit identifier. Primary key.';
COMMENT ON COLUMN SILVER.ENCOUNTERS.PATIENT_ID IS 'FK -> SILVER.PATIENTS. The patient seen.';
COMMENT ON COLUMN SILVER.ENCOUNTERS.PROVIDER_ID IS 'FK -> SILVER.PROVIDERS. The clinician who provided care.';
COMMENT ON COLUMN SILVER.ENCOUNTERS.DATE_OF_SERVICE IS 'Date the healthcare service was rendered (YYYY-MM-DD).';
COMMENT ON COLUMN SILVER.ENCOUNTERS.DISCHARGE_DATE IS 'Discharge date for inpatient stays (YYYY-MM-DD). NULL for outpatient.';
COMMENT ON COLUMN SILVER.ENCOUNTERS.ENCOUNTER_TYPE IS 'Visit type: Outpatient, Emergency, Inpatient, or Telehealth.';
COMMENT ON COLUMN SILVER.ENCOUNTERS.DEPARTMENT IS 'Clinical department where the encounter occurred.';

ALTER TABLE SILVER.CHARGES SET TAG DATA_LAYER = 'silver', DATA_DOMAIN = 'charges';
COMMENT ON TABLE SILVER.CHARGES IS
    'Line-item charges per encounter with CPT procedure codes and ICD-10 diagnoses. '
    'One encounter may have multiple charge lines. FK: ENCOUNTER_ID.';

COMMENT ON COLUMN SILVER.CHARGES.CHARGE_ID IS 'Unique charge line identifier. Primary key.';
COMMENT ON COLUMN SILVER.CHARGES.ENCOUNTER_ID IS 'FK -> SILVER.ENCOUNTERS. The visit that generated this charge.';
COMMENT ON COLUMN SILVER.CHARGES.CPT_CODE IS 'Current Procedural Terminology code (e.g. 99213 for office visit).';
COMMENT ON COLUMN SILVER.CHARGES.CPT_DESCRIPTION IS 'Human-readable description of the CPT procedure.';
COMMENT ON COLUMN SILVER.CHARGES.UNITS IS 'Number of units billed (default 1).';
COMMENT ON COLUMN SILVER.CHARGES.CHARGE_AMOUNT IS 'Billed amount for this charge line in USD.';
COMMENT ON COLUMN SILVER.CHARGES.SERVICE_DATE IS 'Date the service was performed (YYYY-MM-DD).';
COMMENT ON COLUMN SILVER.CHARGES.POST_DATE IS 'Date the charge was posted to the billing system (YYYY-MM-DD).';
COMMENT ON COLUMN SILVER.CHARGES.ICD10_CODE IS 'ICD-10 diagnosis code associated with this charge.';

ALTER TABLE SILVER.CLAIMS SET TAG DATA_LAYER = 'silver', DATA_DOMAIN = 'claims';
COMMENT ON TABLE SILVER.CLAIMS IS
    'Insurance claims — the central table of the revenue cycle. Each claim is a formal '
    'request for payment to a payer. FK: ENCOUNTER_ID, PATIENT_ID, PAYER_ID.';

COMMENT ON COLUMN SILVER.CLAIMS.CLAIM_ID IS 'Unique claim identifier. Primary key.';
COMMENT ON COLUMN SILVER.CLAIMS.ENCOUNTER_ID IS 'FK -> SILVER.ENCOUNTERS. The visit this claim covers.';
COMMENT ON COLUMN SILVER.CLAIMS.PATIENT_ID IS 'FK -> SILVER.PATIENTS. The patient whose services are being billed.';
COMMENT ON COLUMN SILVER.CLAIMS.PAYER_ID IS 'FK -> SILVER.PAYERS. The insurance company being billed.';
COMMENT ON COLUMN SILVER.CLAIMS.DATE_OF_SERVICE IS 'Date the healthcare service was rendered (YYYY-MM-DD).';
COMMENT ON COLUMN SILVER.CLAIMS.SUBMISSION_DATE IS 'Date the claim was submitted to the payer (YYYY-MM-DD).';
COMMENT ON COLUMN SILVER.CLAIMS.TOTAL_CHARGE_AMOUNT IS 'Total billed amount for the claim in USD.';
COMMENT ON COLUMN SILVER.CLAIMS.CLAIM_STATUS IS 'Current adjudication status: Paid, Partially Paid, Denied, Pending, or Appealed.';
COMMENT ON COLUMN SILVER.CLAIMS.IS_CLEAN_CLAIM IS 'Boolean (1/0): claim passed all payer edits on first submission without errors.';
COMMENT ON COLUMN SILVER.CLAIMS.SUBMISSION_METHOD IS 'How the claim was submitted (Electronic).';
COMMENT ON COLUMN SILVER.CLAIMS.FAIL_REASON IS 'Scrubbing fail reason for dirty claims: MISSING_AUTH, ELIGIBILITY_FAIL, CODING_ERROR, DUPLICATE_SUBMISSION, TIMELY_FILING, MISSING_INFO. NULL for clean claims.';

ALTER TABLE SILVER.PAYMENTS SET TAG DATA_LAYER = 'silver', DATA_DOMAIN = 'payments';
COMMENT ON TABLE SILVER.PAYMENTS IS
    'Payer remittances (ERA/835) — payments received against claims. One claim may have '
    'multiple payments (payer portion + patient responsibility). FK: CLAIM_ID, PAYER_ID.';

COMMENT ON COLUMN SILVER.PAYMENTS.PAYMENT_ID IS 'Unique payment identifier. Primary key.';
COMMENT ON COLUMN SILVER.PAYMENTS.CLAIM_ID IS 'FK -> SILVER.CLAIMS. The claim this payment applies to.';
COMMENT ON COLUMN SILVER.PAYMENTS.PAYER_ID IS 'FK -> SILVER.PAYERS. The payer who sent remittance. "PATIENT" for patient payments.';
COMMENT ON COLUMN SILVER.PAYMENTS.PAYMENT_AMOUNT IS 'Amount paid by the payer for this claim in USD.';
COMMENT ON COLUMN SILVER.PAYMENTS.ALLOWED_AMOUNT IS 'Contracted allowed amount per payer agreement in USD.';
COMMENT ON COLUMN SILVER.PAYMENTS.PAYMENT_DATE IS 'Date payment was received (YYYY-MM-DD).';
COMMENT ON COLUMN SILVER.PAYMENTS.PAYMENT_METHOD IS 'Payment method: EFT, Check, Virtual Card, Credit Card, Cash, Online Portal.';
COMMENT ON COLUMN SILVER.PAYMENTS.IS_ACCURATE_PAYMENT IS 'Boolean (1/0): payment matches contracted reimbursement rate. 0 = underpayment.';

ALTER TABLE SILVER.DENIALS SET TAG DATA_LAYER = 'silver', DATA_DOMAIN = 'denials';
COMMENT ON TABLE SILVER.DENIALS IS
    'Claim denials with standardized reason codes and appeal outcomes. '
    'Used for denial rate analysis and recovery tracking. FK: CLAIM_ID.';

COMMENT ON COLUMN SILVER.DENIALS.DENIAL_ID IS 'Unique denial record identifier. Primary key.';
COMMENT ON COLUMN SILVER.DENIALS.CLAIM_ID IS 'FK -> SILVER.CLAIMS. The claim that was denied.';
COMMENT ON COLUMN SILVER.DENIALS.DENIAL_REASON_CODE IS 'Standardized denial reason code (e.g. AUTH, ELIG, COB, CODING, TIMELY, DUP, MEDICAL).';
COMMENT ON COLUMN SILVER.DENIALS.DENIAL_REASON_DESCRIPTION IS 'Human-readable denial reason (e.g. Prior Authorization Required).';
COMMENT ON COLUMN SILVER.DENIALS.DENIAL_DATE IS 'Date the denial was received (YYYY-MM-DD).';
COMMENT ON COLUMN SILVER.DENIALS.DENIED_AMOUNT IS 'Dollar amount denied by the payer in USD.';
COMMENT ON COLUMN SILVER.DENIALS.APPEAL_STATUS IS 'Appeal outcome: Not Appealed, Won, Lost, or In Progress.';
COMMENT ON COLUMN SILVER.DENIALS.APPEAL_DATE IS 'Date the appeal was filed (YYYY-MM-DD). NULL if not appealed.';
COMMENT ON COLUMN SILVER.DENIALS.RECOVERED_AMOUNT IS 'Amount recovered through a successful appeal in USD. 0 if appeal lost or not filed.';

ALTER TABLE SILVER.ADJUSTMENTS SET TAG DATA_LAYER = 'silver', DATA_DOMAIN = 'adjustments';
COMMENT ON TABLE SILVER.ADJUSTMENTS IS
    'Financial adjustments — contractual write-offs, bad debt, charity care, and admin corrections. '
    'Essential for Net Collection Rate calculation. FK: CLAIM_ID.';

COMMENT ON COLUMN SILVER.ADJUSTMENTS.ADJUSTMENT_ID IS 'Unique adjustment identifier. Primary key.';
COMMENT ON COLUMN SILVER.ADJUSTMENTS.CLAIM_ID IS 'FK -> SILVER.CLAIMS. The claim this adjustment applies to.';
COMMENT ON COLUMN SILVER.ADJUSTMENTS.ADJUSTMENT_TYPE_CODE IS 'Adjustment category: CONTRACTUAL, WRITEOFF, ADMIN, PROMPT_PAY, CHARITY, or SMALL_BAL.';
COMMENT ON COLUMN SILVER.ADJUSTMENTS.ADJUSTMENT_TYPE_DESCRIPTION IS 'Human-readable adjustment type description.';
COMMENT ON COLUMN SILVER.ADJUSTMENTS.ADJUSTMENT_AMOUNT IS 'Adjustment amount in USD.';
COMMENT ON COLUMN SILVER.ADJUSTMENTS.ADJUSTMENT_DATE IS 'Date the adjustment was posted (YYYY-MM-DD).';

ALTER TABLE SILVER.OPERATING_COSTS SET TAG DATA_LAYER = 'silver', DATA_DOMAIN = 'operations';
COMMENT ON TABLE SILVER.OPERATING_COSTS IS
    'Monthly RCM department operating costs — feeds the Cost to Collect KPI. '
    'Source: ERP / Finance general ledger.';

COMMENT ON COLUMN SILVER.OPERATING_COSTS.PERIOD IS 'Month in YYYY-MM format. Primary key.';
COMMENT ON COLUMN SILVER.OPERATING_COSTS.BILLING_STAFF_COST IS 'Monthly billing staff salaries in USD ($35K-55K typical).';
COMMENT ON COLUMN SILVER.OPERATING_COSTS.SOFTWARE_COST IS 'Monthly EHR/billing/clearinghouse software costs in USD.';
COMMENT ON COLUMN SILVER.OPERATING_COSTS.OUTSOURCING_COST IS 'Monthly third-party billing services cost in USD.';
COMMENT ON COLUMN SILVER.OPERATING_COSTS.SUPPLIES_OVERHEAD IS 'Monthly supplies and overhead (printing, postage, office) in USD.';
COMMENT ON COLUMN SILVER.OPERATING_COSTS.TOTAL_RCM_COST IS 'Total monthly RCM department cost in USD. Sum of all cost components.';

-- =========================================================================
-- 5. GOLD LAYER — View Tags & Comments
-- =========================================================================

ALTER VIEW GOLD.MONTHLY_KPIS SET TAG DATA_LAYER = 'gold', DATA_DOMAIN = 'kpi';
COMMENT ON VIEW GOLD.MONTHLY_KPIS IS
    'Monthly KPI rollup — claim counts, charges, payments, clean claim rate, denial rate, '
    'and gross collection rate aggregated by month. Source: SILVER.CLAIMS + SILVER.PAYMENTS.';

ALTER VIEW GOLD.PAYER_PERFORMANCE SET TAG DATA_LAYER = 'gold', DATA_DOMAIN = 'payers';
COMMENT ON VIEW GOLD.PAYER_PERFORMANCE IS
    'Per-payer performance — total claims, charges, payments, denial rate, and GCR by payer. '
    'Source: SILVER.PAYERS + SILVER.CLAIMS + SILVER.PAYMENTS.';

ALTER VIEW GOLD.DEPARTMENT_PERFORMANCE SET TAG DATA_LAYER = 'gold', DATA_DOMAIN = 'encounters';
COMMENT ON VIEW GOLD.DEPARTMENT_PERFORMANCE IS
    'Per-department performance — encounters, charges, payments, and revenue per encounter. '
    'Source: SILVER.ENCOUNTERS + SILVER.CHARGES + SILVER.CLAIMS + SILVER.PAYMENTS.';

ALTER VIEW GOLD.AR_AGING SET TAG DATA_LAYER = 'gold', DATA_DOMAIN = 'claims';
COMMENT ON VIEW GOLD.AR_AGING IS
    'Accounts receivable aging buckets (0-30, 31-60, 61-90, 91-120, 120+ days). '
    'Shows claim count, total billed, collected, and outstanding balance per bucket. '
    'Source: SILVER.CLAIMS + SILVER.PAYMENTS. Excludes Paid claims.';

ALTER VIEW GOLD.DENIAL_ANALYSIS SET TAG DATA_LAYER = 'gold', DATA_DOMAIN = 'denials';
COMMENT ON VIEW GOLD.DENIAL_ANALYSIS IS
    'Denial reason analysis — count, total denied, recovered, and appeal success rate by reason code. '
    'Source: SILVER.DENIALS.';

-- =========================================================================
-- 6. METADATA LAYER — Table Tags & Comments
-- =========================================================================

ALTER TABLE METADATA.KPI_CATALOG SET TAG DATA_LAYER = 'metadata', DATA_DOMAIN = 'kpi';
COMMENT ON TABLE METADATA.KPI_CATALOG IS
    'Single source of truth for all 23 RCM KPI definitions including formulas, '
    'data sources, industry benchmarks, and categories.';

ALTER TABLE METADATA.SEMANTIC_LAYER SET TAG DATA_LAYER = 'metadata', DATA_DOMAIN = 'semantic';
COMMENT ON TABLE METADATA.SEMANTIC_LAYER IS
    'Business concept -> KPI -> Silver column mappings. 21 mappings used by Cortex Analyst '
    'to translate natural language questions into SQL.';

ALTER TABLE METADATA.KG_NODES SET TAG DATA_LAYER = 'metadata', DATA_DOMAIN = 'knowledge_graph';
COMMENT ON TABLE METADATA.KG_NODES IS
    'Knowledge graph entity nodes — 10 Silver table entities with descriptions and source systems.';

ALTER TABLE METADATA.KG_EDGES SET TAG DATA_LAYER = 'metadata', DATA_DOMAIN = 'knowledge_graph';
COMMENT ON TABLE METADATA.KG_EDGES IS
    'Knowledge graph FK relationships — 11 edges defining how entities connect (1:N cardinalities).';

ALTER TABLE METADATA.PIPELINE_RUNS SET TAG DATA_LAYER = 'metadata', DATA_DOMAIN = 'pipeline';
COMMENT ON TABLE METADATA.PIPELINE_RUNS IS
    'ETL pipeline tracking — last load timestamp, row count, and source file per data domain.';

-- =========================================================================
-- 7. PII/PHI COLUMN-LEVEL SENSITIVITY TAGS
-- =========================================================================

-- Silver patient PII
ALTER TABLE SILVER.PATIENTS MODIFY COLUMN FIRST_NAME SET TAG SENSITIVITY = 'PII';
ALTER TABLE SILVER.PATIENTS MODIFY COLUMN LAST_NAME SET TAG SENSITIVITY = 'PII';
ALTER TABLE SILVER.PATIENTS MODIFY COLUMN DATE_OF_BIRTH SET TAG SENSITIVITY = 'PII';
ALTER TABLE SILVER.PATIENTS MODIFY COLUMN ZIP_CODE SET TAG SENSITIVITY = 'PII';
ALTER TABLE SILVER.PATIENTS MODIFY COLUMN MEMBER_ID SET TAG SENSITIVITY = 'PII';

-- Bronze patient PII (same columns, raw layer)
ALTER TABLE BRONZE.PATIENTS MODIFY COLUMN FIRST_NAME SET TAG SENSITIVITY = 'PII';
ALTER TABLE BRONZE.PATIENTS MODIFY COLUMN LAST_NAME SET TAG SENSITIVITY = 'PII';
ALTER TABLE BRONZE.PATIENTS MODIFY COLUMN DATE_OF_BIRTH SET TAG SENSITIVITY = 'PII';
ALTER TABLE BRONZE.PATIENTS MODIFY COLUMN ZIP_CODE SET TAG SENSITIVITY = 'PII';
ALTER TABLE BRONZE.PATIENTS MODIFY COLUMN MEMBER_ID SET TAG SENSITIVITY = 'PII';

-- Provider NPI (sensitive but not PII)
ALTER TABLE SILVER.PROVIDERS MODIFY COLUMN NPI SET TAG SENSITIVITY = 'public';
