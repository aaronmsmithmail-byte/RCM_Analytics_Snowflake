-- ============================================================================
-- Horizon Data Catalog: Tags, Comments, and Classification
-- ============================================================================
-- Applies Snowflake tags and object comments so all tables appear in the
-- Snowsight Horizon data catalog with proper classification.
-- ============================================================================

USE DATABASE RCM_ANALYTICS;
USE SCHEMA METADATA;

-- =========================================================================
-- Create Tags
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
-- Bronze Layer Tags & Comments
-- =========================================================================

ALTER TABLE BRONZE.PAYERS SET TAG DATA_LAYER = 'bronze', DATA_DOMAIN = 'payers';
COMMENT ON TABLE BRONZE.PAYERS IS 'Raw CSV ingestion — insurance payer master list';

ALTER TABLE BRONZE.PATIENTS SET TAG DATA_LAYER = 'bronze', DATA_DOMAIN = 'patients';
COMMENT ON TABLE BRONZE.PATIENTS IS 'Raw CSV ingestion — patient demographics';

ALTER TABLE BRONZE.PROVIDERS SET TAG DATA_LAYER = 'bronze', DATA_DOMAIN = 'providers';
COMMENT ON TABLE BRONZE.PROVIDERS IS 'Raw CSV ingestion — clinician roster';

ALTER TABLE BRONZE.ENCOUNTERS SET TAG DATA_LAYER = 'bronze', DATA_DOMAIN = 'encounters';
COMMENT ON TABLE BRONZE.ENCOUNTERS IS 'Raw CSV ingestion — individual patient visits';

ALTER TABLE BRONZE.CHARGES SET TAG DATA_LAYER = 'bronze', DATA_DOMAIN = 'charges';
COMMENT ON TABLE BRONZE.CHARGES IS 'Raw CSV ingestion — line-item charges per encounter';

ALTER TABLE BRONZE.CLAIMS SET TAG DATA_LAYER = 'bronze', DATA_DOMAIN = 'claims';
COMMENT ON TABLE BRONZE.CLAIMS IS 'Raw CSV ingestion — insurance claims submitted for payment';

ALTER TABLE BRONZE.PAYMENTS SET TAG DATA_LAYER = 'bronze', DATA_DOMAIN = 'payments';
COMMENT ON TABLE BRONZE.PAYMENTS IS 'Raw CSV ingestion — payments received against claims';

ALTER TABLE BRONZE.DENIALS SET TAG DATA_LAYER = 'bronze', DATA_DOMAIN = 'denials';
COMMENT ON TABLE BRONZE.DENIALS IS 'Raw CSV ingestion — claim denials with reason codes';

ALTER TABLE BRONZE.ADJUSTMENTS SET TAG DATA_LAYER = 'bronze', DATA_DOMAIN = 'adjustments';
COMMENT ON TABLE BRONZE.ADJUSTMENTS IS 'Raw CSV ingestion — contractual write-offs and adjustments';

ALTER TABLE BRONZE.OPERATING_COSTS SET TAG DATA_LAYER = 'bronze', DATA_DOMAIN = 'operations';
COMMENT ON TABLE BRONZE.OPERATING_COSTS IS 'Raw CSV ingestion — monthly RCM department operating costs';

-- =========================================================================
-- Silver Layer Tags & Comments
-- =========================================================================

ALTER TABLE SILVER.PAYERS SET TAG DATA_LAYER = 'silver', DATA_DOMAIN = 'payers';
COMMENT ON TABLE SILVER.PAYERS IS 'Typed & FK-constrained — insurance payer master list';

ALTER TABLE SILVER.PATIENTS SET TAG DATA_LAYER = 'silver', DATA_DOMAIN = 'patients';
COMMENT ON TABLE SILVER.PATIENTS IS 'Typed & FK-constrained — patient demographics and primary payer';

ALTER TABLE SILVER.PROVIDERS SET TAG DATA_LAYER = 'silver', DATA_DOMAIN = 'providers';
COMMENT ON TABLE SILVER.PROVIDERS IS 'Typed & FK-constrained — clinician roster with department';

ALTER TABLE SILVER.ENCOUNTERS SET TAG DATA_LAYER = 'silver', DATA_DOMAIN = 'encounters';
COMMENT ON TABLE SILVER.ENCOUNTERS IS 'Typed & FK-constrained — individual patient visits with provider and department';

ALTER TABLE SILVER.CHARGES SET TAG DATA_LAYER = 'silver', DATA_DOMAIN = 'charges';
COMMENT ON TABLE SILVER.CHARGES IS 'Typed & FK-constrained — line-item charges per encounter with CPT codes';

ALTER TABLE SILVER.CLAIMS SET TAG DATA_LAYER = 'silver', DATA_DOMAIN = 'claims';
COMMENT ON TABLE SILVER.CLAIMS IS 'Typed & FK-constrained — insurance claims with status, charges, and clean claim flag';

ALTER TABLE SILVER.PAYMENTS SET TAG DATA_LAYER = 'silver', DATA_DOMAIN = 'payments';
COMMENT ON TABLE SILVER.PAYMENTS IS 'Typed & FK-constrained — payer remittances with payment accuracy tracking';

ALTER TABLE SILVER.DENIALS SET TAG DATA_LAYER = 'silver', DATA_DOMAIN = 'denials';
COMMENT ON TABLE SILVER.DENIALS IS 'Typed & FK-constrained — claim denials with reason codes and appeal outcomes';

ALTER TABLE SILVER.ADJUSTMENTS SET TAG DATA_LAYER = 'silver', DATA_DOMAIN = 'adjustments';
COMMENT ON TABLE SILVER.ADJUSTMENTS IS 'Typed & FK-constrained — contractual write-offs and financial adjustments';

ALTER TABLE SILVER.OPERATING_COSTS SET TAG DATA_LAYER = 'silver', DATA_DOMAIN = 'operations';
COMMENT ON TABLE SILVER.OPERATING_COSTS IS 'Typed — monthly RCM department operating costs for cost-to-collect KPI';

-- =========================================================================
-- Metadata Layer Tags
-- =========================================================================

ALTER TABLE METADATA.KPI_CATALOG SET TAG DATA_LAYER = 'metadata', DATA_DOMAIN = 'kpi';
COMMENT ON TABLE METADATA.KPI_CATALOG IS 'KPI definitions with formulas, data sources, and industry benchmarks';

ALTER TABLE METADATA.SEMANTIC_LAYER SET TAG DATA_LAYER = 'metadata', DATA_DOMAIN = 'semantic';
COMMENT ON TABLE METADATA.SEMANTIC_LAYER IS 'Business concept → KPI → Silver column mappings for AI context';

ALTER TABLE METADATA.KG_NODES SET TAG DATA_LAYER = 'metadata', DATA_DOMAIN = 'knowledge_graph';
COMMENT ON TABLE METADATA.KG_NODES IS 'Knowledge graph entity nodes representing Silver tables';

ALTER TABLE METADATA.KG_EDGES SET TAG DATA_LAYER = 'metadata', DATA_DOMAIN = 'knowledge_graph';
COMMENT ON TABLE METADATA.KG_EDGES IS 'Knowledge graph FK relationships between entities';

ALTER TABLE METADATA.PIPELINE_RUNS SET TAG DATA_LAYER = 'metadata', DATA_DOMAIN = 'pipeline';
COMMENT ON TABLE METADATA.PIPELINE_RUNS IS 'ETL pipeline tracking — last load time and row counts per domain';

-- =========================================================================
-- PII/PHI Column-Level Sensitivity Tags
-- =========================================================================

-- Patient PII columns
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

-- Key column comments for Cortex Analyst discoverability
COMMENT ON COLUMN SILVER.CLAIMS.TOTAL_CHARGE_AMOUNT IS 'Total billed amount for the claim in USD';
COMMENT ON COLUMN SILVER.CLAIMS.CLAIM_STATUS IS 'Current status: Paid, Partially Paid, Denied, Pending, or Appealed';
COMMENT ON COLUMN SILVER.CLAIMS.IS_CLEAN_CLAIM IS 'Boolean (1/0): claim passed all payer edits on first submission';
COMMENT ON COLUMN SILVER.CLAIMS.DATE_OF_SERVICE IS 'Date the healthcare service was rendered (YYYY-MM-DD)';
COMMENT ON COLUMN SILVER.CLAIMS.SUBMISSION_DATE IS 'Date the claim was submitted to the payer (YYYY-MM-DD)';
COMMENT ON COLUMN SILVER.PAYMENTS.PAYMENT_AMOUNT IS 'Amount paid by the payer for this claim in USD';
COMMENT ON COLUMN SILVER.PAYMENTS.ALLOWED_AMOUNT IS 'Contracted allowed amount per payer agreement in USD';
COMMENT ON COLUMN SILVER.PAYMENTS.IS_ACCURATE_PAYMENT IS 'Boolean (1/0): payment matches contracted reimbursement rate';
COMMENT ON COLUMN SILVER.DENIALS.DENIED_AMOUNT IS 'Dollar amount denied by the payer in USD';
COMMENT ON COLUMN SILVER.DENIALS.APPEAL_STATUS IS 'Appeal outcome: Not Appealed, Won, Lost, or In Progress';
COMMENT ON COLUMN SILVER.DENIALS.RECOVERED_AMOUNT IS 'Amount recovered through successful appeal in USD';
COMMENT ON COLUMN SILVER.ADJUSTMENTS.ADJUSTMENT_AMOUNT IS 'Adjustment amount in USD (contractual write-off, bad debt, etc.)';
COMMENT ON COLUMN SILVER.ADJUSTMENTS.ADJUSTMENT_TYPE_CODE IS 'Type: CONTRACTUAL, WRITEOFF, ADMIN, PROMPT_PAY, CHARITY, SMALL_BAL';
COMMENT ON COLUMN SILVER.OPERATING_COSTS.TOTAL_RCM_COST IS 'Total monthly RCM department cost in USD';
