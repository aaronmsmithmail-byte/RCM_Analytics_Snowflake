-- ============================================================================
-- SILVER LAYER — Cleaned & Conformed Schema
-- ============================================================================
-- Columns have proper types, FK constraints are declared (not enforced by
-- Snowflake but serve as documentation), and only valid rows land here.
-- This is the primary layer consumed by the dashboard and Cortex Analyst.
-- ============================================================================

USE DATABASE RCM_ANALYTICS;
USE SCHEMA SILVER;

-- =========================================================================
-- Reference Tables (no FK dependencies — created first)
-- =========================================================================

CREATE TABLE IF NOT EXISTS PAYERS (
    PAYER_ID              VARCHAR    PRIMARY KEY,
    PAYER_NAME            VARCHAR    NOT NULL,
    PAYER_TYPE            VARCHAR    NOT NULL,
    AVG_REIMBURSEMENT_PCT FLOAT,
    CONTRACT_ID           VARCHAR
);

CREATE TABLE IF NOT EXISTS PATIENTS (
    PATIENT_ID       VARCHAR PRIMARY KEY,
    FIRST_NAME       VARCHAR NOT NULL,
    LAST_NAME        VARCHAR NOT NULL,
    DATE_OF_BIRTH    VARCHAR,
    GENDER           VARCHAR,
    PRIMARY_PAYER_ID VARCHAR,
    MEMBER_ID        VARCHAR,
    ZIP_CODE         VARCHAR,
    FOREIGN KEY (PRIMARY_PAYER_ID) REFERENCES SILVER.PAYERS(PAYER_ID)
);

CREATE TABLE IF NOT EXISTS PROVIDERS (
    PROVIDER_ID   VARCHAR PRIMARY KEY,
    PROVIDER_NAME VARCHAR NOT NULL,
    NPI           VARCHAR,
    DEPARTMENT    VARCHAR,
    SPECIALTY     VARCHAR
);

-- =========================================================================
-- Transactional Tables (depend on reference tables above)
-- =========================================================================

CREATE TABLE IF NOT EXISTS ENCOUNTERS (
    ENCOUNTER_ID    VARCHAR PRIMARY KEY,
    PATIENT_ID      VARCHAR NOT NULL,
    PROVIDER_ID     VARCHAR NOT NULL,
    DATE_OF_SERVICE VARCHAR NOT NULL,
    DISCHARGE_DATE  VARCHAR,
    ENCOUNTER_TYPE  VARCHAR,
    DEPARTMENT      VARCHAR,
    FOREIGN KEY (PATIENT_ID)  REFERENCES SILVER.PATIENTS(PATIENT_ID),
    FOREIGN KEY (PROVIDER_ID) REFERENCES SILVER.PROVIDERS(PROVIDER_ID)
);

CREATE TABLE IF NOT EXISTS CHARGES (
    CHARGE_ID       VARCHAR PRIMARY KEY,
    ENCOUNTER_ID    VARCHAR NOT NULL,
    CPT_CODE        VARCHAR NOT NULL,
    CPT_DESCRIPTION VARCHAR,
    UNITS           INTEGER DEFAULT 1,
    CHARGE_AMOUNT   FLOAT   NOT NULL,
    SERVICE_DATE    VARCHAR NOT NULL,
    POST_DATE       VARCHAR,
    ICD10_CODE      VARCHAR,
    FOREIGN KEY (ENCOUNTER_ID) REFERENCES SILVER.ENCOUNTERS(ENCOUNTER_ID)
);

CREATE TABLE IF NOT EXISTS CLAIMS (
    CLAIM_ID            VARCHAR PRIMARY KEY,
    ENCOUNTER_ID        VARCHAR NOT NULL,
    PATIENT_ID          VARCHAR NOT NULL,
    PAYER_ID            VARCHAR NOT NULL,
    DATE_OF_SERVICE     VARCHAR NOT NULL,
    SUBMISSION_DATE     VARCHAR NOT NULL,
    TOTAL_CHARGE_AMOUNT FLOAT   NOT NULL,
    CLAIM_STATUS        VARCHAR NOT NULL,
    IS_CLEAN_CLAIM      INTEGER,
    SUBMISSION_METHOD   VARCHAR,
    FAIL_REASON         VARCHAR,
    FOREIGN KEY (ENCOUNTER_ID) REFERENCES SILVER.ENCOUNTERS(ENCOUNTER_ID),
    FOREIGN KEY (PATIENT_ID)   REFERENCES SILVER.PATIENTS(PATIENT_ID),
    FOREIGN KEY (PAYER_ID)     REFERENCES SILVER.PAYERS(PAYER_ID)
);

CREATE TABLE IF NOT EXISTS PAYMENTS (
    PAYMENT_ID          VARCHAR PRIMARY KEY,
    CLAIM_ID            VARCHAR NOT NULL,
    PAYER_ID            VARCHAR NOT NULL,
    PAYMENT_AMOUNT      FLOAT   NOT NULL,
    ALLOWED_AMOUNT      FLOAT,
    PAYMENT_DATE        VARCHAR NOT NULL,
    PAYMENT_METHOD      VARCHAR,
    IS_ACCURATE_PAYMENT INTEGER,
    FOREIGN KEY (CLAIM_ID) REFERENCES SILVER.CLAIMS(CLAIM_ID)
);

CREATE TABLE IF NOT EXISTS DENIALS (
    DENIAL_ID                 VARCHAR PRIMARY KEY,
    CLAIM_ID                  VARCHAR NOT NULL,
    DENIAL_REASON_CODE        VARCHAR NOT NULL,
    DENIAL_REASON_DESCRIPTION VARCHAR,
    DENIAL_DATE               VARCHAR NOT NULL,
    DENIED_AMOUNT             FLOAT   NOT NULL,
    APPEAL_STATUS             VARCHAR,
    APPEAL_DATE               VARCHAR,
    RECOVERED_AMOUNT          FLOAT   DEFAULT 0,
    FOREIGN KEY (CLAIM_ID) REFERENCES SILVER.CLAIMS(CLAIM_ID)
);

CREATE TABLE IF NOT EXISTS ADJUSTMENTS (
    ADJUSTMENT_ID               VARCHAR PRIMARY KEY,
    CLAIM_ID                    VARCHAR NOT NULL,
    ADJUSTMENT_TYPE_CODE        VARCHAR NOT NULL,
    ADJUSTMENT_TYPE_DESCRIPTION VARCHAR,
    ADJUSTMENT_AMOUNT           FLOAT   NOT NULL,
    ADJUSTMENT_DATE             VARCHAR NOT NULL,
    FOREIGN KEY (CLAIM_ID) REFERENCES SILVER.CLAIMS(CLAIM_ID)
);

-- =========================================================================
-- Operational Tables
-- =========================================================================

CREATE TABLE IF NOT EXISTS OPERATING_COSTS (
    PERIOD             VARCHAR PRIMARY KEY,
    BILLING_STAFF_COST FLOAT,
    SOFTWARE_COST      FLOAT,
    OUTSOURCING_COST   FLOAT,
    SUPPLIES_OVERHEAD  FLOAT,
    TOTAL_RCM_COST     FLOAT   NOT NULL
);
