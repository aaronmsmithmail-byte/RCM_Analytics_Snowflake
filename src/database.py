"""
Database Setup and Management for Healthcare RCM Analytics
==========================================================

This module handles all SQLite database operations for the RCM Analytics application.
SQLite was chosen because:
    - It requires zero configuration (no separate server process).
    - It stores the entire database in a single file, making it portable.
    - It ships with Python's standard library (sqlite3 module).
    - It handles millions of rows efficiently for local analytics.
    - It supports full SQL, enabling complex joins and aggregations.

Architecture Overview:
    1. The CSV files (in ./data/) serve as the raw "source of truth" input files.
    2. This module reads those CSVs and loads them into a normalized SQLite database.
    3. The Streamlit dashboard then queries SQLite instead of reading CSVs directly.

    This approach scales much better than CSV-based loading because:
    - SQLite uses indexed B-trees for fast lookups (O(log n) vs O(n) for CSV scans).
    - SQL queries push filtering/aggregation to the database engine.
    - Adding new data is an INSERT rather than rewriting an entire CSV.
    - Multiple processes can read the database concurrently.

Database Schema:
    The schema mirrors the 10 CSV files, organized into the following tables:

    Reference Tables (relatively static):
        - payers:           Insurance companies and self-pay (10 rows)
        - patients:         Patient demographics and insurance info (500 rows)
        - providers:        Physicians/clinicians and their departments (25 rows)

    Transactional Tables (grow over time):
        - encounters:       Patient visits (outpatient, inpatient, ED, telehealth)
        - charges:          Individual CPT-level line items billed per encounter
        - claims:           Insurance claim submissions (one per encounter)
        - payments:         Payer and patient payment remittances
        - denials:          Claim denial records with appeal tracking
        - adjustments:      Contractual write-offs, bad debt, charity care

    Operational Tables:
        - operating_costs:  Monthly RCM department costs (for Cost-to-Collect KPI)

Usage:
    # One-time setup: create database and load CSV data
    python -m src.database

    # Or programmatically:
    from src.database import initialize_database
    initialize_database()

    # Query data for the dashboard:
    from src.database import get_connection
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM claims WHERE claim_status = 'Denied'", conn)
"""

import sqlite3
import os
import pandas as pd

# ---------------------------------------------------------------------------
# Path Configuration
# ---------------------------------------------------------------------------
# The database file lives alongside the CSV data in the ./data/ directory.
# This keeps all data artifacts in one place and makes the project portable.
# ---------------------------------------------------------------------------
_BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(_BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "rcm_analytics.db")


# ===========================================================================
# SQL Schema Definitions
# ===========================================================================
# Each CREATE TABLE statement includes:
#   - A PRIMARY KEY for unique row identification
#   - Appropriate data types (TEXT for IDs/strings, REAL for money, INTEGER for counts)
#   - FOREIGN KEY constraints to enforce referential integrity between tables
#
# Note on money types: We use REAL (floating point) for dollar amounts.
#   In a production system, you might use INTEGER cents to avoid floating-point
#   rounding, but REAL is simpler and sufficient for analytics/reporting.
# ===========================================================================

SCHEMA_SQL = """
-- =========================================================================
-- REFERENCE TABLES
-- These tables contain relatively static lookup/dimension data.
-- =========================================================================

-- Payers: Insurance companies, government programs, and self-pay.
-- Each payer has a contract that defines expected reimbursement rates.
-- The avg_reimbursement_pct field represents the historical average
-- percentage of billed charges that this payer actually reimburses.
CREATE TABLE IF NOT EXISTS payers (
    payer_id                TEXT PRIMARY KEY,   -- Unique payer identifier (e.g., PYR001)
    payer_name              TEXT NOT NULL,       -- Display name (e.g., "Blue Cross Blue Shield")
    payer_type              TEXT NOT NULL,       -- Category: Commercial, Government, or Self-Pay
    avg_reimbursement_pct   REAL,               -- Historical avg reimbursement as decimal (0.80 = 80%)
    contract_id             TEXT                 -- Reference to the payer contract
);

-- Patients: Demographics and primary insurance information.
-- In a real system, a patient could have multiple insurance plans
-- (primary, secondary, tertiary). We simplify to one primary payer here.
CREATE TABLE IF NOT EXISTS patients (
    patient_id       TEXT PRIMARY KEY,           -- Unique patient ID (e.g., PAT00001)
    first_name       TEXT NOT NULL,
    last_name        TEXT NOT NULL,
    date_of_birth    TEXT,                       -- ISO format: YYYY-MM-DD
    gender           TEXT,                       -- M or F
    primary_payer_id TEXT,                       -- FK to payers table
    member_id        TEXT,                       -- Insurance member/subscriber ID
    zip_code         TEXT,                       -- Patient's ZIP code for geographic analysis
    FOREIGN KEY (primary_payer_id) REFERENCES payers(payer_id)
);

-- Providers: Physicians, nurse practitioners, and other clinicians.
-- NPI (National Provider Identifier) is a unique 10-digit number assigned
-- by CMS to every healthcare provider in the United States.
CREATE TABLE IF NOT EXISTS providers (
    provider_id    TEXT PRIMARY KEY,             -- Internal provider ID (e.g., PROV001)
    provider_name  TEXT NOT NULL,                -- Full name (e.g., "Dr. Sarah Chen")
    npi            TEXT,                         -- 10-digit National Provider Identifier
    department     TEXT,                         -- Clinical department (e.g., "Cardiology")
    specialty      TEXT                          -- Medical specialty
);


-- =========================================================================
-- TRANSACTIONAL TABLES
-- These tables grow continuously as the organization sees patients,
-- submits claims, and receives payments.
-- =========================================================================

-- Encounters: Each row represents one patient visit/interaction.
-- An encounter is the starting point of the revenue cycle:
--   Encounter -> Charges -> Claim -> Payment/Denial
CREATE TABLE IF NOT EXISTS encounters (
    encounter_id    TEXT PRIMARY KEY,            -- Unique encounter ID (e.g., ENC000001)
    patient_id      TEXT NOT NULL,               -- FK to patients table
    provider_id     TEXT NOT NULL,               -- FK to providers table
    date_of_service TEXT NOT NULL,               -- When the service was provided (YYYY-MM-DD)
    discharge_date  TEXT,                        -- When patient was discharged (same day for outpatient)
    encounter_type  TEXT,                        -- Outpatient, Inpatient, Emergency, or Telehealth
    department      TEXT,                        -- Department where service occurred
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
    FOREIGN KEY (provider_id) REFERENCES providers(provider_id)
);

-- Charges: Individual billable line items for each encounter.
-- A single encounter may have multiple charges (e.g., an office visit +
-- a blood draw + a lab panel = 3 charges on one encounter).
-- CPT (Current Procedural Terminology) codes identify the service performed.
-- ICD-10 codes identify the diagnosis that justified the service.
CREATE TABLE IF NOT EXISTS charges (
    charge_id       TEXT PRIMARY KEY,            -- Unique charge ID (e.g., CHG0000001)
    encounter_id    TEXT NOT NULL,               -- FK to encounters table
    cpt_code        TEXT NOT NULL,               -- CPT procedure code (e.g., "99213")
    cpt_description TEXT,                        -- Human-readable CPT description
    units           INTEGER DEFAULT 1,           -- Number of units billed
    charge_amount   REAL NOT NULL,               -- Dollar amount billed
    service_date    TEXT NOT NULL,               -- Date the service was rendered
    post_date       TEXT,                        -- Date the charge was posted to the billing system
    icd10_code      TEXT,                        -- ICD-10 diagnosis code
    FOREIGN KEY (encounter_id) REFERENCES encounters(encounter_id)
);

-- Claims: Insurance claim submissions. Typically one claim per encounter,
-- though complex encounters may generate multiple claims.
-- The claim lifecycle: Submitted -> Paid/Denied/Partially Paid/Pending/Appealed
CREATE TABLE IF NOT EXISTS claims (
    claim_id              TEXT PRIMARY KEY,      -- Unique claim ID (e.g., CLM0000001)
    encounter_id          TEXT NOT NULL,         -- FK to encounters table
    patient_id            TEXT NOT NULL,         -- FK to patients table
    payer_id              TEXT NOT NULL,         -- FK to payers table (who we're billing)
    date_of_service       TEXT NOT NULL,         -- Service date from the encounter
    submission_date       TEXT NOT NULL,         -- When the claim was submitted to the payer
    total_charge_amount   REAL NOT NULL,         -- Total billed amount on this claim
    claim_status          TEXT NOT NULL,         -- Paid, Denied, Pending, Partially Paid, Appealed
    is_clean_claim        INTEGER,               -- 1 = clean (no errors), 0 = had issues
    submission_method     TEXT,                  -- Electronic or Paper
    FOREIGN KEY (encounter_id) REFERENCES encounters(encounter_id),
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
    FOREIGN KEY (payer_id) REFERENCES payers(payer_id)
);

-- Payments: Remittance records from payers and patients.
-- A single claim can have multiple payments (e.g., payer pays 80%,
-- then patient pays the remaining 20% copay/coinsurance).
CREATE TABLE IF NOT EXISTS payments (
    payment_id          TEXT PRIMARY KEY,        -- Unique payment ID (e.g., PAY0000001)
    claim_id            TEXT NOT NULL,           -- FK to claims table
    payer_id            TEXT NOT NULL,           -- Who paid (payer ID or "PATIENT")
    payment_amount      REAL NOT NULL,           -- Dollar amount received
    allowed_amount      REAL,                    -- Payer's allowed/approved amount for the service
    payment_date        TEXT NOT NULL,           -- Date payment was received
    payment_method      TEXT,                    -- EFT, Check, Virtual Card, Credit Card, Cash, etc.
    is_accurate_payment INTEGER,                 -- 1 = correct amount, 0 = over/underpayment
    FOREIGN KEY (claim_id) REFERENCES claims(claim_id)
);

-- Denials: Records of claim denials and their appeal outcomes.
-- Denials are a major source of revenue leakage in healthcare.
-- Common reasons include: missing prior auth, coding errors, eligibility issues.
CREATE TABLE IF NOT EXISTS denials (
    denial_id                  TEXT PRIMARY KEY, -- Unique denial ID (e.g., DEN000001)
    claim_id                   TEXT NOT NULL,    -- FK to claims table
    denial_reason_code         TEXT NOT NULL,    -- Short code (e.g., "AUTH", "COD", "ELIG")
    denial_reason_description  TEXT,             -- Human-readable reason
    denial_date                TEXT NOT NULL,    -- When the denial was received
    denied_amount              REAL NOT NULL,    -- Dollar amount denied
    appeal_status              TEXT,             -- Not Appealed, Won, Lost, In Progress
    appeal_date                TEXT,             -- When the appeal was filed (if applicable)
    recovered_amount           REAL DEFAULT 0,   -- Amount recovered through successful appeal
    FOREIGN KEY (claim_id) REFERENCES claims(claim_id)
);

-- Adjustments: Financial adjustments applied to claims.
-- Types include:
--   CONTRACTUAL: Negotiated discount between provider and payer
--   WRITEOFF:    Bad debt written off as uncollectable
--   CHARITY:     Charity care provided at no cost to patient
--   ADMIN:       Administrative corrections
--   PROMPT_PAY:  Discount for early payment
--   SMALL_BAL:   Small balance write-off (not worth pursuing)
CREATE TABLE IF NOT EXISTS adjustments (
    adjustment_id               TEXT PRIMARY KEY,   -- Unique adjustment ID (e.g., ADJ000001)
    claim_id                    TEXT NOT NULL,       -- FK to claims table
    adjustment_type_code        TEXT NOT NULL,       -- Type code (e.g., "CONTRACTUAL")
    adjustment_type_description TEXT,                -- Human-readable description
    adjustment_amount           REAL NOT NULL,       -- Dollar amount adjusted
    adjustment_date             TEXT NOT NULL,       -- When the adjustment was applied
    FOREIGN KEY (claim_id) REFERENCES claims(claim_id)
);


-- =========================================================================
-- OPERATIONAL TABLES
-- Supporting data for operational KPIs.
-- =========================================================================

-- Operating Costs: Monthly cost breakdown for the RCM department.
-- Used to calculate the "Cost to Collect" KPI, which measures how many
-- cents it costs to collect each dollar of revenue.
-- Industry benchmark: 3-8% of collections.
CREATE TABLE IF NOT EXISTS operating_costs (
    period              TEXT PRIMARY KEY,        -- Month in YYYY-MM format
    billing_staff_cost  REAL,                   -- Salaries for billing/coding staff
    software_cost       REAL,                   -- EHR, billing software, clearinghouse fees
    outsourcing_cost    REAL,                   -- Third-party billing service fees
    supplies_overhead   REAL,                   -- Office supplies, postage, printing
    total_rcm_cost      REAL NOT NULL           -- Sum of all cost categories
);
"""

# ===========================================================================
# Index Definitions
# ===========================================================================
# Indexes speed up the most common query patterns in our dashboard.
# Without indexes, the database would do full table scans for every filter.
#
# We index:
#   - Foreign keys used in JOINs (claim_id, encounter_id, patient_id, payer_id)
#   - Date columns used in WHERE clauses for date-range filtering
#   - Status columns used in GROUP BY / WHERE for KPI calculations
# ===========================================================================

INDEX_SQL = """
-- Claims indexes: most-queried table in the dashboard
CREATE INDEX IF NOT EXISTS idx_claims_dos          ON claims(date_of_service);
CREATE INDEX IF NOT EXISTS idx_claims_submission    ON claims(submission_date);
CREATE INDEX IF NOT EXISTS idx_claims_payer         ON claims(payer_id);
CREATE INDEX IF NOT EXISTS idx_claims_patient       ON claims(patient_id);
CREATE INDEX IF NOT EXISTS idx_claims_encounter     ON claims(encounter_id);
CREATE INDEX IF NOT EXISTS idx_claims_status        ON claims(claim_status);

-- Payments indexes: frequently joined with claims
CREATE INDEX IF NOT EXISTS idx_payments_claim       ON payments(claim_id);
CREATE INDEX IF NOT EXISTS idx_payments_date        ON payments(payment_date);
CREATE INDEX IF NOT EXISTS idx_payments_payer       ON payments(payer_id);

-- Denials indexes: used for denial analysis tab
CREATE INDEX IF NOT EXISTS idx_denials_claim        ON denials(claim_id);
CREATE INDEX IF NOT EXISTS idx_denials_reason       ON denials(denial_reason_code);
CREATE INDEX IF NOT EXISTS idx_denials_date         ON denials(denial_date);

-- Adjustments indexes
CREATE INDEX IF NOT EXISTS idx_adjustments_claim    ON adjustments(claim_id);
CREATE INDEX IF NOT EXISTS idx_adjustments_type     ON adjustments(adjustment_type_code);

-- Encounters indexes: filtered by date, department, type
CREATE INDEX IF NOT EXISTS idx_encounters_dos       ON encounters(date_of_service);
CREATE INDEX IF NOT EXISTS idx_encounters_dept      ON encounters(department);
CREATE INDEX IF NOT EXISTS idx_encounters_type      ON encounters(encounter_type);
CREATE INDEX IF NOT EXISTS idx_encounters_patient   ON encounters(patient_id);

-- Charges indexes
CREATE INDEX IF NOT EXISTS idx_charges_encounter    ON charges(encounter_id);
CREATE INDEX IF NOT EXISTS idx_charges_service_date ON charges(service_date);
"""


def get_connection(db_path=None):
    """
    Create and return a SQLite database connection.

    Args:
        db_path: Optional override for the database file path.
                 Defaults to ./data/rcm_analytics.db.

    Returns:
        sqlite3.Connection: A connection object to the SQLite database.

    Notes:
        - Each call creates a new connection. In a production app, you might
          use a connection pool, but for Streamlit's single-user model this
          is fine.
        - We enable WAL (Write-Ahead Logging) mode for better concurrent
          read performance, which helps when the dashboard refreshes while
          data is being loaded.
    """
    path = db_path or DB_PATH
    conn = sqlite3.connect(path)
    # WAL mode allows concurrent readers and one writer, which is ideal
    # for a dashboard that reads frequently while data loads happen occasionally.
    conn.execute("PRAGMA journal_mode=WAL;")
    # Foreign key enforcement is off by default in SQLite; turn it on
    # so that our FOREIGN KEY constraints actually prevent bad data.
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def create_tables(conn):
    """
    Create all database tables if they don't already exist.

    This is idempotent - safe to call multiple times. The "IF NOT EXISTS"
    clause in each CREATE TABLE statement means existing tables and their
    data are preserved.

    Args:
        conn: An active SQLite connection.
    """
    conn.executescript(SCHEMA_SQL)
    conn.executescript(INDEX_SQL)
    conn.commit()
    print("  [OK] Tables and indexes created successfully.")


def load_csv_to_table(conn, table_name, csv_filename):
    """
    Load a single CSV file into its corresponding database table.

    This function:
        1. Reads the CSV into a pandas DataFrame.
        2. Handles boolean columns (True/False -> 1/0 for SQLite compatibility).
        3. Replaces all existing data in the table (DELETE + INSERT pattern).
        4. Reports the number of rows loaded.

    Args:
        conn:          An active SQLite connection.
        table_name:    The database table name (e.g., "claims").
        csv_filename:  The CSV filename in the data directory (e.g., "claims.csv").

    Why DELETE + INSERT instead of just INSERT?
        This makes the function idempotent. Running it twice loads the same
        data without duplicates. In production, you'd use an upsert (INSERT
        OR REPLACE) or a staging-table pattern for incremental loads.
    """
    csv_path = os.path.join(DATA_DIR, csv_filename)
    if not os.path.exists(csv_path):
        print(f"  [SKIP] {csv_filename} not found at {csv_path}")
        return

    # Read the CSV into a DataFrame
    df = pd.read_csv(csv_path)

    # SQLite doesn't have a native BOOLEAN type. Convert True/False to 1/0
    # so that SUM() and other aggregate functions work correctly on these columns.
    bool_cols = [c for c in df.columns if c.startswith("is_")]
    for col in bool_cols:
        df[col] = df[col].astype(str).str.lower().map({"true": 1, "false": 0})

    # Clear existing data and insert fresh rows.
    # We use a transaction (BEGIN/COMMIT) so that the table is never empty
    # if something goes wrong mid-load.
    conn.execute(f"DELETE FROM {table_name};")
    df.to_sql(table_name, conn, if_exists="append", index=False)
    conn.commit()

    print(f"  [OK] Loaded {len(df):,} rows into '{table_name}' from {csv_filename}")


def initialize_database(db_path=None):
    """
    Full database initialization: create tables, load all CSV data.

    This is the main entry point for setting up the database. It:
        1. Creates a new SQLite database file (or opens existing one).
        2. Creates all 10 tables with proper schema and indexes.
        3. Loads each CSV file into its corresponding table.

    Args:
        db_path: Optional override for the database file path.

    The CSV-to-table mapping:
        payers.csv          -> payers          (insurance companies)
        patients.csv        -> patients        (patient demographics)
        providers.csv       -> providers       (physicians)
        encounters.csv      -> encounters      (patient visits)
        charges.csv         -> charges         (billable line items)
        claims.csv          -> claims          (insurance submissions)
        payments.csv        -> payments        (remittances received)
        denials.csv         -> denials         (denied claims)
        adjustments.csv     -> adjustments     (financial adjustments)
        operating_costs.csv -> operating_costs (monthly RCM costs)
    """
    print("=" * 60)
    print("Healthcare RCM Analytics - Database Initialization")
    print("=" * 60)

    conn = get_connection(db_path)
    print(f"\n  Database: {db_path or DB_PATH}")
    print()

    # Step 1: Create the schema
    print("Step 1: Creating tables and indexes...")
    create_tables(conn)
    print()

    # Step 2: Load CSV data into tables
    # The order matters because of foreign key constraints.
    # We load reference tables first, then transactional tables.
    print("Step 2: Loading CSV data into tables...")
    csv_table_map = [
        # Reference tables first (no foreign key dependencies)
        ("payers",          "payers.csv"),
        ("patients",        "patients.csv"),
        ("providers",       "providers.csv"),
        # Transactional tables (depend on reference tables)
        ("encounters",      "encounters.csv"),
        ("charges",         "charges.csv"),
        ("claims",          "claims.csv"),
        ("payments",        "payments.csv"),
        ("denials",         "denials.csv"),
        ("adjustments",     "adjustments.csv"),
        # Operational tables
        ("operating_costs", "operating_costs.csv"),
    ]

    for table_name, csv_filename in csv_table_map:
        load_csv_to_table(conn, table_name, csv_filename)

    # Step 3: Verify the load
    print("\nStep 3: Verifying loaded data...")
    for table_name, _ in csv_table_map:
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"  {table_name}: {count:,} rows")

    conn.close()
    print("\n" + "=" * 60)
    print("Database initialization complete!")
    print(f"Database file: {db_path or DB_PATH}")
    print("=" * 60)


def query_to_dataframe(sql, params=None, db_path=None):
    """
    Execute a SQL query and return results as a pandas DataFrame.

    This is a convenience function used by the data_loader module to
    fetch data for the Streamlit dashboard.

    Args:
        sql:     SQL query string. Can include ? placeholders for parameters.
        params:  Optional tuple/list of parameters for parameterized queries.
                 Always use parameters instead of string formatting to prevent
                 SQL injection (even though this is a local app, it's good practice).
        db_path: Optional override for the database file path.

    Returns:
        pd.DataFrame: Query results as a DataFrame.

    Example:
        # Get all denied claims for a specific payer
        df = query_to_dataframe(
            "SELECT * FROM claims WHERE claim_status = ? AND payer_id = ?",
            params=("Denied", "PYR001")
        )
    """
    conn = get_connection(db_path)
    try:
        if params:
            df = pd.read_sql_query(sql, conn, params=params)
        else:
            df = pd.read_sql_query(sql, conn)
        return df
    finally:
        conn.close()


def get_table_info(table_name, db_path=None):
    """
    Get column information for a specific table (useful for debugging).

    Args:
        table_name: Name of the table to inspect.
        db_path:    Optional override for the database file path.

    Returns:
        list of tuples: Each tuple contains (column_id, name, type, not_null, default, pk).
    """
    conn = get_connection(db_path)
    try:
        cursor = conn.execute(f"PRAGMA table_info({table_name});")
        return cursor.fetchall()
    finally:
        conn.close()


# ===========================================================================
# CLI Entry Point
# ===========================================================================
# Run this module directly to initialize the database:
#   python -m src.database
# ===========================================================================

if __name__ == "__main__":
    initialize_database()
