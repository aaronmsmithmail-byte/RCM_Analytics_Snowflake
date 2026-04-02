"""
Data Quality Validators for Snowflake RCM Analytics
====================================================

SQL COUNT-based data quality assertions adapted for Snowflake.
Uses Snowpark session instead of DuckDB connections.
"""

from snowflake.snowpark.context import get_active_session


def _get_session():
    """Get the active Snowpark session."""
    return get_active_session()


def _count(sql):
    """Execute a COUNT query and return the integer result."""
    session = _get_session()
    result = session.sql(sql).collect()
    return result[0][0] if result else 0


# =========================================================================
# Row-count validators
# =========================================================================


def validate_payers_not_empty():
    """Silver payers table must have at least one row."""
    return _count("SELECT COUNT(*) FROM RCM_ANALYTICS.SILVER.PAYERS") > 0


def validate_patients_not_empty():
    """Silver patients table must have at least one row."""
    return _count("SELECT COUNT(*) FROM RCM_ANALYTICS.SILVER.PATIENTS") > 0


def validate_providers_not_empty():
    """Silver providers table must have at least one row."""
    return _count("SELECT COUNT(*) FROM RCM_ANALYTICS.SILVER.PROVIDERS") > 0


def validate_encounters_not_empty():
    """Silver encounters table must have at least one row."""
    return _count("SELECT COUNT(*) FROM RCM_ANALYTICS.SILVER.ENCOUNTERS") > 0


def validate_charges_not_empty():
    """Silver charges table must have at least one row."""
    return _count("SELECT COUNT(*) FROM RCM_ANALYTICS.SILVER.CHARGES") > 0


def validate_claims_not_empty():
    """Silver claims table must have at least one row."""
    return _count("SELECT COUNT(*) FROM RCM_ANALYTICS.SILVER.CLAIMS") > 0


def validate_payments_not_empty():
    """Silver payments table must have at least one row."""
    return _count("SELECT COUNT(*) FROM RCM_ANALYTICS.SILVER.PAYMENTS") > 0


def validate_denials_not_empty():
    """Silver denials table must have at least one row."""
    return _count("SELECT COUNT(*) FROM RCM_ANALYTICS.SILVER.DENIALS") > 0


def validate_adjustments_not_empty():
    """Silver adjustments table must have at least one row."""
    return _count("SELECT COUNT(*) FROM RCM_ANALYTICS.SILVER.ADJUSTMENTS") > 0


def validate_operating_costs_not_empty():
    """Silver operating costs table must have at least one row."""
    return _count("SELECT COUNT(*) FROM RCM_ANALYTICS.SILVER.OPERATING_COSTS") > 0


# =========================================================================
# Referential integrity validators
# =========================================================================


def validate_claims_have_valid_payers():
    """Every claim must reference a valid payer."""
    orphan_count = _count("""
        SELECT COUNT(*)
        FROM RCM_ANALYTICS.SILVER.CLAIMS c
        LEFT JOIN RCM_ANALYTICS.SILVER.PAYERS p ON c.PAYER_ID = p.PAYER_ID
        WHERE p.PAYER_ID IS NULL
    """)
    return orphan_count == 0


def validate_claims_have_valid_encounters():
    """Every claim must reference a valid encounter."""
    orphan_count = _count("""
        SELECT COUNT(*)
        FROM RCM_ANALYTICS.SILVER.CLAIMS c
        LEFT JOIN RCM_ANALYTICS.SILVER.ENCOUNTERS e ON c.ENCOUNTER_ID = e.ENCOUNTER_ID
        WHERE e.ENCOUNTER_ID IS NULL
    """)
    return orphan_count == 0


def validate_claims_have_valid_patients():
    """Every claim must reference a valid patient."""
    orphan_count = _count("""
        SELECT COUNT(*)
        FROM RCM_ANALYTICS.SILVER.CLAIMS c
        LEFT JOIN RCM_ANALYTICS.SILVER.PATIENTS pt ON c.PATIENT_ID = pt.PATIENT_ID
        WHERE pt.PATIENT_ID IS NULL
    """)
    return orphan_count == 0


def validate_payments_have_valid_claims():
    """Every payment must reference a valid claim."""
    orphan_count = _count("""
        SELECT COUNT(*)
        FROM RCM_ANALYTICS.SILVER.PAYMENTS p
        LEFT JOIN RCM_ANALYTICS.SILVER.CLAIMS c ON p.CLAIM_ID = c.CLAIM_ID
        WHERE c.CLAIM_ID IS NULL
    """)
    return orphan_count == 0


def validate_denials_have_valid_claims():
    """Every denial must reference a valid claim."""
    orphan_count = _count("""
        SELECT COUNT(*)
        FROM RCM_ANALYTICS.SILVER.DENIALS d
        LEFT JOIN RCM_ANALYTICS.SILVER.CLAIMS c ON d.CLAIM_ID = c.CLAIM_ID
        WHERE c.CLAIM_ID IS NULL
    """)
    return orphan_count == 0


def validate_adjustments_have_valid_claims():
    """Every adjustment must reference a valid claim."""
    orphan_count = _count("""
        SELECT COUNT(*)
        FROM RCM_ANALYTICS.SILVER.ADJUSTMENTS a
        LEFT JOIN RCM_ANALYTICS.SILVER.CLAIMS c ON a.CLAIM_ID = c.CLAIM_ID
        WHERE c.CLAIM_ID IS NULL
    """)
    return orphan_count == 0


def validate_encounters_have_valid_patients():
    """Every encounter must reference a valid patient."""
    orphan_count = _count("""
        SELECT COUNT(*)
        FROM RCM_ANALYTICS.SILVER.ENCOUNTERS e
        LEFT JOIN RCM_ANALYTICS.SILVER.PATIENTS pt ON e.PATIENT_ID = pt.PATIENT_ID
        WHERE pt.PATIENT_ID IS NULL
    """)
    return orphan_count == 0


def validate_encounters_have_valid_providers():
    """Every encounter must reference a valid provider."""
    orphan_count = _count("""
        SELECT COUNT(*)
        FROM RCM_ANALYTICS.SILVER.ENCOUNTERS e
        LEFT JOIN RCM_ANALYTICS.SILVER.PROVIDERS pr ON e.PROVIDER_ID = pr.PROVIDER_ID
        WHERE pr.PROVIDER_ID IS NULL
    """)
    return orphan_count == 0


# =========================================================================
# Data quality validators
# =========================================================================


def validate_no_negative_charges():
    """Charge amounts must be non-negative."""
    return _count("SELECT COUNT(*) FROM RCM_ANALYTICS.SILVER.CHARGES WHERE CHARGE_AMOUNT < 0") == 0


def validate_no_negative_payments():
    """Payment amounts must be non-negative."""
    return _count("SELECT COUNT(*) FROM RCM_ANALYTICS.SILVER.PAYMENTS WHERE PAYMENT_AMOUNT < 0") == 0


def validate_no_negative_denied_amounts():
    """Denied amounts must be non-negative."""
    return _count("SELECT COUNT(*) FROM RCM_ANALYTICS.SILVER.DENIALS WHERE DENIED_AMOUNT < 0") == 0


def validate_claim_status_values():
    """Claim status must be one of the allowed values."""
    return (
        _count("""
        SELECT COUNT(*)
        FROM RCM_ANALYTICS.SILVER.CLAIMS
        WHERE CLAIM_STATUS NOT IN ('Paid', 'Partially Paid', 'Denied', 'Pending', 'Appealed')
    """)
        == 0
    )


def validate_clean_claim_boolean():
    """is_clean_claim must be 0 or 1."""
    return (
        _count("""
        SELECT COUNT(*)
        FROM RCM_ANALYTICS.SILVER.CLAIMS
        WHERE IS_CLEAN_CLAIM NOT IN (0, 1)
    """)
        == 0
    )


def validate_payment_accuracy_boolean():
    """is_accurate_payment must be 0 or 1."""
    return (
        _count("""
        SELECT COUNT(*)
        FROM RCM_ANALYTICS.SILVER.PAYMENTS
        WHERE IS_ACCURATE_PAYMENT NOT IN (0, 1)
    """)
        == 0
    )


def validate_no_null_primary_keys():
    """Primary key columns must not be NULL."""
    checks = [
        "SELECT COUNT(*) FROM RCM_ANALYTICS.SILVER.PAYERS WHERE PAYER_ID IS NULL",
        "SELECT COUNT(*) FROM RCM_ANALYTICS.SILVER.PATIENTS WHERE PATIENT_ID IS NULL",
        "SELECT COUNT(*) FROM RCM_ANALYTICS.SILVER.PROVIDERS WHERE PROVIDER_ID IS NULL",
        "SELECT COUNT(*) FROM RCM_ANALYTICS.SILVER.ENCOUNTERS WHERE ENCOUNTER_ID IS NULL",
        "SELECT COUNT(*) FROM RCM_ANALYTICS.SILVER.CLAIMS WHERE CLAIM_ID IS NULL",
        "SELECT COUNT(*) FROM RCM_ANALYTICS.SILVER.CHARGES WHERE CHARGE_ID IS NULL",
        "SELECT COUNT(*) FROM RCM_ANALYTICS.SILVER.PAYMENTS WHERE PAYMENT_ID IS NULL",
        "SELECT COUNT(*) FROM RCM_ANALYTICS.SILVER.DENIALS WHERE DENIAL_ID IS NULL",
        "SELECT COUNT(*) FROM RCM_ANALYTICS.SILVER.ADJUSTMENTS WHERE ADJUSTMENT_ID IS NULL",
    ]
    return all(_count(sql) == 0 for sql in checks)


def run_all_validators():
    """Run all validators and return a list of issue dicts (only failures).

    Each issue dict has keys: level, table, message.
    Returns an empty list if all checks pass.
    """
    validators = [
        validate_payers_not_empty,
        validate_patients_not_empty,
        validate_providers_not_empty,
        validate_encounters_not_empty,
        validate_charges_not_empty,
        validate_claims_not_empty,
        validate_payments_not_empty,
        validate_denials_not_empty,
        validate_adjustments_not_empty,
        validate_operating_costs_not_empty,
        validate_claims_have_valid_payers,
        validate_claims_have_valid_encounters,
        validate_claims_have_valid_patients,
        validate_payments_have_valid_claims,
        validate_denials_have_valid_claims,
        validate_adjustments_have_valid_claims,
        validate_encounters_have_valid_patients,
        validate_encounters_have_valid_providers,
        validate_no_negative_charges,
        validate_no_negative_payments,
        validate_no_negative_denied_amounts,
        validate_claim_status_values,
        validate_clean_claim_boolean,
        validate_payment_accuracy_boolean,
        validate_no_null_primary_keys,
    ]
    issues = []
    for fn in validators:
        try:
            passed = fn()
            if not passed:
                # Extract table name from function name (validate_X_not_empty -> X)
                name = fn.__name__.replace("validate_", "")
                issues.append(
                    {
                        "level": "error",
                        "table": name,
                        "message": fn.__doc__ or f"Validation failed: {fn.__name__}",
                    }
                )
        except Exception as e:
            issues.append(
                {
                    "level": "warning",
                    "table": fn.__name__,
                    "message": f"Check error: {str(e)}",
                }
            )
    return issues
