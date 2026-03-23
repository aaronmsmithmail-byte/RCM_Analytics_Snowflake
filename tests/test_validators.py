"""Unit tests for src/validators.py — all 6 validation check functions."""

import pandas as pd
import pytest

from src.validators import (
    validate_all,
    _check_negative_amounts,
    _check_orphaned_keys,
    _check_nulls,
    _check_date_ranges,
    _check_claim_status_values,
    _check_boolean_columns,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def clean_data():
    """A minimal dataset that should produce zero validation issues."""
    return {
        "payers": pd.DataFrame({
            "payer_id": ["PYR001", "PYR002"],
            "payer_name": ["Aetna", "Medicare"],
            "payer_type": ["Commercial", "Government"],
        }),
        "patients": pd.DataFrame({
            "patient_id": ["PAT001", "PAT002"],
            "primary_payer_id": ["PYR001", "PYR002"],
        }),
        "providers": pd.DataFrame({
            "provider_id": ["PROV01", "PROV02"],
            "provider_name": ["Dr. Smith", "Dr. Jones"],
        }),
        "encounters": pd.DataFrame({
            "encounter_id": ["ENC001", "ENC002"],
            "patient_id": ["PAT001", "PAT002"],
            "provider_id": ["PROV01", "PROV02"],
            "date_of_service": pd.to_datetime(["2024-06-01", "2024-06-15"]),
        }),
        "claims": pd.DataFrame({
            "claim_id": ["CLM001", "CLM002"],
            "encounter_id": ["ENC001", "ENC002"],
            "patient_id": ["PAT001", "PAT002"],
            "payer_id": ["PYR001", "PYR002"],
            "date_of_service": pd.to_datetime(["2024-06-01", "2024-06-15"]),
            "submission_date": pd.to_datetime(["2024-06-03", "2024-06-17"]),
            "total_charge_amount": [500.0, 1200.0],
            "claim_status": ["Paid", "Partially Paid"],
            "is_clean_claim": [True, True],
        }),
        "payments": pd.DataFrame({
            "payment_id": ["PAY001", "PAY002"],
            "claim_id": ["CLM001", "CLM002"],
            "payment_amount": [450.0, 600.0],
            "allowed_amount": [480.0, 650.0],
            "payment_date": pd.to_datetime(["2024-07-01", "2024-07-15"]),
            "is_accurate_payment": [True, True],
        }),
        "denials": pd.DataFrame({
            "denial_id": ["DEN001"],
            "claim_id": ["CLM002"],
            "denied_amount": [600.0],
            "recovered_amount": [0.0],
        }),
        "adjustments": pd.DataFrame({
            "adjustment_id": ["ADJ001"],
            "claim_id": ["CLM001"],
            "adjustment_amount": [50.0],
        }),
        "charges": pd.DataFrame({
            "charge_id": ["CHG001"],
            "service_date": pd.to_datetime(["2024-06-01"]),
            "post_date": pd.to_datetime(["2024-06-02"]),
        }),
        "operating_costs": pd.DataFrame({
            "period": ["2024-06"],
            "total_rcm_cost": [15000.0],
        }),
    }


# ── Tests: validate_all ───────────────────────────────────────────────────────

class TestValidateAll:
    def test_returns_list(self, clean_data):
        result = validate_all(clean_data)
        assert isinstance(result, list)

    def test_clean_data_returns_no_issues(self, clean_data):
        issues = validate_all(clean_data)
        assert issues == []

    def test_issues_have_required_keys(self, clean_data):
        clean_data["claims"].loc[0, "total_charge_amount"] = -100.0
        issues = validate_all(clean_data)
        assert len(issues) > 0
        for issue in issues:
            assert "level" in issue
            assert "table" in issue
            assert "message" in issue

    def test_empty_data_dict_returns_no_errors(self):
        issues = validate_all({})
        assert isinstance(issues, list)
        assert issues == []


# ── Tests: _check_negative_amounts ───────────────────────────────────────────

class TestCheckNegativeAmounts:
    def test_no_issues_when_all_positive(self, clean_data):
        issues = _check_negative_amounts(clean_data)
        assert issues == []

    def test_warns_on_negative_charge_amount(self, clean_data):
        clean_data["claims"].loc[0, "total_charge_amount"] = -50.0
        issues = _check_negative_amounts(clean_data)
        assert len(issues) == 1
        assert issues[0]["level"] == "warning"
        assert issues[0]["table"] == "claims"
        assert "total_charge_amount" in issues[0]["message"]

    def test_warns_on_negative_payment_amount(self, clean_data):
        clean_data["payments"].loc[0, "payment_amount"] = -10.0
        issues = _check_negative_amounts(clean_data)
        tables = [i["table"] for i in issues]
        assert "payments" in tables

    def test_warns_on_negative_denied_amount(self, clean_data):
        clean_data["denials"].loc[0, "denied_amount"] = -200.0
        issues = _check_negative_amounts(clean_data)
        assert any(i["table"] == "denials" for i in issues)

    def test_warns_on_negative_rcm_cost(self, clean_data):
        clean_data["operating_costs"].loc[0, "total_rcm_cost"] = -1000.0
        issues = _check_negative_amounts(clean_data)
        assert any(i["table"] == "operating_costs" for i in issues)

    def test_skips_missing_table(self):
        issues = _check_negative_amounts({})
        assert issues == []

    def test_counts_multiple_negatives(self, clean_data):
        clean_data["payments"]["payment_amount"] = [-10.0, -20.0]
        issues = _check_negative_amounts(clean_data)
        pay_issues = [i for i in issues if i["table"] == "payments" and "payment_amount" in i["message"]]
        assert len(pay_issues) == 1
        assert "2" in pay_issues[0]["message"]


# ── Tests: _check_orphaned_keys ───────────────────────────────────────────────

class TestCheckOrphanedKeys:
    def test_no_issues_when_all_keys_valid(self, clean_data):
        issues = _check_orphaned_keys(clean_data)
        assert issues == []

    def test_warns_on_payment_with_unknown_claim(self, clean_data):
        clean_data["payments"].loc[0, "claim_id"] = "GHOST_CLAIM"
        issues = _check_orphaned_keys(clean_data)
        assert any("payments" in i["table"] for i in issues)

    def test_warns_on_denial_with_unknown_claim(self, clean_data):
        clean_data["denials"].loc[0, "claim_id"] = "MISSING"
        issues = _check_orphaned_keys(clean_data)
        assert any(i["table"] == "denials" for i in issues)

    def test_warns_on_claim_with_unknown_payer(self, clean_data):
        clean_data["claims"].loc[0, "payer_id"] = "PYR999"
        issues = _check_orphaned_keys(clean_data)
        assert any(i["table"] == "claims" for i in issues)

    def test_warns_on_encounter_with_unknown_patient(self, clean_data):
        clean_data["encounters"].loc[0, "patient_id"] = "GHOST_PAT"
        issues = _check_orphaned_keys(clean_data)
        assert any(i["table"] == "encounters" for i in issues)

    def test_issue_level_is_warning(self, clean_data):
        clean_data["payments"].loc[0, "claim_id"] = "GHOST"
        issues = _check_orphaned_keys(clean_data)
        for i in issues:
            assert i["level"] == "warning"

    def test_skips_when_parent_table_missing(self, clean_data):
        del clean_data["claims"]
        issues = _check_orphaned_keys(clean_data)
        # payments/denials/adjustments checks should be skipped; no crash
        assert isinstance(issues, list)


# ── Tests: _check_nulls ───────────────────────────────────────────────────────

class TestCheckNulls:
    def test_no_issues_when_all_required_filled(self, clean_data):
        issues = _check_nulls(clean_data)
        assert issues == []

    def test_errors_on_null_claim_id(self, clean_data):
        clean_data["claims"].loc[0, "claim_id"] = None
        issues = _check_nulls(clean_data)
        assert any(i["level"] == "error" and i["table"] == "claims" for i in issues)

    def test_errors_on_null_payment_amount(self, clean_data):
        clean_data["payments"].loc[0, "payment_amount"] = None
        issues = _check_nulls(clean_data)
        assert any(i["level"] == "error" and i["table"] == "payments" for i in issues)

    def test_errors_on_null_encounter_date(self, clean_data):
        clean_data["encounters"].loc[0, "date_of_service"] = None
        issues = _check_nulls(clean_data)
        assert any(i["level"] == "error" and i["table"] == "encounters" for i in issues)

    def test_error_message_names_column(self, clean_data):
        clean_data["claims"].loc[0, "claim_status"] = None
        issues = _check_nulls(clean_data)
        claim_issues = [i for i in issues if i["table"] == "claims"]
        assert any("claim_status" in i["message"] for i in claim_issues)

    def test_skips_missing_table(self):
        issues = _check_nulls({})
        assert issues == []


# ── Tests: _check_date_ranges ─────────────────────────────────────────────────

class TestCheckDateRanges:
    def test_no_issues_with_valid_dates(self, clean_data):
        issues = _check_date_ranges(clean_data)
        assert issues == []

    def test_warns_on_date_before_2020(self, clean_data):
        clean_data["claims"].loc[0, "date_of_service"] = pd.Timestamp("2019-12-31")
        issues = _check_date_ranges(clean_data)
        assert any(i["table"] == "claims" for i in issues)

    def test_warns_on_date_after_2030(self, clean_data):
        clean_data["payments"].loc[0, "payment_date"] = pd.Timestamp("2031-01-01")
        issues = _check_date_ranges(clean_data)
        assert any(i["table"] == "payments" for i in issues)

    def test_issue_level_is_warning(self, clean_data):
        clean_data["claims"].loc[0, "date_of_service"] = pd.Timestamp("2015-01-01")
        issues = _check_date_ranges(clean_data)
        for i in issues:
            assert i["level"] == "warning"

    def test_tolerates_nat_values(self, clean_data):
        clean_data["claims"].loc[0, "submission_date"] = pd.NaT
        issues = _check_date_ranges(clean_data)
        # NaT should not be counted as out-of-range
        assert not any(
            "submission_date" in i["message"] for i in issues
            if i["table"] == "claims"
        )


# ── Tests: _check_claim_status_values ────────────────────────────────────────

class TestCheckClaimStatusValues:
    def test_no_issues_with_all_valid_statuses(self):
        for status in ["Paid", "Denied", "Appealed", "Pending", "Partially Paid"]:
            data = {"claims": pd.DataFrame({"claim_status": [status]})}
            issues = _check_claim_status_values(data)
            assert issues == [], f"Status '{status}' should be valid but got: {issues}"

    def test_warns_on_unknown_status(self):
        data = {"claims": pd.DataFrame({"claim_status": ["Unknown", "Paid"]})}
        issues = _check_claim_status_values(data)
        assert len(issues) == 1
        assert issues[0]["level"] == "warning"
        assert "Unknown" in issues[0]["message"]

    def test_message_lists_bad_values(self):
        data = {"claims": pd.DataFrame({"claim_status": ["Bad1", "Bad2", "Paid"]})}
        issues = _check_claim_status_values(data)
        assert len(issues) == 1
        assert "Bad1" in issues[0]["message"] or "Bad2" in issues[0]["message"]

    def test_no_issues_when_claims_absent(self):
        issues = _check_claim_status_values({})
        assert issues == []

    def test_no_issues_when_column_absent(self):
        data = {"claims": pd.DataFrame({"claim_id": [1]})}
        issues = _check_claim_status_values(data)
        assert issues == []

    def test_partially_paid_is_valid(self):
        data = {"claims": pd.DataFrame({"claim_status": ["Partially Paid"] * 100})}
        issues = _check_claim_status_values(data)
        assert issues == []


# ── Tests: _check_boolean_columns ─────────────────────────────────────────────

class TestCheckBooleanColumns:
    def test_no_issues_when_all_filled(self, clean_data):
        issues = _check_boolean_columns(clean_data)
        assert issues == []

    def test_warns_on_null_is_clean_claim(self, clean_data):
        clean_data["claims"]["is_clean_claim"] = clean_data["claims"]["is_clean_claim"].astype(object)
        clean_data["claims"].loc[0, "is_clean_claim"] = None
        issues = _check_boolean_columns(clean_data)
        assert any(
            i["table"] == "claims" and "is_clean_claim" in i["message"]
            for i in issues
        )

    def test_warns_on_null_is_accurate_payment(self, clean_data):
        clean_data["payments"]["is_accurate_payment"] = clean_data["payments"]["is_accurate_payment"].astype(object)
        clean_data["payments"].loc[0, "is_accurate_payment"] = None
        issues = _check_boolean_columns(clean_data)
        assert any(
            i["table"] == "payments" and "is_accurate_payment" in i["message"]
            for i in issues
        )

    def test_issue_level_is_warning(self, clean_data):
        clean_data["claims"]["is_clean_claim"] = clean_data["claims"]["is_clean_claim"].astype(object)
        clean_data["claims"].loc[0, "is_clean_claim"] = None
        issues = _check_boolean_columns(clean_data)
        for i in issues:
            assert i["level"] == "warning"

    def test_skips_missing_table(self):
        issues = _check_boolean_columns({})
        assert issues == []
