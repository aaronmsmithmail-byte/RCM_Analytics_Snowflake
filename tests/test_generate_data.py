"""
Test that generate_sample_data.py produces all expected CSV files.
"""

import os
import subprocess

import pytest

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")

EXPECTED_FILES = [
    ("payers.csv", 10),
    ("patients.csv", 500),
    ("providers.csv", 25),
    ("encounters.csv", 3000),
    ("claims.csv", 2800),
    ("operating_costs.csv", 24),
]


@pytest.fixture(scope="module", autouse=True)
def generate_data():
    """Run the data generator once for all tests in this module."""
    subprocess.run(
        ["python", "generate_sample_data.py"],
        check=True,
        capture_output=True,
    )


@pytest.mark.parametrize("filename,expected_rows", EXPECTED_FILES)
def test_csv_exists_and_has_rows(filename, expected_rows):
    path = os.path.join(DATA_DIR, filename)
    assert os.path.isfile(path), f"Missing: {filename}"
    with open(path) as f:
        lines = f.readlines()
    # Header + data rows
    actual_rows = len(lines) - 1
    assert actual_rows == expected_rows, f"{filename}: expected {expected_rows} rows, got {actual_rows}"


def test_all_10_csv_files_exist():
    expected = [
        "payers.csv",
        "patients.csv",
        "providers.csv",
        "encounters.csv",
        "charges.csv",
        "claims.csv",
        "payments.csv",
        "denials.csv",
        "adjustments.csv",
        "operating_costs.csv",
    ]
    for filename in expected:
        path = os.path.join(DATA_DIR, filename)
        assert os.path.isfile(path), f"Missing CSV: {filename}"
