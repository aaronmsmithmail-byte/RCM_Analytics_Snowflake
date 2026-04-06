"""
RCM Performance Metrics — Snowflake SQL Pipeline
=================================================

All 26 Revenue Cycle Management KPIs as parameterized SQL queries
executing against the Silver layer in Snowflake via Snowpark session.
"""

from dataclasses import dataclass

import numpy as np
import pandas as pd
from snowflake.snowpark.context import get_active_session

# ===========================================================================
# Filter parameter container
# ===========================================================================


@dataclass
class FilterParams:
    """All four sidebar filter dimensions in one object."""

    start_date: str
    end_date: str
    payer_id: str | None = None
    department: str | None = None
    encounter_type: str | None = None


# ===========================================================================
# Internal helpers
# ===========================================================================


def _get_session():
    return get_active_session()


def _esc(val):
    """Escape a string value for safe inclusion in SQL."""
    if val is None:
        return None
    return str(val).replace("'", "''")


def _cte(p: FilterParams):
    """Return a CTE SQL string for filtered_claims using Snowflake syntax."""
    clauses = [f"c.DATE_OF_SERVICE >= '{_esc(p.start_date)}'", f"c.DATE_OF_SERVICE <= '{_esc(p.end_date)}'"]
    if p.payer_id:
        clauses.append(f"c.PAYER_ID = '{_esc(p.payer_id)}'")
    if p.department:
        clauses.append(f"e.DEPARTMENT = '{_esc(p.department)}'")
    if p.encounter_type:
        clauses.append(f"e.ENCOUNTER_TYPE = '{_esc(p.encounter_type)}'")
    where = " AND ".join(clauses)
    return f"""WITH filtered_claims AS (
    SELECT c.*
    FROM RCM_ANALYTICS.SILVER.CLAIMS c
    LEFT JOIN RCM_ANALYTICS.SILVER.ENCOUNTERS e ON c.ENCOUNTER_ID = e.ENCOUNTER_ID
    WHERE {where}
)
"""


def _query(sql):
    """Execute SQL via Snowpark and return a pandas DataFrame with lowercase columns."""
    import decimal

    session = _get_session()
    df = session.sql(sql).to_pandas()
    df.columns = [c.lower() for c in df.columns]
    # Coerce Decimal → float for columns that Snowpark returns as decimal.Decimal.
    # Only target actual Decimal columns to avoid converting string codes (e.g.
    # cpt_code "99213") into integers, which breaks string concatenation.
    for col in df.columns:
        if len(df) > 0 and isinstance(df[col].dropna().iloc[0] if not df[col].dropna().empty else None, decimal.Decimal):
            df[col] = df[col].astype(float)
    return df


def _empty_trend(*columns):
    return pd.DataFrame(columns=list(columns))


def _set_period_index(df):
    df = df.set_index("period")
    df.index.name = "year_month"
    return df


# ===========================================================================
# 1. DAYS IN ACCOUNTS RECEIVABLE (DAR)
# ===========================================================================


def query_days_in_ar(p: FilterParams):
    cte = _cte(p)
    sql = (
        cte
        + """
, monthly_charges AS (
    SELECT TO_CHAR(TRY_TO_DATE(date_of_service, 'YYYY-MM-DD'), 'YYYY-MM') AS period,
           SUM(total_charge_amount) AS charges
    FROM filtered_claims
    GROUP BY TO_CHAR(TRY_TO_DATE(date_of_service, 'YYYY-MM-DD'), 'YYYY-MM')
), monthly_payments AS (
    SELECT TO_CHAR(TRY_TO_DATE(fc.date_of_service, 'YYYY-MM-DD'), 'YYYY-MM') AS period,
           COALESCE(SUM(p.payment_amount), 0) AS payments
    FROM filtered_claims fc
    LEFT JOIN RCM_ANALYTICS.SILVER.PAYMENTS p ON fc.claim_id = p.claim_id
    GROUP BY TO_CHAR(TRY_TO_DATE(fc.date_of_service, 'YYYY-MM-DD'), 'YYYY-MM')
)
SELECT c.period, c.charges, COALESCE(mp.payments, 0) AS payments
FROM monthly_charges c
LEFT JOIN monthly_payments mp ON c.period = mp.period
ORDER BY c.period
"""
    )
    df = _query(sql)
    if df.empty:
        return 0.0, _empty_trend("charges", "payments", "ar_balance", "days_in_ar")
    df["ar_balance"] = df["charges"].cumsum() - df["payments"].cumsum()
    df["avg_daily_charges"] = df["charges"] / 30
    df["days_in_ar"] = np.where(df["avg_daily_charges"] > 0, df["ar_balance"] / df["avg_daily_charges"], 0)
    df = _set_period_index(df)
    overall_dar = df["days_in_ar"].iloc[-1] if len(df) > 0 else 0.0
    return round(float(overall_dar), 1), df[["charges", "payments", "ar_balance", "days_in_ar"]]


# ===========================================================================
# 2. NET COLLECTION RATE (NCR)
# ===========================================================================


def query_net_collection_rate(p: FilterParams):
    cte = _cte(p)
    sql = (
        cte
        + """
, monthly_charges AS (
    SELECT TO_CHAR(TRY_TO_DATE(date_of_service, 'YYYY-MM-DD'), 'YYYY-MM') AS period,
           SUM(total_charge_amount) AS charges
    FROM filtered_claims
    GROUP BY TO_CHAR(TRY_TO_DATE(date_of_service, 'YYYY-MM-DD'), 'YYYY-MM')
), monthly_payments AS (
    SELECT TO_CHAR(TRY_TO_DATE(fc.date_of_service, 'YYYY-MM-DD'), 'YYYY-MM') AS period,
           COALESCE(SUM(p.payment_amount), 0) AS payments
    FROM filtered_claims fc
    LEFT JOIN RCM_ANALYTICS.SILVER.PAYMENTS p ON fc.claim_id = p.claim_id
    GROUP BY TO_CHAR(TRY_TO_DATE(fc.date_of_service, 'YYYY-MM-DD'), 'YYYY-MM')
), monthly_contractual AS (
    SELECT TO_CHAR(TRY_TO_DATE(fc.date_of_service, 'YYYY-MM-DD'), 'YYYY-MM') AS period,
           COALESCE(SUM(CASE WHEN a.adjustment_type_code = 'CONTRACTUAL'
                             THEN a.adjustment_amount ELSE 0 END), 0) AS contractual_adj
    FROM filtered_claims fc
    LEFT JOIN RCM_ANALYTICS.SILVER.ADJUSTMENTS a ON fc.claim_id = a.claim_id
    GROUP BY TO_CHAR(TRY_TO_DATE(fc.date_of_service, 'YYYY-MM-DD'), 'YYYY-MM')
)
SELECT c.period, c.charges, COALESCE(mp.payments, 0) AS payments,
       COALESCE(mc.contractual_adj, 0) AS contractual_adj
FROM monthly_charges c
LEFT JOIN monthly_payments mp ON c.period = mp.period
LEFT JOIN monthly_contractual mc ON c.period = mc.period
ORDER BY c.period
"""
    )
    df = _query(sql)
    if df.empty:
        return 0.0, _empty_trend("charges", "payments", "contractual_adj", "ncr")
    total_charges = df["charges"].sum()
    total_payments = df["payments"].sum()
    total_contractual = df["contractual_adj"].sum()
    denominator = total_charges - total_contractual
    ncr = (total_payments / denominator * 100) if denominator > 0 else 0.0
    df["ncr"] = np.where(
        (df["charges"] - df["contractual_adj"]) > 0, df["payments"] / (df["charges"] - df["contractual_adj"]) * 100, 0
    )
    df = _set_period_index(df)
    return round(float(ncr), 2), df


# ===========================================================================
# 3. GROSS COLLECTION RATE (GCR)
# ===========================================================================


def query_gross_collection_rate(p: FilterParams):
    cte = _cte(p)
    sql = (
        cte
        + """
, monthly_charges AS (
    SELECT TO_CHAR(TRY_TO_DATE(date_of_service, 'YYYY-MM-DD'), 'YYYY-MM') AS period,
           SUM(total_charge_amount) AS charges
    FROM filtered_claims
    GROUP BY TO_CHAR(TRY_TO_DATE(date_of_service, 'YYYY-MM-DD'), 'YYYY-MM')
), monthly_payments AS (
    SELECT TO_CHAR(TRY_TO_DATE(fc.date_of_service, 'YYYY-MM-DD'), 'YYYY-MM') AS period,
           COALESCE(SUM(p.payment_amount), 0) AS payments
    FROM filtered_claims fc
    LEFT JOIN RCM_ANALYTICS.SILVER.PAYMENTS p ON fc.claim_id = p.claim_id
    GROUP BY TO_CHAR(TRY_TO_DATE(fc.date_of_service, 'YYYY-MM-DD'), 'YYYY-MM')
)
SELECT c.period, c.charges, COALESCE(mp.payments, 0) AS payments
FROM monthly_charges c
LEFT JOIN monthly_payments mp ON c.period = mp.period
ORDER BY c.period
"""
    )
    df = _query(sql)
    if df.empty:
        return 0.0, _empty_trend("charges", "payments", "gcr")
    total_charges = df["charges"].sum()
    total_payments = df["payments"].sum()
    gcr = (total_payments / total_charges * 100) if total_charges > 0 else 0.0
    df["gcr"] = np.where(df["charges"] > 0, df["payments"] / df["charges"] * 100, 0)
    df = _set_period_index(df)
    return round(float(gcr), 2), df


# ===========================================================================
# 4. CLEAN CLAIM RATE (CCR)
# ===========================================================================


def query_clean_claim_rate(p: FilterParams):
    cte = _cte(p)
    sql = (
        cte
        + """
SELECT TO_CHAR(TRY_TO_DATE(submission_date, 'YYYY-MM-DD'), 'YYYY-MM') AS period,
       COUNT(*) AS total_claims,
       SUM(is_clean_claim) AS clean_claims
FROM filtered_claims
GROUP BY TO_CHAR(TRY_TO_DATE(submission_date, 'YYYY-MM-DD'), 'YYYY-MM')
ORDER BY period
"""
    )
    df = _query(sql)
    if df.empty:
        return 0.0, _empty_trend("total_claims", "clean_claims", "ccr")
    total = df["total_claims"].sum()
    clean = df["clean_claims"].sum()
    ccr = (clean / total * 100) if total > 0 else 0.0
    df["ccr"] = np.where(df["total_claims"] > 0, df["clean_claims"] / df["total_claims"] * 100, 0)
    df = _set_period_index(df)
    return round(float(ccr), 2), df


# ===========================================================================
# 5. CLAIM DENIAL RATE
# ===========================================================================


def query_denial_rate(p: FilterParams):
    cte = _cte(p)
    sql = (
        cte
        + """
SELECT TO_CHAR(TRY_TO_DATE(submission_date, 'YYYY-MM-DD'), 'YYYY-MM') AS period,
       COUNT(*) AS total_claims,
       SUM(CASE WHEN claim_status IN ('Denied', 'Appealed') THEN 1 ELSE 0 END) AS denied_claims
FROM filtered_claims
GROUP BY TO_CHAR(TRY_TO_DATE(submission_date, 'YYYY-MM-DD'), 'YYYY-MM')
ORDER BY period
"""
    )
    df = _query(sql)
    if df.empty:
        return 0.0, _empty_trend("total_claims", "denied_claims", "denial_rate")
    total = df["total_claims"].sum()
    denied = df["denied_claims"].sum()
    rate = (denied / total * 100) if total > 0 else 0.0
    df["denial_rate"] = np.where(df["total_claims"] > 0, df["denied_claims"] / df["total_claims"] * 100, 0)
    df = _set_period_index(df)
    return round(float(rate), 2), df


# ===========================================================================
# 6. DENIAL REASONS BREAKDOWN
# ===========================================================================


def query_denial_reasons(p: FilterParams):
    cte = _cte(p)
    sql = (
        cte
        + """
SELECT d.denial_reason_code,
       d.denial_reason_description,
       COUNT(*) AS count,
       SUM(d.denied_amount) AS total_denied_amount,
       COALESCE(SUM(d.recovered_amount), 0) AS total_recovered
FROM filtered_claims fc
JOIN RCM_ANALYTICS.SILVER.DENIALS d ON fc.claim_id = d.claim_id
GROUP BY d.denial_reason_code, d.denial_reason_description
ORDER BY count DESC
"""
    )
    df = _query(sql)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "denial_reason_code",
                "denial_reason_description",
                "count",
                "total_denied_amount",
                "total_recovered",
                "recovery_rate",
            ]
        )
    df["recovery_rate"] = np.where(
        df["total_denied_amount"] > 0, df["total_recovered"] / df["total_denied_amount"] * 100, 0
    )
    return df


# ===========================================================================
# 7. FIRST-PASS RESOLUTION RATE (FPRR)
# ===========================================================================


def query_first_pass_rate(p: FilterParams):
    cte = _cte(p)
    sql = (
        cte
        + """
SELECT TO_CHAR(TRY_TO_DATE(submission_date, 'YYYY-MM-DD'), 'YYYY-MM') AS period,
       COUNT(*) AS total,
       SUM(CASE WHEN claim_status = 'Paid' THEN 1 ELSE 0 END) AS paid
FROM filtered_claims
GROUP BY TO_CHAR(TRY_TO_DATE(submission_date, 'YYYY-MM-DD'), 'YYYY-MM')
ORDER BY period
"""
    )
    df = _query(sql)
    if df.empty:
        return 0.0, _empty_trend("total", "paid", "fpr")
    total = df["total"].sum()
    paid = df["paid"].sum()
    rate = (paid / total * 100) if total > 0 else 0.0
    df["fpr"] = np.where(df["total"] > 0, df["paid"] / df["total"] * 100, 0)
    df = _set_period_index(df)
    return round(float(rate), 2), df


# ===========================================================================
# 8. AVERAGE CHARGE LAG (Days)
# ===========================================================================


def query_charge_lag(p: FilterParams):
    cte = _cte(p)
    sql = (
        cte
        + """
SELECT TO_CHAR(TRY_TO_DATE(ch.service_date, 'YYYY-MM-DD'), 'YYYY-MM') AS period,
       DATEDIFF('day', TRY_TO_DATE(ch.service_date, 'YYYY-MM-DD'), TRY_TO_DATE(ch.post_date, 'YYYY-MM-DD')) AS lag_days
FROM RCM_ANALYTICS.SILVER.CHARGES ch
WHERE ch.encounter_id IN (SELECT DISTINCT encounter_id FROM filtered_claims)
  AND ch.post_date IS NOT NULL
  AND ch.service_date IS NOT NULL
"""
    )
    df = _query(sql)
    if df.empty:
        return 0.0, pd.Series(dtype=float), pd.Series(dtype=float)
    avg_lag = df["lag_days"].mean()
    trend = df.groupby("period")["lag_days"].mean()
    trend.index.name = "year_month"
    distribution = df["lag_days"].value_counts().sort_index()
    return round(float(avg_lag), 1), trend, distribution


# ===========================================================================
# 9. COST TO COLLECT (CTC)
# ===========================================================================


def query_cost_to_collect(p: FilterParams):
    cte = _cte(p)
    sql = (
        cte
        + """
SELECT TO_CHAR(TRY_TO_DATE(fc.date_of_service, 'YYYY-MM-DD'), 'YYYY-MM') AS period,
       COALESCE(SUM(p.payment_amount), 0) AS collections
FROM filtered_claims fc
LEFT JOIN RCM_ANALYTICS.SILVER.PAYMENTS p ON fc.claim_id = p.claim_id
GROUP BY TO_CHAR(TRY_TO_DATE(fc.date_of_service, 'YYYY-MM-DD'), 'YYYY-MM')
ORDER BY period
"""
    )
    collections_df = _query(sql)
    if collections_df.empty:
        return 0.0, _empty_trend("rcm_cost", "collections", "cost_to_collect_pct")

    costs_df = _query("SELECT period, total_rcm_cost AS rcm_cost FROM RCM_ANALYTICS.SILVER.OPERATING_COSTS")
    trend = collections_df.merge(costs_df, on="period", how="left").fillna(0)
    trend["cost_to_collect_pct"] = np.where(trend["collections"] > 0, trend["rcm_cost"] / trend["collections"] * 100, 0)
    total_cost = costs_df["rcm_cost"].sum()
    total_collected = collections_df["collections"].sum()
    ctc = (total_cost / total_collected * 100) if total_collected > 0 else 0.0
    trend = _set_period_index(trend)
    return round(float(ctc), 2), trend


# ===========================================================================
# 10. A/R AGING BUCKETS
# ===========================================================================


def query_ar_aging(p: FilterParams):
    empty_summary = pd.DataFrame(
        {"claim_count": 0, "total_ar": 0.0, "pct_of_total": 0.0}, index=["0-30", "31-60", "61-90", "91-120", "120+"]
    )
    cte = _cte(p)
    sql = (
        cte
        + """
SELECT fc.claim_id,
       fc.date_of_service,
       fc.total_charge_amount - COALESCE(SUM(p.payment_amount), 0) AS ar_balance,
       DATEDIFF('day', TRY_TO_DATE(fc.date_of_service, 'YYYY-MM-DD'), CURRENT_DATE()) AS days_outstanding
FROM filtered_claims fc
LEFT JOIN RCM_ANALYTICS.SILVER.PAYMENTS p ON fc.claim_id = p.claim_id
GROUP BY fc.claim_id, fc.date_of_service, fc.total_charge_amount
HAVING ar_balance > 0
"""
    )
    df = _query(sql)
    if df.empty:
        return empty_summary, 0.0

    def _bucket(days):
        if days <= 30:
            return "0-30"
        elif days <= 60:
            return "31-60"
        elif days <= 90:
            return "61-90"
        elif days <= 120:
            return "91-120"
        return "120+"

    df["aging_bucket"] = df["days_outstanding"].apply(_bucket)
    summary = (
        df.groupby("aging_bucket")
        .agg(claim_count=("claim_id", "count"), total_ar=("ar_balance", "sum"))
        .reindex(["0-30", "31-60", "61-90", "91-120", "120+"])
        .fillna(0)
    )
    total_ar = summary["total_ar"].sum()
    summary["pct_of_total"] = np.where(total_ar > 0, summary["total_ar"] / total_ar * 100, 0)
    return summary, float(total_ar)


# ===========================================================================
# 11. PAYMENT ACCURACY RATE
# ===========================================================================


def query_payment_accuracy(p: FilterParams):
    cte = _cte(p)
    sql = (
        cte
        + """
SELECT COUNT(*) AS total,
       SUM(p.is_accurate_payment) AS accurate
FROM filtered_claims fc
JOIN RCM_ANALYTICS.SILVER.PAYMENTS p ON fc.claim_id = p.claim_id
"""
    )
    df = _query(sql)
    if df.empty or df["total"].iloc[0] == 0:
        return 0.0
    total = df["total"].iloc[0]
    accurate = df["accurate"].iloc[0] or 0
    return round(float(accurate / total * 100), 2)


# ===========================================================================
# 12. BAD DEBT RATE
# ===========================================================================


def query_bad_debt_rate(p: FilterParams):
    cte = _cte(p)
    sql = (
        cte
        + """
SELECT
    (SELECT COALESCE(SUM(total_charge_amount), 0) FROM filtered_claims) AS total_charges,
    COALESCE((
        SELECT SUM(a.adjustment_amount)
        FROM RCM_ANALYTICS.SILVER.ADJUSTMENTS a
        WHERE a.adjustment_type_code = 'WRITEOFF'
          AND a.claim_id IN (SELECT claim_id FROM filtered_claims)
    ), 0) AS bad_debt
"""
    )
    df = _query(sql)
    if df.empty:
        return 0.0, 0.0, 0.0
    total_charges = df["total_charges"].iloc[0] or 0.0
    bad_debt = df["bad_debt"].iloc[0] or 0.0
    rate = (bad_debt / total_charges * 100) if total_charges > 0 else 0.0
    return round(float(rate), 2), float(bad_debt), float(total_charges)


# ===========================================================================
# 13. APPEAL SUCCESS RATE
# ===========================================================================


def query_appeal_success_rate(p: FilterParams):
    cte = _cte(p)
    sql = (
        cte
        + """
SELECT d.appeal_status,
       COUNT(*) AS n
FROM filtered_claims fc
JOIN RCM_ANALYTICS.SILVER.DENIALS d ON fc.claim_id = d.claim_id
WHERE d.appeal_status IN ('Won', 'Lost', 'In Progress')
GROUP BY d.appeal_status
"""
    )
    df = _query(sql)
    if df.empty:
        return 0.0, 0, 0
    total_appealed = int(df["n"].sum())
    won = int(df.loc[df["appeal_status"] == "Won", "n"].sum())
    rate = (won / total_appealed * 100) if total_appealed > 0 else 0.0
    return round(float(rate), 2), total_appealed, won


# ===========================================================================
# 14. AVERAGE REIMBURSEMENT PER ENCOUNTER
# ===========================================================================


def query_avg_reimbursement(p: FilterParams):
    cte = _cte(p)
    sql = (
        cte
        + """
SELECT TO_CHAR(TRY_TO_DATE(fc.date_of_service, 'YYYY-MM-DD'), 'YYYY-MM') AS period,
       COALESCE(SUM(p.payment_amount), 0) AS payment_amount
FROM filtered_claims fc
LEFT JOIN RCM_ANALYTICS.SILVER.PAYMENTS p ON fc.claim_id = p.claim_id
GROUP BY fc.claim_id, fc.date_of_service
ORDER BY period
"""
    )
    df = _query(sql)
    if df.empty:
        return 0.0, pd.Series(dtype=float)
    avg = df["payment_amount"].mean()
    trend = df.groupby("period")["payment_amount"].mean()
    trend.index.name = "year_month"
    return round(float(avg), 2), trend


# ===========================================================================
# 15. PAYER MIX ANALYSIS
# ===========================================================================


def query_payer_mix(p: FilterParams):
    cte = _cte(p)
    sql = (
        cte
        + """
SELECT fc.payer_id,
       py.payer_name,
       py.payer_type,
       COUNT(DISTINCT fc.claim_id) AS claim_count,
       SUM(fc.total_charge_amount) AS total_charges,
       COALESCE(SUM(p.payment_amount), 0) AS total_payments
FROM filtered_claims fc
JOIN RCM_ANALYTICS.SILVER.PAYERS py ON fc.payer_id = py.payer_id
LEFT JOIN RCM_ANALYTICS.SILVER.PAYMENTS p ON fc.claim_id = p.claim_id
GROUP BY fc.payer_id, py.payer_name, py.payer_type
ORDER BY total_payments DESC
"""
    )
    df = _query(sql)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "payer_id",
                "payer_name",
                "payer_type",
                "claim_count",
                "total_charges",
                "total_payments",
                "collection_rate",
            ]
        )
    df["collection_rate"] = np.where(df["total_charges"] > 0, df["total_payments"] / df["total_charges"] * 100, 0)
    return df


# ===========================================================================
# 16. DENIAL RATE BY PAYER
# ===========================================================================


def query_denial_rate_by_payer(p: FilterParams):
    cte = _cte(p)
    sql = (
        cte
        + """
SELECT fc.payer_id,
       py.payer_name,
       COUNT(*) AS total_claims,
       SUM(CASE WHEN fc.claim_status IN ('Denied','Appealed') THEN 1 ELSE 0 END) AS denied
FROM filtered_claims fc
JOIN RCM_ANALYTICS.SILVER.PAYERS py ON fc.payer_id = py.payer_id
GROUP BY fc.payer_id, py.payer_name
ORDER BY denied * 1.0 / NULLIF(COUNT(*), 0) DESC
"""
    )
    df = _query(sql)
    if df.empty:
        return pd.DataFrame(columns=["payer_id", "payer_name", "total_claims", "denied", "denial_rate"])
    df["denial_rate"] = np.where(df["total_claims"] > 0, df["denied"] / df["total_claims"] * 100, 0)
    return df


# ===========================================================================
# 17. DEPARTMENT PERFORMANCE
# ===========================================================================


def query_department_performance(p: FilterParams):
    cte = _cte(p)
    sql = (
        cte
        + """
SELECT e.department,
       COUNT(DISTINCT e.encounter_id) AS encounter_count,
       COALESCE(SUM(fc.total_charge_amount), 0) AS total_charges,
       COALESCE(SUM(p.payment_amount), 0) AS total_payments
FROM filtered_claims fc
JOIN RCM_ANALYTICS.SILVER.ENCOUNTERS e ON fc.encounter_id = e.encounter_id
LEFT JOIN RCM_ANALYTICS.SILVER.PAYMENTS p ON fc.claim_id = p.claim_id
GROUP BY e.department
ORDER BY total_payments DESC
"""
    )
    df = _query(sql)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "department",
                "encounter_count",
                "total_charges",
                "total_payments",
                "collection_rate",
                "avg_payment_per_encounter",
            ]
        )
    df["collection_rate"] = np.where(df["total_charges"] > 0, df["total_payments"] / df["total_charges"] * 100, 0)
    df["avg_payment_per_encounter"] = np.where(
        df["encounter_count"] > 0, df["total_payments"] / df["encounter_count"], 0
    )
    return df


# ===========================================================================
# 18. PROVIDER PERFORMANCE
# ===========================================================================


def query_provider_performance(p: FilterParams):
    cte = _cte(p)
    sql = (
        cte
        + """
, provider_claims AS (
    SELECT pr.provider_id, pr.provider_name, pr.specialty, pr.department,
           COUNT(DISTINCT fc.encounter_id) AS encounter_count,
           COUNT(DISTINCT fc.claim_id) AS claim_count,
           SUM(fc.total_charge_amount) AS total_charges,
           SUM(fc.is_clean_claim) AS clean_claims,
           SUM(CASE WHEN fc.claim_status IN ('Denied','Appealed') THEN 1 ELSE 0 END) AS denied_claims
    FROM filtered_claims fc
    JOIN RCM_ANALYTICS.SILVER.ENCOUNTERS e ON fc.encounter_id = e.encounter_id
    JOIN RCM_ANALYTICS.SILVER.PROVIDERS pr ON e.provider_id = pr.provider_id
    GROUP BY pr.provider_id, pr.provider_name, pr.specialty, pr.department
), provider_payments AS (
    SELECT e.provider_id,
           COALESCE(SUM(p.payment_amount), 0) AS total_payments
    FROM filtered_claims fc
    JOIN RCM_ANALYTICS.SILVER.ENCOUNTERS e ON fc.encounter_id = e.encounter_id
    LEFT JOIN RCM_ANALYTICS.SILVER.PAYMENTS p ON fc.claim_id = p.claim_id
    GROUP BY e.provider_id
)
SELECT pc.provider_id, pc.provider_name, pc.specialty, pc.department,
       pc.encounter_count, pc.claim_count, pc.total_charges,
       COALESCE(pp.total_payments, 0) AS total_payments,
       pc.clean_claims, pc.denied_claims
FROM provider_claims pc
LEFT JOIN provider_payments pp ON pc.provider_id = pp.provider_id
ORDER BY total_payments DESC
"""
    )
    df = _query(sql)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "provider_id",
                "provider_name",
                "specialty",
                "department",
                "encounter_count",
                "claim_count",
                "total_charges",
                "total_payments",
                "collection_rate",
                "denial_rate",
                "clean_claim_rate",
                "avg_payment_per_encounter",
            ]
        )
    df["collection_rate"] = np.where(df["total_charges"] > 0, df["total_payments"] / df["total_charges"] * 100, 0)
    df["denial_rate"] = np.where(df["claim_count"] > 0, df["denied_claims"] / df["claim_count"] * 100, 0)
    df["clean_claim_rate"] = np.where(df["claim_count"] > 0, df["clean_claims"] / df["claim_count"] * 100, 0)
    df["avg_payment_per_encounter"] = np.where(
        df["encounter_count"] > 0, df["total_payments"] / df["encounter_count"], 0
    )
    return df


# ===========================================================================
# 19. CPT CODE ANALYSIS
# ===========================================================================


def query_cpt_analysis(p: FilterParams):
    cte = _cte(p)
    sql = (
        cte
        + """
, encounter_ids AS (
    SELECT DISTINCT encounter_id FROM filtered_claims
), charge_stats AS (
    SELECT ch.cpt_code, ch.cpt_description,
           COUNT(ch.charge_id) AS charge_count,
           SUM(ch.units) AS total_units,
           SUM(ch.charge_amount) AS total_charges
    FROM RCM_ANALYTICS.SILVER.CHARGES ch
    WHERE ch.encounter_id IN (SELECT encounter_id FROM encounter_ids)
    GROUP BY ch.cpt_code, ch.cpt_description
), cpt_claim_pairs AS (
    SELECT DISTINCT ch.cpt_code, fc.claim_id, fc.claim_status
    FROM RCM_ANALYTICS.SILVER.CHARGES ch
    JOIN filtered_claims fc ON ch.encounter_id = fc.encounter_id
), claim_stats AS (
    SELECT cpt_code,
           COUNT(DISTINCT claim_id) AS claim_count,
           SUM(CASE WHEN claim_status IN ('Denied','Appealed') THEN 1 ELSE 0 END) AS denied_claims
    FROM cpt_claim_pairs
    GROUP BY cpt_code
)
SELECT cs.cpt_code, cs.cpt_description, cs.charge_count, cs.total_units, cs.total_charges,
       COALESCE(cls.claim_count, 0) AS claim_count,
       COALESCE(cls.denied_claims, 0) AS denied_claims
FROM charge_stats cs
LEFT JOIN claim_stats cls ON cs.cpt_code = cls.cpt_code
ORDER BY cs.total_charges DESC
"""
    )
    df = _query(sql)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "cpt_code",
                "cpt_description",
                "charge_count",
                "total_units",
                "total_charges",
                "claim_count",
                "denied_claims",
                "avg_charge_per_unit",
                "denial_rate",
            ]
        )
    df["avg_charge_per_unit"] = np.where(df["total_units"] > 0, df["total_charges"] / df["total_units"], 0)
    df["denial_rate"] = np.where(df["claim_count"] > 0, df["denied_claims"] / df["claim_count"] * 100, 0)
    return df


# ===========================================================================
# 20. UNDERPAYMENT ANALYSIS
# ===========================================================================


def query_underpayment_analysis(p: FilterParams):
    cte = _cte(p)
    sql = (
        cte
        + """
SELECT fc.payer_id, py.payer_name, py.payer_type,
       COUNT(p.payment_id) AS payment_count,
       SUM(p.allowed_amount) AS total_allowed,
       SUM(p.payment_amount) AS total_paid,
       SUM(CASE WHEN p.allowed_amount > p.payment_amount
                THEN p.allowed_amount - p.payment_amount ELSE 0 END) AS total_underpaid,
       COUNT(CASE WHEN p.allowed_amount > p.payment_amount THEN 1 END) AS underpaid_count
FROM filtered_claims fc
JOIN RCM_ANALYTICS.SILVER.PAYERS py ON fc.payer_id = py.payer_id
JOIN RCM_ANALYTICS.SILVER.PAYMENTS p ON fc.claim_id = p.claim_id
WHERE p.allowed_amount IS NOT NULL AND p.allowed_amount > 0
GROUP BY fc.payer_id, py.payer_name, py.payer_type
ORDER BY total_underpaid DESC
"""
    )
    df = _query(sql)
    empty_cols = [
        "payer_id",
        "payer_name",
        "payer_type",
        "payment_count",
        "total_allowed",
        "total_paid",
        "total_underpaid",
        "underpaid_count",
        "underpayment_rate",
    ]
    if df.empty:
        return pd.DataFrame(columns=empty_cols), 0.0
    df["underpayment_rate"] = np.where(df["total_allowed"] > 0, df["total_underpaid"] / df["total_allowed"] * 100, 0)
    return df, float(df["total_underpaid"].sum())


# ===========================================================================
# 21. UNDERPAYMENT TREND
# ===========================================================================


def query_underpayment_trend(p: FilterParams):
    cte = _cte(p)
    sql = (
        cte
        + """
SELECT TO_CHAR(TRY_TO_DATE(p.payment_date, 'YYYY-MM-DD'), 'YYYY-MM') AS period,
       SUM(p.allowed_amount) AS total_allowed,
       SUM(p.payment_amount) AS total_paid,
       SUM(CASE WHEN p.allowed_amount > p.payment_amount
                THEN p.allowed_amount - p.payment_amount ELSE 0 END) AS total_underpaid
FROM filtered_claims fc
JOIN RCM_ANALYTICS.SILVER.PAYMENTS p ON fc.claim_id = p.claim_id
WHERE p.allowed_amount IS NOT NULL AND p.allowed_amount > 0
GROUP BY TO_CHAR(TRY_TO_DATE(p.payment_date, 'YYYY-MM-DD'), 'YYYY-MM')
ORDER BY period
"""
    )
    df = _query(sql)
    if df.empty:
        return pd.DataFrame(columns=["total_allowed", "total_paid", "total_underpaid", "underpayment_rate"])
    df["underpayment_rate"] = np.where(df["total_allowed"] > 0, df["total_underpaid"] / df["total_allowed"] * 100, 0)
    df = df.set_index("period")
    df.index.name = "year_month"
    return df


# ===========================================================================
# 22. CLEAN CLAIM SCRUBBING BREAKDOWN
# ===========================================================================

_FAIL_REASON_LABELS = {
    "MISSING_AUTH": "Missing Prior Authorization",
    "ELIGIBILITY_FAIL": "Patient Eligibility Not Verified",
    "CODING_ERROR": "Invalid CPT/ICD-10 Combination",
    "DUPLICATE_SUBMISSION": "Duplicate Claim Submission",
    "TIMELY_FILING": "Outside Timely Filing Window",
    "MISSING_INFO": "Missing Required Information",
}
_FAIL_REASON_GUIDANCE = {
    "MISSING_AUTH": "Automate auth check at scheduling; obtain PA before service date.",
    "ELIGIBILITY_FAIL": "Verify eligibility 24-48h before appointment via real-time check.",
    "CODING_ERROR": "Add CPT/ICD-10 edit rules to charge capture; schedule coder training.",
    "DUPLICATE_SUBMISSION": "Enable duplicate detection in clearinghouse scrubber settings.",
    "TIMELY_FILING": "Set automated alerts when claims approach payer filing deadlines.",
    "MISSING_INFO": "Implement front-desk registration checklists with required-field validation.",
}


def query_clean_claim_breakdown(p: FilterParams):
    cte = _cte(p)
    sql = (
        cte
        + """
SELECT fail_reason,
       COUNT(*) AS count,
       SUM(total_charge_amount) AS total_charges
FROM filtered_claims
WHERE is_clean_claim = 0
  AND fail_reason IS NOT NULL
GROUP BY fail_reason
ORDER BY count DESC
"""
    )
    df = _query(sql)
    if df.empty:
        return pd.DataFrame(columns=["fail_reason", "label", "count", "total_charges", "pct_of_dirty", "guidance"])
    total_dirty = df["count"].sum()
    df["pct_of_dirty"] = np.where(total_dirty > 0, df["count"] / total_dirty * 100, 0)
    df["label"] = df["fail_reason"].map(_FAIL_REASON_LABELS).fillna(df["fail_reason"])
    df["guidance"] = df["fail_reason"].map(_FAIL_REASON_GUIDANCE).fillna("")
    return df


# ===========================================================================
# 23. PATIENT FINANCIAL RESPONSIBILITY — BY PAYER
# ===========================================================================


def query_patient_responsibility_by_payer(p: FilterParams):
    cte = _cte(p)
    sql = (
        cte
        + """
SELECT py.payer_name, py.payer_type,
       COUNT(p.payment_id) AS payment_count,
       SUM(CASE WHEN p.allowed_amount > p.payment_amount
                THEN p.allowed_amount - p.payment_amount ELSE 0 END) AS total_patient_resp,
       AVG(CASE WHEN p.allowed_amount > p.payment_amount
                THEN p.allowed_amount - p.payment_amount ELSE 0 END) AS avg_patient_resp,
       SUM(p.allowed_amount) AS total_allowed
FROM filtered_claims fc
JOIN RCM_ANALYTICS.SILVER.PAYERS py ON fc.payer_id = py.payer_id
JOIN RCM_ANALYTICS.SILVER.PAYMENTS p ON fc.claim_id = p.claim_id
WHERE p.allowed_amount IS NOT NULL AND p.allowed_amount > 0
GROUP BY py.payer_name, py.payer_type
ORDER BY total_patient_resp DESC
"""
    )
    df = _query(sql)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "payer_name",
                "payer_type",
                "payment_count",
                "total_patient_resp",
                "avg_patient_resp",
                "pct_of_allowed",
            ]
        )
    df["pct_of_allowed"] = np.where(df["total_allowed"] > 0, df["total_patient_resp"] / df["total_allowed"] * 100, 0)
    return df


# ===========================================================================
# 24. PATIENT FINANCIAL RESPONSIBILITY — BY DEPARTMENT
# ===========================================================================


def query_patient_responsibility_by_dept(p: FilterParams):
    cte = _cte(p)
    sql = (
        cte
        + """
SELECT e.department, e.encounter_type,
       COUNT(DISTINCT fc.claim_id) AS claim_count,
       SUM(CASE WHEN p.allowed_amount > p.payment_amount
                THEN p.allowed_amount - p.payment_amount ELSE 0 END) AS total_patient_resp,
       AVG(CASE WHEN p.allowed_amount > p.payment_amount
                THEN p.allowed_amount - p.payment_amount ELSE 0 END) AS avg_patient_resp
FROM filtered_claims fc
JOIN RCM_ANALYTICS.SILVER.ENCOUNTERS e ON fc.encounter_id = e.encounter_id
JOIN RCM_ANALYTICS.SILVER.PAYMENTS p ON fc.claim_id = p.claim_id
WHERE p.allowed_amount IS NOT NULL AND p.allowed_amount > 0
GROUP BY e.department, e.encounter_type
ORDER BY total_patient_resp DESC
"""
    )
    df = _query(sql)
    if df.empty:
        return pd.DataFrame(
            columns=["department", "encounter_type", "claim_count", "total_patient_resp", "avg_patient_resp"]
        )
    return df


# ===========================================================================
# 25. PATIENT FINANCIAL RESPONSIBILITY — TREND
# ===========================================================================


def query_patient_responsibility_trend(p: FilterParams):
    cte = _cte(p)
    sql = (
        cte
        + """
SELECT TO_CHAR(TRY_TO_DATE(fc.date_of_service, 'YYYY-MM-DD'), 'YYYY-MM') AS period,
       SUM(CASE WHEN p.allowed_amount > p.payment_amount
                THEN p.allowed_amount - p.payment_amount ELSE 0 END) AS total_patient_resp,
       SUM(p.allowed_amount) AS total_allowed,
       COUNT(DISTINCT fc.claim_id) AS claim_count
FROM filtered_claims fc
JOIN RCM_ANALYTICS.SILVER.PAYMENTS p ON fc.claim_id = p.claim_id
WHERE p.allowed_amount IS NOT NULL AND p.allowed_amount > 0
GROUP BY TO_CHAR(TRY_TO_DATE(fc.date_of_service, 'YYYY-MM-DD'), 'YYYY-MM')
ORDER BY period
"""
    )
    df = _query(sql)
    if df.empty:
        return pd.DataFrame(columns=["total_patient_resp", "total_allowed", "patient_resp_rate"])
    df["patient_resp_rate"] = np.where(df["total_allowed"] > 0, df["total_patient_resp"] / df["total_allowed"] * 100, 0)
    df = df.set_index("period")
    df.index.name = "year_month"
    return df


# ===========================================================================
# 26. DATA FRESHNESS
# ===========================================================================

_DOMAIN_CADENCE_HOURS = {
    "claims": 4,
    "payments": 6,
    "encounters": 4,
    "charges": 4,
    "denials": 12,
    "adjustments": 8,
    "payers": 24,
    "patients": 24,
    "providers": 24,
    "operating_costs": 720,
}

_DOMAIN_LABELS = {
    "claims": "Claims",
    "payments": "Payments / ERA",
    "encounters": "Encounters / ADT",
    "charges": "Charges / CDM",
    "denials": "Denials",
    "adjustments": "Adjustments",
    "payers": "Payer Master",
    "patients": "Patient Demographics",
    "providers": "Provider Roster",
    "operating_costs": "Operating Costs",
}


def query_data_freshness():
    sql = "SELECT domain, last_loaded_at, row_count, source_file FROM RCM_ANALYTICS.METADATA.PIPELINE_RUNS ORDER BY domain"
    df = _query(sql)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "domain",
                "label",
                "last_loaded_at",
                "row_count",
                "source_file",
                "cadence_hours",
                "age_hours",
                "status",
            ]
        )
    now = pd.Timestamp.utcnow().replace(tzinfo=None)
    df["label"] = df["domain"].map(_DOMAIN_LABELS).fillna(df["domain"])
    df["cadence_hours"] = df["domain"].map(_DOMAIN_CADENCE_HOURS).fillna(24)
    df["last_loaded_at_dt"] = pd.to_datetime(df["last_loaded_at"], errors="coerce", utc=True).dt.tz_localize(None)
    df["age_hours"] = (now - df["last_loaded_at_dt"]).dt.total_seconds() / 3600
    df["status"] = "fresh"
    df.loc[df["age_hours"] > df["cadence_hours"], "status"] = "stale"
    df.loc[df["age_hours"] > df["cadence_hours"] * 3, "status"] = "critical"
    return df[["domain", "label", "last_loaded_at", "row_count", "source_file", "cadence_hours", "age_hours", "status"]]
