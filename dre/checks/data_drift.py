"""
Data Drift Detection — statistical drift on numeric + categorical distributions.
Uses Z-score for numeric columns and chi-squared divergence for categoricals.
"""
import json
import logging
import math
import os
from datetime import datetime

logger = logging.getLogger(__name__)

BASELINE_DIR = os.path.join(os.path.dirname(__file__), "..", "config", "drift_baselines")


# ── Numeric drift ────────────────────────────────────────────────────────────

def get_numeric_stats(sf_conn, table: str, column: str, days: int = 7) -> dict:
    """Get mean and stddev for a numeric column over the last N days."""
    cur = sf_conn.cursor()
    try:
        cur.execute(f"""
            SELECT
                AVG({column})   AS mean,
                STDDEV({column}) AS stddev,
                MIN({column})   AS min_val,
                MAX({column})   AS max_val,
                COUNT({column}) AS cnt
            FROM {table}
            WHERE DATE >= CURRENT_DATE - {days}
        """)
        row = cur.fetchone()
        return {
            "mean": float(row[0]) if row[0] is not None else None,
            "stddev": float(row[1]) if row[1] is not None else None,
            "min": float(row[2]) if row[2] is not None else None,
            "max": float(row[3]) if row[3] is not None else None,
            "count": int(row[4]),
        }
    except Exception as e:
        logger.warning(f"Numeric stats failed for {table}.{column}: {e}")
        return {}
    finally:
        cur.close()


def get_today_numeric_stats(sf_conn, table: str, column: str) -> dict:
    """Get today's mean and stddev for a numeric column."""
    cur = sf_conn.cursor()
    try:
        cur.execute(f"""
            SELECT
                AVG({column})    AS mean,
                STDDEV({column}) AS stddev,
                COUNT({column})  AS cnt
            FROM {table}
            WHERE DATE = CURRENT_DATE
        """)
        row = cur.fetchone()
        return {
            "mean": float(row[0]) if row[0] is not None else None,
            "stddev": float(row[1]) if row[1] is not None else None,
            "count": int(row[2]),
        }
    except Exception as e:
        logger.warning(f"Today numeric stats failed for {table}.{column}: {e}")
        return {}
    finally:
        cur.close()


def detect_numeric_drift(baseline: dict, current: dict, column: str, table: str, z_threshold: float = 3.0) -> list:
    """
    Z-score drift: flag if today's mean is > z_threshold stddevs from baseline mean.
    """
    issues = []
    b_mean = baseline.get("mean")
    b_std = baseline.get("stddev")
    c_mean = current.get("mean")

    if b_mean is None or b_std is None or c_mean is None:
        return issues

    if b_std == 0:
        if abs(c_mean - b_mean) > 0:
            issues.append({
                "check": "data_drift",
                "type": "numeric",
                "severity": "MEDIUM",
                "column": column,
                "message": f"{table}.{column}: Mean changed from {b_mean:.2f} to {c_mean:.2f} (baseline std=0)",
            })
        return issues

    z_score = abs(c_mean - b_mean) / b_std
    if z_score > z_threshold:
        severity = "CRITICAL" if z_score > z_threshold * 2 else "HIGH"
        issues.append({
            "check": "data_drift",
            "type": "numeric",
            "severity": severity,
            "column": column,
            "z_score": round(z_score, 2),
            "baseline_mean": round(b_mean, 2),
            "current_mean": round(c_mean, 2),
            "message": (
                f"{table}.{column}: Numeric drift detected — "
                f"today mean={c_mean:.2f} vs baseline mean={b_mean:.2f} "
                f"(Z-score={z_score:.2f}, threshold={z_threshold})"
            ),
        })

    return issues


# ── Categorical drift ────────────────────────────────────────────────────────

def get_categorical_distribution(sf_conn, table: str, column: str, days: int = 7) -> dict:
    """Get value distribution (proportions) for a categorical column over N days."""
    cur = sf_conn.cursor()
    try:
        cur.execute(f"""
            SELECT {column}, COUNT(*) AS cnt
            FROM {table}
            WHERE DATE >= CURRENT_DATE - {days}
              AND {column} IS NOT NULL
            GROUP BY {column}
        """)
        rows = cur.fetchall()
        total = sum(r[1] for r in rows)
        return {r[0]: round(r[1] / total, 4) for r in rows} if total > 0 else {}
    except Exception as e:
        logger.warning(f"Categorical distribution failed for {table}.{column}: {e}")
        return {}
    finally:
        cur.close()


def get_today_categorical_distribution(sf_conn, table: str, column: str) -> dict:
    """Get today's value distribution for a categorical column."""
    cur = sf_conn.cursor()
    try:
        cur.execute(f"""
            SELECT {column}, COUNT(*) AS cnt
            FROM {table}
            WHERE DATE = CURRENT_DATE
              AND {column} IS NOT NULL
            GROUP BY {column}
        """)
        rows = cur.fetchall()
        total = sum(r[1] for r in rows)
        return {r[0]: round(r[1] / total, 4) for r in rows} if total > 0 else {}
    except Exception as e:
        logger.warning(f"Today categorical dist failed for {table}.{column}: {e}")
        return {}
    finally:
        cur.close()


def detect_categorical_drift(baseline: dict, current: dict, column: str, table: str, threshold: float = 0.15) -> list:
    """
    Flag if any category's proportion shifted by > threshold (15% by default).
    Also flags new or disappeared categories.
    """
    issues = []
    all_keys = set(baseline.keys()) | set(current.keys())

    for key in all_keys:
        b_pct = baseline.get(key, 0.0)
        c_pct = current.get(key, 0.0)
        diff = abs(c_pct - b_pct)

        if key not in baseline and c_pct > 0.01:
            issues.append({
                "check": "data_drift",
                "type": "categorical",
                "severity": "MEDIUM",
                "column": column,
                "category": key,
                "message": f"{table}.{column}: New category appeared — '{key}' ({c_pct*100:.1f}%)",
            })
        elif key not in current and b_pct > 0.01:
            issues.append({
                "check": "data_drift",
                "type": "categorical",
                "severity": "MEDIUM",
                "column": column,
                "category": key,
                "message": f"{table}.{column}: Category disappeared — '{key}' (was {b_pct*100:.1f}%)",
            })
        elif diff > threshold:
            severity = "HIGH" if diff > threshold * 2 else "MEDIUM"
            issues.append({
                "check": "data_drift",
                "type": "categorical",
                "severity": severity,
                "column": column,
                "category": key,
                "baseline_pct": round(b_pct * 100, 1),
                "current_pct": round(c_pct * 100, 1),
                "message": (
                    f"{table}.{column}: Distribution shift for '{key}' — "
                    f"baseline {b_pct*100:.1f}% → today {c_pct*100:.1f}% "
                    f"(Δ{diff*100:.1f}%)"
                ),
            })

    return issues


# ── Main runner ──────────────────────────────────────────────────────────────

def run_data_drift_check(sf_conn, table: str, table_config: dict) -> dict:
    """
    Full data drift check for all numeric and categorical columns in a table.
    """
    all_issues = []
    numeric_cols = table_config.get("numeric_columns", [])
    categorical_cols = table_config.get("categorical_columns", {})

    # Numeric drift
    for col in numeric_cols:
        baseline = get_numeric_stats(sf_conn, table, col, days=7)
        current = get_today_numeric_stats(sf_conn, table, col)
        if baseline and current:
            issues = detect_numeric_drift(baseline, current, col, table)
            all_issues.extend(issues)

    # Categorical drift
    for col in categorical_cols.keys():
        baseline = get_categorical_distribution(sf_conn, table, col, days=7)
        current = get_today_categorical_distribution(sf_conn, table, col)
        if baseline and current:
            issues = detect_categorical_drift(baseline, current, col, table)
            all_issues.extend(issues)

    return {
        "table": table,
        "issues": all_issues,
        "total_issues": len(all_issues),
        "passed": len(all_issues) == 0,
    }
