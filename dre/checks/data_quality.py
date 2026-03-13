"""
Data Quality Checks — row count, freshness, nulls, duplicates.
Runs directly against Snowflake.
"""
import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)


def run_row_count_check(sf_conn, table: str, deviation_threshold_pct: float = 20.0) -> dict:
    """
    Compare today's row count vs yesterday and 7-day average.
    Flags if deviation > threshold.
    """
    cur = sf_conn.cursor()
    issues = []

    try:
        # Today's count
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        today_count = cur.fetchone()[0]

        # Yesterday's count (if table has DATE column)
        try:
            cur.execute(f"""
                SELECT COUNT(*) FROM {table}
                WHERE DATE = CURRENT_DATE - 1
            """)
            yesterday_count = cur.fetchone()[0]
        except Exception:
            yesterday_count = None

        # 7-day average
        try:
            cur.execute(f"""
                SELECT AVG(daily_count) FROM (
                    SELECT DATE, COUNT(*) AS daily_count
                    FROM {table}
                    WHERE DATE >= CURRENT_DATE - 8 AND DATE < CURRENT_DATE
                    GROUP BY DATE
                )
            """)
            avg_7d = cur.fetchone()[0]
            avg_7d = round(float(avg_7d), 0) if avg_7d else None
        except Exception:
            avg_7d = None

        # Deviation checks
        vs_yesterday = None
        vs_7d_avg = None

        if yesterday_count and yesterday_count > 0:
            vs_yesterday = round(abs(today_count - yesterday_count) / yesterday_count * 100, 2)
            if vs_yesterday > deviation_threshold_pct:
                issues.append({
                    "check": "row_count_vs_yesterday",
                    "severity": "HIGH" if vs_yesterday > 50 else "MEDIUM",
                    "message": f"{table}: Today {today_count:,} rows vs yesterday {yesterday_count:,} ({vs_yesterday}% deviation)",
                })

        if avg_7d and avg_7d > 0:
            vs_7d_avg = round(abs(today_count - avg_7d) / avg_7d * 100, 2)
            if vs_7d_avg > deviation_threshold_pct:
                issues.append({
                    "check": "row_count_vs_7d_avg",
                    "severity": "HIGH" if vs_7d_avg > 50 else "MEDIUM",
                    "message": f"{table}: Today {today_count:,} rows vs 7d avg {avg_7d:,.0f} ({vs_7d_avg}% deviation)",
                })

        return {
            "table": table,
            "today_count": today_count,
            "yesterday_count": yesterday_count,
            "avg_7d": avg_7d,
            "deviation_vs_yesterday_pct": vs_yesterday,
            "deviation_vs_7d_avg_pct": vs_7d_avg,
            "issues": issues,
            "passed": len(issues) == 0,
        }

    except Exception as e:
        logger.error(f"Row count check failed for {table}: {e}")
        return {"table": table, "error": str(e), "passed": False, "issues": []}
    finally:
        cur.close()


def run_freshness_check(sf_conn, table: str, key_column: str, max_delay_minutes: int) -> dict:
    """
    Check that the latest data timestamp is within SLA.
    """
    cur = sf_conn.cursor()
    try:
        cur.execute(f"SELECT MAX({key_column}) FROM {table}")
        latest = cur.fetchone()[0]

        if latest is None:
            return {
                "table": table,
                "key_column": key_column,
                "latest_timestamp": None,
                "delay_minutes": None,
                "sla_minutes": max_delay_minutes,
                "passed": False,
                "issues": [{
                    "check": "freshness",
                    "severity": "CRITICAL",
                    "message": f"{table}: No data found — table may be empty or pipeline never ran",
                }],
            }

        # If it's a date (not timestamp), convert to datetime midnight UTC
        if hasattr(latest, 'date') and not hasattr(latest, 'hour'):
            latest_dt = datetime(latest.year, latest.month, latest.day, tzinfo=timezone.utc)
        else:
            latest_dt = latest if latest.tzinfo else latest.replace(tzinfo=timezone.utc)

        now = datetime.now(timezone.utc)
        delay_min = round((now - latest_dt).total_seconds() / 60, 1)
        breached = delay_min > max_delay_minutes

        issues = []
        if breached:
            severity = "CRITICAL" if delay_min > max_delay_minutes * 3 else "HIGH"
            issues.append({
                "check": "freshness",
                "severity": severity,
                "message": (
                    f"{table}: Data is {delay_min:.0f} min old "
                    f"(SLA: {max_delay_minutes} min). "
                    f"Latest record: {latest}"
                ),
            })

        return {
            "table": table,
            "key_column": key_column,
            "latest_timestamp": str(latest),
            "delay_minutes": delay_min,
            "sla_minutes": max_delay_minutes,
            "passed": not breached,
            "issues": issues,
        }

    except Exception as e:
        logger.error(f"Freshness check failed for {table}: {e}")
        return {"table": table, "error": str(e), "passed": False, "issues": []}
    finally:
        cur.close()


def run_null_check(sf_conn, table: str, not_null_columns: list) -> dict:
    """
    Detect unexpected nulls in key columns.
    """
    cur = sf_conn.cursor()
    issues = []
    null_counts = {}

    try:
        for col in not_null_columns:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")
                null_count = cur.fetchone()[0]
                null_counts[col] = null_count
                if null_count > 0:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    total = cur.fetchone()[0]
                    pct = round(null_count / total * 100, 2) if total > 0 else 0
                    severity = "CRITICAL" if pct > 10 else "HIGH" if pct > 1 else "MEDIUM"
                    issues.append({
                        "check": "null_validation",
                        "severity": severity,
                        "column": col,
                        "message": f"{table}.{col}: {null_count:,} nulls ({pct}% of rows)",
                    })
            except Exception as col_err:
                logger.warning(f"Null check failed for {table}.{col}: {col_err}")

        return {
            "table": table,
            "null_counts": null_counts,
            "passed": len(issues) == 0,
            "issues": issues,
        }

    except Exception as e:
        logger.error(f"Null check failed for {table}: {e}")
        return {"table": table, "error": str(e), "passed": False, "issues": []}
    finally:
        cur.close()


def run_duplicate_check(sf_conn, table: str, primary_key: str) -> dict:
    """
    Detect duplicate primary keys.
    """
    cur = sf_conn.cursor()
    try:
        cur.execute(f"""
            SELECT COUNT(*) AS dup_count FROM (
                SELECT {primary_key}, COUNT(*) AS cnt
                FROM {table}
                GROUP BY {primary_key}
                HAVING cnt > 1
            )
        """)
        dup_count = cur.fetchone()[0]
        issues = []

        if dup_count > 0:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            total = cur.fetchone()[0]
            pct = round(dup_count / total * 100, 2) if total > 0 else 0
            issues.append({
                "check": "duplicate_detection",
                "severity": "HIGH",
                "message": f"{table}: {dup_count:,} duplicate {primary_key} values ({pct}% of rows)",
            })

        return {
            "table": table,
            "primary_key": primary_key,
            "duplicate_count": dup_count,
            "passed": dup_count == 0,
            "issues": issues,
        }

    except Exception as e:
        logger.error(f"Duplicate check failed for {table}: {e}")
        return {"table": table, "error": str(e), "passed": False, "issues": []}
    finally:
        cur.close()


def run_categorical_check(sf_conn, table: str, categorical_columns: dict) -> dict:
    """
    Detect unexpected categorical values outside the allowed set.
    """
    cur = sf_conn.cursor()
    issues = []

    try:
        for col, allowed_values in categorical_columns.items():
            allowed_str = ", ".join(f"'{v}'" for v in allowed_values)
            cur.execute(f"""
                SELECT {col}, COUNT(*) AS cnt
                FROM {table}
                WHERE {col} NOT IN ({allowed_str})
                GROUP BY {col}
                LIMIT 10
            """)
            bad_vals = cur.fetchall()
            if bad_vals:
                summary = ", ".join(f"'{r[0]}' ({r[1]:,})" for r in bad_vals)
                issues.append({
                    "check": "categorical_validation",
                    "severity": "MEDIUM",
                    "column": col,
                    "message": f"{table}.{col}: Unexpected values found — {summary}",
                })

        return {
            "table": table,
            "passed": len(issues) == 0,
            "issues": issues,
        }

    except Exception as e:
        logger.error(f"Categorical check failed for {table}: {e}")
        return {"table": table, "error": str(e), "passed": False, "issues": []}
    finally:
        cur.close()


def run_all_quality_checks(sf_conn, table: str, table_config: dict, pipeline_config: dict) -> dict:
    """
    Run all data quality checks for a table.
    Returns aggregated results and all issues.
    """
    all_issues = []
    results = {}

    # Row count
    rc = run_row_count_check(
        sf_conn, table,
        pipeline_config.get("row_count", {}).get("deviation_threshold_pct", 20),
    )
    results["row_count"] = rc
    all_issues.extend(rc.get("issues", []))

    # Freshness
    freshness_cfg = pipeline_config.get("freshness", {})
    if freshness_cfg:
        fr = run_freshness_check(
            sf_conn, table,
            freshness_cfg.get("key_column", "DATE"),
            freshness_cfg.get("max_delay_minutes", 60),
        )
        results["freshness"] = fr
        all_issues.extend(fr.get("issues", []))

    # Nulls
    not_null_cols = table_config.get("not_null_columns", [])
    if not_null_cols:
        nv = run_null_check(sf_conn, table, not_null_cols)
        results["nulls"] = nv
        all_issues.extend(nv.get("issues", []))

    # Duplicates
    pk = table_config.get("primary_key")
    if pk:
        dd = run_duplicate_check(sf_conn, table, pk)
        results["duplicates"] = dd
        all_issues.extend(dd.get("issues", []))

    # Categoricals
    cat_cols = table_config.get("categorical_columns", {})
    if cat_cols:
        cc = run_categorical_check(sf_conn, table, cat_cols)
        results["categoricals"] = cc
        all_issues.extend(cc.get("issues", []))

    return {
        "table": table,
        "checks": results,
        "all_issues": all_issues,
        "total_issues": len(all_issues),
        "passed": len(all_issues) == 0,
    }
