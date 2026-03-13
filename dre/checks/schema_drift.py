"""
Schema Drift Detection — compares current Snowflake schema against saved snapshot.
Detects: new columns, dropped columns, type changes.
"""
import json
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

SNAPSHOT_DIR = os.path.join(os.path.dirname(__file__), "..", "config", "schema_snapshots")


def get_current_schema(sf_conn, table: str) -> dict:
    """
    Fetch current column definitions from Snowflake INFORMATION_SCHEMA.
    Returns {column_name: {type, nullable, position}}.
    """
    cur = sf_conn.cursor()
    db, schema, tbl = _parse_table(table)

    try:
        cur.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION
            FROM {db}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}'
              AND TABLE_NAME   = '{tbl}'
            ORDER BY ORDINAL_POSITION
        """)
        rows = cur.fetchall()
        return {
            row[0]: {
                "type": row[1],
                "nullable": row[2],
                "position": row[3],
            }
            for row in rows
        }
    except Exception as e:
        logger.error(f"Failed to fetch schema for {table}: {e}")
        return {}
    finally:
        cur.close()


def load_snapshot(table: str) -> dict:
    """Load saved schema snapshot from disk."""
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    path = _snapshot_path(table)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f).get("columns", {})


def save_snapshot(table: str, schema: dict):
    """Save current schema as the new baseline snapshot."""
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    path = _snapshot_path(table)
    with open(path, "w") as f:
        json.dump({
            "table": table,
            "captured_at": datetime.utcnow().isoformat(),
            "columns": schema,
        }, f, indent=2)
    logger.info(f"Schema snapshot saved for {table}")


def detect_schema_drift(current: dict, previous: dict) -> dict:
    """
    Compare current vs previous schema.
    Returns drift summary with added, dropped, changed columns.
    """
    issues = []
    current_cols = set(current.keys())
    previous_cols = set(previous.keys())

    added = current_cols - previous_cols
    dropped = previous_cols - current_cols
    common = current_cols & previous_cols

    for col in added:
        issues.append({
            "check": "schema_drift",
            "severity": "MEDIUM",
            "change": "COLUMN_ADDED",
            "column": col,
            "message": f"New column added: {col} ({current[col]['type']})",
        })

    for col in dropped:
        issues.append({
            "check": "schema_drift",
            "severity": "HIGH",
            "change": "COLUMN_DROPPED",
            "column": col,
            "message": f"Column dropped: {col} (was {previous[col]['type']})",
        })

    for col in common:
        curr = current[col]
        prev = previous[col]
        if curr["type"] != prev["type"]:
            issues.append({
                "check": "schema_drift",
                "severity": "CRITICAL",
                "change": "TYPE_CHANGED",
                "column": col,
                "message": f"Type changed for {col}: {prev['type']} → {curr['type']}",
            })
        if curr["nullable"] != prev["nullable"]:
            issues.append({
                "check": "schema_drift",
                "severity": "MEDIUM",
                "change": "NULLABILITY_CHANGED",
                "column": col,
                "message": f"Nullability changed for {col}: {prev['nullable']} → {curr['nullable']}",
            })

    return {
        "added_columns": list(added),
        "dropped_columns": list(dropped),
        "changed_columns": [i["column"] for i in issues if i["change"] in ("TYPE_CHANGED", "NULLABILITY_CHANGED")],
        "issues": issues,
        "drift_detected": len(issues) > 0,
        "passed": len(issues) == 0,
    }


def run_schema_drift_check(sf_conn, table: str) -> dict:
    """
    Full schema drift check for a table.
    Fetches current schema, compares to snapshot, saves new snapshot.
    """
    current = get_current_schema(sf_conn, table)
    if not current:
        return {
            "table": table,
            "passed": False,
            "issues": [{
                "check": "schema_drift",
                "severity": "HIGH",
                "message": f"Could not fetch schema for {table}",
            }],
        }

    previous = load_snapshot(table)

    if previous is None:
        # First run — save baseline, no drift to report
        save_snapshot(table, current)
        logger.info(f"No schema snapshot found for {table}. Saved baseline with {len(current)} columns.")
        return {
            "table": table,
            "passed": True,
            "drift_detected": False,
            "issues": [],
            "message": f"Baseline snapshot created ({len(current)} columns)",
        }

    drift = detect_schema_drift(current, previous)

    # Update snapshot to current if drift was detected (so next run compares to latest)
    if drift["drift_detected"]:
        save_snapshot(table, current)

    return {
        "table": table,
        **drift,
    }


def _snapshot_path(table: str) -> str:
    safe = table.replace(".", "_").replace("/", "_")
    return os.path.join(SNAPSHOT_DIR, f"{safe}.json")


def _parse_table(table: str) -> tuple[str, str, str]:
    """Parse 'DB.SCHEMA.TABLE' → (db, schema, table)."""
    parts = table.split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    elif len(parts) == 2:
        return "ANALYTICS", parts[0], parts[1]
    return "ANALYTICS", "RETAIL", parts[0]
