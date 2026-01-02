#!/usr/bin/env python3
# file: src/meta/schema_snapshot.py

import argparse
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pyodbc


DEFAULT_TABLES = [
    # raw
    "raw.canvas_users",
    "raw.canvas_courses",
    "raw.canvas_enrollments",
    "raw.canvas_submissions",
    # curated
    "cur.person_identity_map",
    "cur.dim_student",
    "cur.dim_course",
    "cur.fact_submission",
    # meta / dq
    "meta.job_run",
    "meta.dq_check_result",
    "cur.person_identity_map_dq",  # 你现在的 DQ 表在 cur 还是 meta 取决于你之前实现；不存在会自动跳过
]


DDL = r"""
IF SCHEMA_ID('meta') IS NULL EXEC('CREATE SCHEMA meta');

IF OBJECT_ID('meta.schema_snapshot','U') IS NULL
BEGIN
  CREATE TABLE meta.schema_snapshot (
    snapshot_time DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    table_name NVARCHAR(300) NOT NULL,
    schema_json NVARCHAR(MAX) NOT NULL,
    PRIMARY KEY (snapshot_time, table_name)
  );
END;

IF OBJECT_ID('meta.schema_change_log','U') IS NULL
BEGIN
  CREATE TABLE meta.schema_change_log (
    change_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    detected_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    table_name NVARCHAR(300) NOT NULL,
    change_type NVARCHAR(50) NOT NULL,
    change_detail_json NVARCHAR(MAX) NOT NULL,
    job_run_id UNIQUEIDENTIFIER NULL
  );
END;
"""


def utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str)


def split_table(full: str) -> Tuple[str, str]:
    if "." not in full:
        raise ValueError(f"table_name must be schema.table, got: {full}")
    schema, name = full.split(".", 1)
    return schema, name


def table_exists(cur: pyodbc.Cursor, full_name: str) -> bool:
    return cur.execute("SELECT OBJECT_ID(?, 'U')", full_name).fetchone()[0] is not None


def get_latest_snapshot(cur: pyodbc.Cursor, table_name: str) -> Optional[Dict[str, Any]]:
    row = cur.execute(
        """
        SELECT TOP (1) schema_json
        FROM meta.schema_snapshot
        WHERE table_name = ?
        ORDER BY snapshot_time DESC;
        """,
        table_name,
    ).fetchone()
    if not row or not row[0]:
        return None
    try:
        return json.loads(row[0])
    except Exception:
        return None


def read_table_schema(cur: pyodbc.Cursor, full_name: str) -> Dict[str, Any]:
    schema, name = split_table(full_name)
    rows = cur.execute(
        """
        SELECT
          COLUMN_NAME,
          ORDINAL_POSITION,
          DATA_TYPE,
          CHARACTER_MAXIMUM_LENGTH,
          NUMERIC_PRECISION,
          NUMERIC_SCALE,
          IS_NULLABLE,
          COLUMN_DEFAULT,
          COLLATION_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION ASC;
        """,
        schema,
        name,
    ).fetchall()

    cols: List[Dict[str, Any]] = []
    for (
        col_name,
        ordinal,
        data_type,
        char_len,
        num_prec,
        num_scale,
        is_nullable,
        col_default,
        collation_name,
    ) in rows:
        cols.append(
            {
                "name": str(col_name),
                "ordinal": int(ordinal),
                "data_type": str(data_type).lower() if data_type is not None else None,
                "char_max_length": int(char_len) if char_len is not None else None,
                "numeric_precision": int(num_prec) if num_prec is not None else None,
                "numeric_scale": int(num_scale) if num_scale is not None else None,
                "is_nullable": (str(is_nullable).upper() == "YES") if is_nullable is not None else None,
                "default": str(col_default) if col_default is not None else None,
                "collation": str(collation_name) if collation_name is not None else None,
            }
        )

    return {"table_name": full_name, "columns": cols}


def index_columns(schema_json: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    m: Dict[str, Dict[str, Any]] = {}
    for c in schema_json.get("columns", []):
        nm = c.get("name")
        if nm:
            m[str(nm)] = c
    return m


def col_signature(c: Dict[str, Any]) -> Dict[str, Any]:
    """
    Only compare fields that matter for compatibility.
    """
    return {
        "data_type": c.get("data_type"),
        "char_max_length": c.get("char_max_length"),
        "numeric_precision": c.get("numeric_precision"),
        "numeric_scale": c.get("numeric_scale"),
        "is_nullable": c.get("is_nullable"),
        "default": c.get("default"),
        "collation": c.get("collation"),
        "ordinal": c.get("ordinal"),
    }


def diff_schema(prev: Dict[str, Any], curr: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Return list of (change_type, detail_dict)
    """
    changes: List[Tuple[str, Dict[str, Any]]] = []

    prev_cols = index_columns(prev)
    curr_cols = index_columns(curr)

    prev_set = set(prev_cols.keys())
    curr_set = set(curr_cols.keys())

    added = sorted(curr_set - prev_set)
    removed = sorted(prev_set - curr_set)
    common = sorted(prev_set & curr_set)

    for name in added:
        changes.append(("column_added", {"column": name, "after": curr_cols[name]}))

    for name in removed:
        changes.append(("column_removed", {"column": name, "before": prev_cols[name]}))

    for name in common:
        b = prev_cols[name]
        a = curr_cols[name]
        if col_signature(b) != col_signature(a):
            changes.append(
                (
                    "column_changed",
                    {
                        "column": name,
                        "before": b,
                        "after": a,
                        "before_sig": col_signature(b),
                        "after_sig": col_signature(a),
                    },
                )
            )

    return changes


def insert_change(
    cur: pyodbc.Cursor,
    table_name: str,
    change_type: str,
    detail: Dict[str, Any],
    job_run_id: Optional[str] = None,  # UUID string or None
) -> None:
    cur.execute(
        """
        INSERT INTO meta.schema_change_log (table_name, change_type, change_detail_json, job_run_id)
        VALUES (?, ?, ?, ?);
        """,
        table_name,
        change_type,
        dumps(detail),
        job_run_id,
    )


def insert_snapshot(cur: pyodbc.Cursor, snapshot_time: datetime, table_name: str, schema_json: Dict[str, Any]) -> None:
    cur.execute(
        """
        INSERT INTO meta.schema_snapshot (snapshot_time, table_name, schema_json)
        VALUES (?, ?, ?);
        """,
        snapshot_time,
        table_name,
        dumps(schema_json),
    )


def main():
    ap = argparse.ArgumentParser(description="Capture schema snapshots and log schema changes.")
    ap.add_argument("--conn", required=True, help="ODBC connection string")
    ap.add_argument(
        "--tables",
        default=",".join(DEFAULT_TABLES),
        help="Comma-separated list of schema.table to check",
    )
    ap.add_argument(
        "--job-run-id",
        default=None,
        help="Optional UNIQUEIDENTIFIER (UUID) to attach to schema_change_log rows",
    )
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    tables = [t.strip() for t in args.tables.split(",") if t.strip()]
    snapshot_time = utcnow_naive()

    conn = pyodbc.connect(args.conn, autocommit=False)
    cur = conn.cursor()

    # Ensure meta tables
    cur.execute(DDL)
    conn.commit()

    changed = 0
    snapped = 0
    skipped_missing = 0

    for t in tables:
        try:
            prev = get_latest_snapshot(cur, t)

            if not table_exists(cur, t):
                skipped_missing += 1
                # 如果以前有快照，现在表没了 => 记录一次 table_missing
                if prev is not None:
                    insert_change(
                        cur,
                        table_name=t,
                        change_type="table_missing",
                        detail={"table_name": t, "note": "table exists in previous snapshot but missing now"},
                        job_run_id=args.job_run_id,
                    )
                    changed += 1
                logging.warning("SKIP missing table: %s", t)
                continue

            curr = read_table_schema(cur, t)

            # diff
            if prev is None:
                # 第一次见到这张表：记 table_added（可选，但很有用）
                insert_change(
                    cur,
                    table_name=t,
                    change_type="table_added",
                    detail={"table_name": t, "columns": [c["name"] for c in curr.get("columns", [])]},
                    job_run_id=args.job_run_id,
                )
                changed += 1
            else:
                changes = diff_schema(prev, curr)
                for change_type, detail in changes:
                    insert_change(cur, t, change_type, detail, job_run_id=args.job_run_id)
                changed += len(changes)

            # snapshot
            insert_snapshot(cur, snapshot_time, t, curr)
            snapped += 1

        except Exception as e:
            logging.exception("Failed processing table=%s: %s", t, e)
            # 不让单表失败卡住全局（更像生产）
            continue

    conn.commit()
    cur.close()
    conn.close()

    logging.info(
        "DONE snapshot_time=%s tables=%d snapped=%d changes_logged=%d missing_skipped=%d",
        snapshot_time,
        len(tables),
        snapped,
        changed,
        skipped_missing,
    )


if __name__ == "__main__":
    main()
