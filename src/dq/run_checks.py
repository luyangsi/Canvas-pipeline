# file: src/dq/run_checks.py
import argparse
import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Any, Tuple, Optional, List

import pyodbc


def utcnow():
    return datetime.now(timezone.utc)


def utcnow_sql_naive() -> datetime:
    """
    Return a naive UTC datetime suitable for SQL DATETIME2, without tzinfo.
    Avoids timezone / driver edge cases.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


def dumps_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str)


DDL = r"""
IF SCHEMA_ID('meta') IS NULL EXEC('CREATE SCHEMA meta');

IF OBJECT_ID('meta.job_run','U') IS NULL
BEGIN
  CREATE TABLE meta.job_run (
    job_run_id      BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    job_name        NVARCHAR(200) NOT NULL,
    start_time      DATETIME2 NOT NULL,
    end_time        DATETIME2 NULL,
    status          NVARCHAR(20) NOT NULL,   -- running/success/failed
    records_read    BIGINT NULL,
    records_written BIGINT NULL,
    error_message   NVARCHAR(MAX) NULL
  );
END

-- add duration + metrics_json (compatible ALTER)
IF COL_LENGTH('meta.job_run','duration_ms') IS NULL
  ALTER TABLE meta.job_run ADD duration_ms BIGINT NULL;

IF COL_LENGTH('meta.job_run','metrics_json') IS NULL
  ALTER TABLE meta.job_run ADD metrics_json NVARCHAR(MAX) NULL;

IF OBJECT_ID('meta.dq_check_result','U') IS NULL
BEGIN
  CREATE TABLE meta.dq_check_result (
    dq_result_id    BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    job_run_id      BIGINT NOT NULL,
    check_name      NVARCHAR(200) NOT NULL,
    severity        NVARCHAR(20) NOT NULL DEFAULT 'info', -- info/warn/error
    metric_value    FLOAT NULL,
    numerator       BIGINT NULL,
    denominator     BIGINT NULL,
    details_json    NVARCHAR(MAX) NULL,
    created_at      DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT FK_dq_jobrun FOREIGN KEY (job_run_id) REFERENCES meta.job_run(job_run_id)
  );
END

-- sample_keys column (JSON array, up to 10 ids/keys)
IF COL_LENGTH('meta.dq_check_result','sample_keys') IS NULL
  ALTER TABLE meta.dq_check_result ADD sample_keys NVARCHAR(MAX) NULL;
"""


def resolve_table(cur: pyodbc.Cursor, candidates) -> str:
    """
    candidates: list of ('schema', 'table') tuples, return first existing "schema.table"
    """
    for schema, table in candidates:
        full = f"{schema}.{table}"
        oid = cur.execute("SELECT OBJECT_ID(?, 'U')", full).fetchone()[0]
        if oid is not None:
            return full
    raise RuntimeError(f"None of candidate tables exist: {candidates}")


def table_exists(cur: pyodbc.Cursor, full_name: str) -> bool:
    return cur.execute("SELECT OBJECT_ID(?, 'U')", full_name).fetchone()[0] is not None


def fetch_sample_keys(cur: pyodbc.Cursor, sql: str, params: Tuple = (), limit: int = 10) -> List[str]:
    rows = cur.execute(sql, params).fetchall()
    out: List[str] = []
    for r in rows:
        if len(out) >= limit:
            break
        v = r[0]
        if v is None:
            continue
        out.append(str(v))
    return out[:limit]



def insert_job_run_running(cur: pyodbc.Cursor, job_name: str) -> int:
    cur.execute(
        """
        INSERT INTO meta.job_run (job_name, start_time, status)
        OUTPUT INSERTED.job_run_id
        VALUES (?, ?, 'running');
        """,
        job_name,
        utcnow_sql_naive(),
    )
    row = cur.fetchone()
    if row is None or row[0] is None:
        raise RuntimeError("Failed to fetch job_run_id via OUTPUT INSERTED.job_run_id")
    return int(row[0])


def update_job_run_success(
    cur: pyodbc.Cursor,
    job_run_id: int,
    records_read: int,
    records_written: int,
    duration_ms: int,
    metrics_json: str,
):
    cur.execute(
        """
        UPDATE meta.job_run
        SET end_time = ?,
            status = 'success',
            records_read = ?,
            records_written = ?,
            duration_ms = ?,
            metrics_json = ?
        WHERE job_run_id = ?;
        """,
        utcnow_sql_naive(),
        int(records_read),
        int(records_written),
        int(duration_ms),
        metrics_json,
        int(job_run_id),
    )


def update_job_run_failed(
    cur: pyodbc.Cursor,
    job_run_id: int,
    error_message: str,
    duration_ms: int,
    metrics_json: str,
):
    cur.execute(
        """
        UPDATE meta.job_run
        SET end_time = ?,
            status = 'failed',
            error_message = ?,
            duration_ms = ?,
            metrics_json = ?
        WHERE job_run_id = ?;
        """,
        utcnow_sql_naive(),
        error_message[:8000],
        int(duration_ms),
        metrics_json,
        int(job_run_id),
    )


def insert_dq_result(
    cur: pyodbc.Cursor,
    job_run_id: int,
    check_name: str,
    severity: str,
    metric_value: Optional[float],
    numerator: Optional[int],
    denominator: Optional[int],
    details: Dict[str, Any],
    sample_keys: Optional[List[str]] = None,
) -> None:
    cur.execute(
        """
        INSERT INTO meta.dq_check_result
          (job_run_id, check_name, severity, metric_value, numerator, denominator, sample_keys, details_json)
        VALUES
          (?, ?, ?, ?, ?, ?, ?, ?);
        """,
        int(job_run_id),
        check_name,
        severity,
        metric_value,
        numerator,
        denominator,
        dumps_json((sample_keys or [])[:10]) if sample_keys is not None else None,
        dumps_json(details),
    )



def check_raw_users_pk_unique(cur: pyodbc.Cursor, raw_users: str) -> Tuple[Dict[str, Any], int, List[str]]:
    total, distinct_ids = cur.execute(
        f"SELECT COUNT_BIG(*), COUNT_BIG(DISTINCT id) FROM {raw_users};"
    ).fetchone()
    dup = int(total - distinct_ids)

    sample = []
    if dup > 0:
        sample = fetch_sample_keys(
            cur,
            f"""
            SELECT TOP (10) CAST(id AS NVARCHAR(255)) AS k
            FROM {raw_users}
            GROUP BY id
            HAVING COUNT_BIG(*) > 1
            ORDER BY COUNT_BIG(*) DESC;
            """,
        )

    details = {"table": raw_users, "total_rows": int(total), "distinct_ids": int(distinct_ids), "duplicate_rows": dup}
    return details, int(total), sample


def check_raw_users_email_missing(cur: pyodbc.Cursor, raw_users: str) -> Tuple[Dict[str, Any], int, List[str]]:
    total, invalid_json, missing_email = cur.execute(
        f"""
        SELECT
          COUNT_BIG(*) AS total,
          SUM(CASE WHEN ISJSON(raw_payload) = 0 THEN 1 ELSE 0 END) AS invalid_json,
          SUM(CASE
                WHEN ISJSON(raw_payload) = 0 THEN 1
                WHEN NULLIF(LTRIM(RTRIM(JSON_VALUE(raw_payload,'$.email'))),'') IS NULL THEN 1
                ELSE 0
              END) AS missing_email
        FROM {raw_users};
        """
    ).fetchone()

    total = int(total)
    invalid_json = int(invalid_json or 0)
    missing_email = int(missing_email or 0)
    rate = (missing_email / total) if total else None

    sample = []
    if missing_email > 0:
        sample = fetch_sample_keys(
            cur,
            f"""
            SELECT TOP (10) CAST(id AS NVARCHAR(255)) AS k
            FROM {raw_users}
            WHERE
              ISJSON(raw_payload) = 0
              OR NULLIF(LTRIM(RTRIM(JSON_VALUE(raw_payload,'$.email'))),'') IS NULL
            ORDER BY id;
            """,
        )

    details = {
        "table": raw_users,
        "total_rows": total,
        "invalid_json_rows": invalid_json,
        "missing_email_rows": missing_email,
        "missing_rate": rate,
        "email_json_path": "$.email",
    }
    return details, total, sample


def check_identity_map_email_dupe(cur: pyodbc.Cursor) -> Tuple[Dict[str, Any], int, List[str]]:
    row = cur.execute(
        """
        WITH base AS (
          SELECT email_normalized
          FROM cur.person_identity_map
          WHERE NULLIF(LTRIM(RTRIM(email_normalized)),'') IS NOT NULL
        ),
        grp AS (
          SELECT email_normalized, COUNT_BIG(*) AS cnt
          FROM base
          GROUP BY email_normalized
        )
        SELECT
          SUM(cnt) AS rows_with_email,
          SUM(CASE WHEN cnt > 1 THEN cnt ELSE 0 END) AS duplicated_rows,
          SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END) AS duplicated_email_values
        FROM grp;
        """
    ).fetchone()

    rows_with_email = int(row[0] or 0)
    duplicated_rows = int(row[1] or 0)
    duplicated_email_values = int(row[2] or 0)
    rate = (duplicated_rows / rows_with_email) if rows_with_email else None

    sample = []
    if duplicated_email_values > 0:
        sample = fetch_sample_keys(
            cur,
            """
            WITH grp AS (
              SELECT email_normalized, COUNT_BIG(*) AS cnt
              FROM cur.person_identity_map
              WHERE NULLIF(LTRIM(RTRIM(email_normalized)),'') IS NOT NULL
              GROUP BY email_normalized
              HAVING COUNT_BIG(*) > 1
            )
            SELECT TOP (10) email_normalized
            FROM grp
            ORDER BY cnt DESC, email_normalized;
            """,
        )

    details = {
        "table": "cur.person_identity_map",
        "rows_with_email": rows_with_email,
        "duplicated_rows": duplicated_rows,
        "duplicated_email_values": duplicated_email_values,
        "duplicate_rate": rate,
        "definition": "rows in email groups with count>1 / rows_with_email",
    }
    return details, rows_with_email, sample



def check_fact_submission_person_coverage(cur: pyodbc.Cursor) -> Tuple[Dict[str, Any], int, List[str]]:
    total, null_fk, bad_fk = cur.execute(
        """
        WITH x AS (
          SELECT
            f.canvas_submission_id,
            f.gies_person_id,
            CASE
              WHEN f.gies_person_id IS NULL THEN 0
              WHEN s.gies_person_id IS NULL THEN 1
              ELSE 0
            END AS is_bad_fk
          FROM cur.fact_submission f
          LEFT JOIN cur.dim_student s
            ON s.gies_person_id = f.gies_person_id
        )
        SELECT
          COUNT_BIG(*) AS total,
          SUM(CASE WHEN gies_person_id IS NULL THEN 1 ELSE 0 END) AS null_fk,
          SUM(is_bad_fk) AS bad_fk
        FROM x;
        """
    ).fetchone()

    total = int(total)
    null_fk = int(null_fk or 0)
    bad_fk = int(bad_fk or 0)
    covered = total - null_fk - bad_fk
    coverage = (covered / total) if total else None

    sample = []
    if (null_fk + bad_fk) > 0:
        sample = fetch_sample_keys(
            cur,
            """
            SELECT TOP (10) CAST(f.canvas_submission_id AS NVARCHAR(255)) AS k
            FROM cur.fact_submission f
            LEFT JOIN cur.dim_student s
              ON s.gies_person_id = f.gies_person_id
            WHERE f.gies_person_id IS NULL OR (f.gies_person_id IS NOT NULL AND s.gies_person_id IS NULL)
            ORDER BY f.canvas_submission_id;
            """,
        )

    details = {
        "table": "cur.fact_submission",
        "total_rows": total,
        "null_gies_person_id_rows": null_fk,
        "bad_fk_rows": bad_fk,
        "covered_rows": covered,
        "coverage_rate": coverage,
    }
    return details, total, sample



def check_fact_submission_course_coverage(cur: pyodbc.Cursor) -> Tuple[Dict[str, Any], int, List[str]]:
    total, null_fk, bad_fk = cur.execute(
        """
        WITH x AS (
          SELECT
            f.canvas_submission_id,
            f.course_key,
            CASE
              WHEN f.course_key IS NULL THEN 0
              WHEN c.course_key IS NULL THEN 1
              ELSE 0
            END AS is_bad_fk
          FROM cur.fact_submission f
          LEFT JOIN cur.dim_course c
            ON c.course_key = f.course_key
        )
        SELECT
          COUNT_BIG(*) AS total,
          SUM(CASE WHEN course_key IS NULL THEN 1 ELSE 0 END) AS null_fk,
          SUM(is_bad_fk) AS bad_fk
        FROM x;
        """
    ).fetchone()

    total = int(total)
    null_fk = int(null_fk or 0)
    bad_fk = int(bad_fk or 0)
    covered = total - null_fk - bad_fk
    coverage = (covered / total) if total else None

    sample = []
    if (null_fk + bad_fk) > 0:
        sample = fetch_sample_keys(
            cur,
            """
            SELECT TOP (10) CAST(f.canvas_submission_id AS NVARCHAR(255)) AS k
            FROM cur.fact_submission f
            LEFT JOIN cur.dim_course c
              ON c.course_key = f.course_key
            WHERE f.course_key IS NULL OR (f.course_key IS NOT NULL AND c.course_key IS NULL)
            ORDER BY f.canvas_submission_id;
            """,
        )

    details = {
        "table": "cur.fact_submission",
        "total_rows": total,
        "null_course_key_rows": null_fk,
        "bad_fk_rows": bad_fk,
        "covered_rows": covered,
        "coverage_rate": coverage,
    }
    return details, total, sample


def check_reconcile_raw_vs_fact(cur: pyodbc.Cursor, raw_submissions: str) -> Tuple[Dict[str, Any], int, List[str]]:
    raw_total, raw_invalid_json = cur.execute(
        f"""
        SELECT
          COUNT_BIG(*) AS total,
          SUM(CASE WHEN ISJSON(raw_payload) = 0 THEN 1 ELSE 0 END) AS invalid_json
        FROM {raw_submissions};
        """
    ).fetchone()
    raw_total = int(raw_total)
    raw_invalid_json = int(raw_invalid_json or 0)

    fact_total = int(cur.execute("SELECT COUNT_BIG(*) FROM cur.fact_submission;").fetchone()[0])

    missing_in_fact = int(
        cur.execute(
            f"""
            SELECT COUNT_BIG(*)
            FROM {raw_submissions} r
            WHERE NOT EXISTS (
              SELECT 1 FROM cur.fact_submission f WHERE f.canvas_submission_id = r.id
            );
            """
        ).fetchone()[0]
    )

    extras_in_fact = int(
        cur.execute(
            f"""
            SELECT COUNT_BIG(*)
            FROM cur.fact_submission f
            WHERE NOT EXISTS (
              SELECT 1 FROM {raw_submissions} r WHERE r.id = f.canvas_submission_id
            );
            """
        ).fetchone()[0]
    )

    orphan_person = int(cur.execute("SELECT COUNT_BIG(*) FROM cur.fact_submission WHERE gies_person_id IS NULL;").fetchone()[0])
    orphan_course = int(cur.execute("SELECT COUNT_BIG(*) FROM cur.fact_submission WHERE course_key IS NULL;").fetchone()[0])

    diff = fact_total - raw_total

    # sample keys: prioritize missing_in_fact ids; if none, show extras ids
    sample: List[str] = []
    if missing_in_fact > 0:
        sample = fetch_sample_keys(
            cur,
            f"""
            SELECT TOP (10) CAST(r.id AS NVARCHAR(255)) AS k
            FROM {raw_submissions} r
            WHERE NOT EXISTS (
              SELECT 1 FROM cur.fact_submission f WHERE f.canvas_submission_id = r.id
            )
            ORDER BY r.id;
            """,
        )
    elif extras_in_fact > 0:
        sample = fetch_sample_keys(
            cur,
            f"""
            SELECT TOP (10) CAST(f.canvas_submission_id AS NVARCHAR(255)) AS k
            FROM cur.fact_submission f
            WHERE NOT EXISTS (
              SELECT 1 FROM {raw_submissions} r WHERE r.id = f.canvas_submission_id
            )
            ORDER BY f.canvas_submission_id;
            """,
        )

    details = {
        "raw_table": raw_submissions,
        "fact_table": "cur.fact_submission",
        "raw_rows": raw_total,
        "fact_rows": fact_total,
        "diff_fact_minus_raw": diff,
        "missing_in_fact": missing_in_fact,
        "extras_in_fact": extras_in_fact,
        "parse_fail_raw_invalid_json": raw_invalid_json,
        "orphans_in_fact_person_null": orphan_person,
        "orphans_in_fact_course_null": orphan_course,
        "notes": [
            "missing_in_fact",
            "orphans",
        ],
    }
    return details, raw_total, sample


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--conn", required=True, help="ODBC connection string")
    ap.add_argument("--job-name", default="dq.run_checks", help="job name for meta.job_run")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    conn = pyodbc.connect(args.conn, autocommit=False)
    cur = conn.cursor()

    # Ensure meta tables / columns
    cur.execute(DDL)
    conn.commit()

    run_perf_start = time.perf_counter()

    job_run_id = insert_job_run_running(cur, args.job_name)
    conn.commit()
    logging.info("job_run_id=%s status=running", job_run_id)

    records_read = 0
    records_written = 0

    # metrics_json payload (Step 3.1)
    metrics: Dict[str, Any] = {
        "raw_users_read": 0,
        "raw_courses_read": 0,
        "raw_submissions_read": 0,
        "checks_total": 0,
        "checks_fail_warn": 0,
        "checks_fail_error": 0,
        "orphans": {},
        "tables": {},
        "checks": [],
    }

    def record_check_metric(check_name: str, severity: str, numerator: Optional[int], denominator: Optional[int], extra: Dict[str, Any]):
        # define "fail" loosely for observability (warn/error with numerator>0, or thresholds already encoded)
        fail = False
        if severity in ("warn", "error"):
            if numerator is not None and int(numerator) > 0:
                fail = True
        metrics["checks_total"] += 1
        if fail and severity == "warn":
            metrics["checks_fail_warn"] += 1
        if fail and severity == "error":
            metrics["checks_fail_error"] += 1

        entry = {"check_name": check_name, "severity": severity, "numerator": numerator, "denominator": denominator}
        entry.update(extra)
        metrics["checks"].append(entry)

    try:
        raw_users = resolve_table(cur, [("raw", "users"), ("raw", "canvas_users")])
        raw_submissions = resolve_table(cur, [("raw", "submissions"), ("raw", "canvas_submissions")])

        metrics["tables"]["raw_users"] = raw_users
        metrics["tables"]["raw_submissions"] = raw_submissions

        # Optional: if courses table exists, record count (useful metric)
        try:
            raw_courses = resolve_table(cur, [("raw", "courses"), ("raw", "canvas_courses")])
            metrics["tables"]["raw_courses"] = raw_courses
            metrics["raw_courses_read"] = int(cur.execute(f"SELECT COUNT_BIG(*) FROM {raw_courses};").fetchone()[0])
        except Exception:
            pass

        metrics["raw_users_read"] = int(cur.execute(f"SELECT COUNT_BIG(*) FROM {raw_users};").fetchone()[0])
        metrics["raw_submissions_read"] = int(cur.execute(f"SELECT COUNT_BIG(*) FROM {raw_submissions};").fetchone()[0])

        # 1) raw.users PK unique
        d1, r1, s1 = check_raw_users_pk_unique(cur, raw_users)
        records_read += r1
        sev1 = "error" if d1["duplicate_rows"] > 0 else "info"
        insert_dq_result(
            cur, job_run_id,
            check_name="raw.users.pk_unique.duplicate_rows",
            severity=sev1,
            metric_value=float(d1["duplicate_rows"]),
            numerator=int(d1["duplicate_rows"]),
            denominator=int(d1["total_rows"]),
            details=d1,
            sample_keys=s1,
        )
        record_check_metric("raw.users.pk_unique.duplicate_rows", sev1, int(d1["duplicate_rows"]), int(d1["total_rows"]), {"sample_keys_count": len(s1)})
        records_written += 1

        # 2) raw.users email missing rate
        d2, r2, s2 = check_raw_users_email_missing(cur, raw_users)
        records_read += r2
        sev2 = "warn" if (d2["missing_rate"] is not None and d2["missing_rate"] > 0.05) else "info"
        insert_dq_result(
            cur, job_run_id,
            check_name="raw.users.email_missing_rate",
            severity=sev2,
            metric_value=float(d2["missing_rate"]) if d2["missing_rate"] is not None else None,
            numerator=int(d2["missing_email_rows"]),
            denominator=int(d2["total_rows"]),
            details=d2,
            sample_keys=s2,
        )
        record_check_metric("raw.users.email_missing_rate", sev2, int(d2["missing_email_rows"]), int(d2["total_rows"]), {"threshold": 0.05, "sample_keys_count": len(s2)})
        records_written += 1

        # 3) identity_map email duplicate rate
        d3, r3, s3 = check_identity_map_email_dupe(cur)
        records_read += r3
        sev3 = "warn" if (d3["duplicate_rate"] is not None and d3["duplicate_rate"] > 0.0) else "info"
        insert_dq_result(
            cur, job_run_id,
            check_name="identity_map.email_duplicate_rate",
            severity=sev3,
            metric_value=float(d3["duplicate_rate"]) if d3["duplicate_rate"] is not None else None,
            numerator=int(d3["duplicated_rows"]),
            denominator=int(d3["rows_with_email"]),
            details=d3,
            sample_keys=s3,
        )
        record_check_metric("identity_map.email_duplicate_rate", sev3, int(d3["duplicated_rows"]), int(d3["rows_with_email"]), {"sample_keys_count": len(s3)})
        records_written += 1

        # curated tables present?
        has_fact = table_exists(cur, "cur.fact_submission")
        has_dim_student = table_exists(cur, "cur.dim_student")
        has_dim_course = table_exists(cur, "cur.dim_course")

        # 4) fact_submission gies_person_id coverage
        if has_fact and has_dim_student:
            d4, r4, s4 = check_fact_submission_person_coverage(cur)
            records_read += r4
            miss = int(d4["null_gies_person_id_rows"]) + int(d4["bad_fk_rows"])
            sev4 = "warn" if miss > 0 else "info"
            insert_dq_result(
                cur, job_run_id,
                check_name="fact_submission.person_fk_coverage",
                severity=sev4,
                metric_value=float(d4["coverage_rate"]) if d4["coverage_rate"] is not None else None,
                numerator=int(miss),
                denominator=int(d4["total_rows"]),
                details=d4,
                sample_keys=s4,
            )
            metrics["orphans"]["person_fk_missing"] = miss
            record_check_metric("fact_submission.person_fk_coverage", sev4, miss, int(d4["total_rows"]), {"sample_keys_count": len(s4)})
        else:
            sev4 = "warn"
            insert_dq_result(
                cur, job_run_id,
                check_name="fact_submission.person_fk_coverage",
                severity=sev4,
                metric_value=None,
                numerator=None,
                denominator=None,
                details={"skipped": True, "reason": "Missing cur.fact_submission or cur.dim_student"},
                sample_keys=[],
            )
            record_check_metric("fact_submission.person_fk_coverage", sev4, None, None, {"skipped": True})
        records_written += 1

        # 5) fact_submission course_key coverage
        if has_fact and has_dim_course:
            d5, r5, s5 = check_fact_submission_course_coverage(cur)
            records_read += r5
            miss = int(d5["null_course_key_rows"]) + int(d5["bad_fk_rows"])
            sev5 = "warn" if miss > 0 else "info"
            insert_dq_result(
                cur, job_run_id,
                check_name="fact_submission.course_fk_coverage",
                severity=sev5,
                metric_value=float(d5["coverage_rate"]) if d5["coverage_rate"] is not None else None,
                numerator=int(miss),
                denominator=int(d5["total_rows"]),
                details=d5,
                sample_keys=s5,
            )
            metrics["orphans"]["course_fk_missing"] = miss
            record_check_metric("fact_submission.course_fk_coverage", sev5, miss, int(d5["total_rows"]), {"sample_keys_count": len(s5)})
        else:
            sev5 = "warn"
            insert_dq_result(
                cur, job_run_id,
                check_name="fact_submission.course_fk_coverage",
                severity=sev5,
                metric_value=None,
                numerator=None,
                denominator=None,
                details={"skipped": True, "reason": "Missing cur.fact_submission or cur.dim_course"},
                sample_keys=[],
            )
            record_check_metric("fact_submission.course_fk_coverage", sev5, None, None, {"skipped": True})
        records_written += 1

        # 6) reconcile raw.submissions vs fact
        if has_fact:
            d6, r6, s6 = check_reconcile_raw_vs_fact(cur, raw_submissions)
            records_read += r6
            sev6 = "warn" if (d6["missing_in_fact"] > 0 or d6["extras_in_fact"] > 0) else "info"
            # numerator: number of mismatched rows (missing+extras) makes it easier to reason about "fail"
            mismatch = int(d6["missing_in_fact"]) + int(d6["extras_in_fact"])
            insert_dq_result(
                cur, job_run_id,
                check_name="reconcile.raw_submissions_vs_fact_submission",
                severity=sev6,
                metric_value=float(d6["diff_fact_minus_raw"]),
                numerator=mismatch,
                denominator=int(d6["raw_rows"]),
                details=d6,
                sample_keys=s6,
            )
            record_check_metric("reconcile.raw_submissions_vs_fact_submission", sev6, mismatch, int(d6["raw_rows"]), {"sample_keys_count": len(s6)})
        else:
            sev6 = "warn"
            insert_dq_result(
                cur, job_run_id,
                check_name="reconcile.raw_submissions_vs_fact_submission",
                severity=sev6,
                metric_value=None,
                numerator=None,
                denominator=None,
                details={"skipped": True, "reason": "Missing cur.fact_submission"},
                sample_keys=[],
            )
            record_check_metric("reconcile.raw_submissions_vs_fact_submission", sev6, None, None, {"skipped": True})
        records_written += 1

        duration_ms = int((time.perf_counter() - run_perf_start) * 1000)
        metrics_json = dumps_json(metrics)

        update_job_run_success(
            cur,
            job_run_id,
            records_read=records_read,
            records_written=records_written,
            duration_ms=duration_ms,
            metrics_json=metrics_json,
        )
        conn.commit()
        logging.info(
            "job_run_id=%s status=success records_read=%s records_written=%s duration_ms=%s",
            job_run_id,
            records_read,
            records_written,
            duration_ms,
        )

    except Exception as e:
        conn.rollback()
        duration_ms = int((time.perf_counter() - run_perf_start) * 1000)
        metrics["exception"] = f"{type(e).__name__}: {e}"
        metrics_json = dumps_json(metrics)
        msg = f"{type(e).__name__}: {e}"
        try:
            update_job_run_failed(cur, job_run_id, msg, duration_ms=duration_ms, metrics_json=metrics_json)
            conn.commit()
        except Exception:
            pass
        logging.exception("job_run_id=%s status=failed", job_run_id)
        raise


if __name__ == "__main__":
    main()
