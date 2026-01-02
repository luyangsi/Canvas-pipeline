#!/usr/bin/env python3
import os
import re
import json
import sys
import pyodbc

"""
build_identity_map.py (Azure SQL)

- Read raw.canvas_users (columns: id, raw_payload)
- Parse raw_payload JSON to extract an email (search common keys recursively)
- Normalize email: lower(trim())
- Insert one primary row per normalized email into cur.person_identity_map with:
    canvas_user_id, email_normalized, match_method='email_exact', match_confidence=100
- Any other canvas_user_id that share the same normalized email are recorded as a WARN
  in cur.person_identity_map_dq (duplicate email)
"""

EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")


def find_email(obj):
    if obj is None:
        return None
    if isinstance(obj, str):
        s = obj.strip()
        if EMAIL_RE.match(s):
            return s
        return None
    if isinstance(obj, dict):
        for k in ("email", "email_address", "login_id", "user_email", "user", "contact"):
            if k in obj and obj[k]:
                e = find_email(obj[k])
                if e:
                    return e
        for v in obj.values():
            e = find_email(v)
            if e:
                return e
    if isinstance(obj, list):
        for item in obj:
            e = find_email(item)
            if e:
                return e
    return None


def normalize_email(e):
    if not e:
        return None
    return e.strip().lower()


def ensure_schema_and_tables(cur):
    cur.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'cur')
    BEGIN
        EXEC('CREATE SCHEMA cur');
    END
    """)

    cur.execute("""
    IF OBJECT_ID('cur.person_identity_map','U') IS NULL
    BEGIN
        CREATE TABLE cur.person_identity_map (
            canvas_user_id BIGINT NOT NULL PRIMARY KEY,
            email_normalized NVARCHAR(320) NOT NULL,
            match_method NVARCHAR(50) NOT NULL,
            match_confidence INT NOT NULL
        );
    END
    """)

    cur.execute("""
    IF OBJECT_ID('cur.person_identity_map_dq','U') IS NULL
    BEGIN
        CREATE TABLE cur.person_identity_map_dq (
            canvas_user_id BIGINT NULL,
            email_normalized NVARCHAR(320) NULL,
            reason NVARCHAR(100) NULL,
            severity NVARCHAR(20) NULL,
            primary_canvas_user_id BIGINT NULL
        );
    END
    """)


def main():
    dsn = os.getenv("DB_CONN")
    if not dsn:
        print("DB_CONN environment variable required (Azure SQL connection string)", file=sys.stderr)
        sys.exit(2)

    conn = pyodbc.connect(dsn, autocommit=False)
    try:
        cur = conn.cursor()
        ensure_schema_and_tables(cur)

        # ✅ FIX: raw.canvas_users has only (id, raw_payload, updated_at, ingested_at)
        cur.execute("""
        SELECT id, raw_payload
        FROM raw.canvas_users;
        """)
        rows = cur.fetchall()

        email_map = {}  # email_normalized -> list of canvas_user_id

        for row_id, raw_payload in rows:
            payload = None
            try:
                if raw_payload is None:
                    payload = None
                elif isinstance(raw_payload, (dict, list)):
                    payload = raw_payload
                else:
                    # pyodbc returns str for NVARCHAR(MAX)
                    payload = json.loads(raw_payload)
            except Exception:
                # fallback: treat as string
                payload = raw_payload

            email = find_email(payload)
            email_norm = normalize_email(email)
            if email_norm:
                email_map.setdefault(email_norm, []).append(int(row_id))

        primaries = []
        dq_rows = []
        for email_norm, ids in email_map.items():
            ids_sorted = sorted(set(ids))
            primary = ids_sorted[0]
            primaries.append((primary, email_norm, "email_exact", 100))
            for dup_id in ids_sorted[1:]:
                dq_rows.append((dup_id, email_norm, "duplicate_email", "warn", primary))

        # cleanup existing rows for involved ids
        if primaries:
            cur.fast_executemany = True
            cur.executemany(
                "DELETE FROM cur.person_identity_map WHERE canvas_user_id = ?",
                [(p[0],) for p in primaries],
            )

        if dq_rows:
            cur.fast_executemany = True
            cur.executemany(
                "DELETE FROM cur.person_identity_map_dq WHERE canvas_user_id = ?",
                [(r[0],) for r in dq_rows],
            )

        # insert primaries
        if primaries:
            cur.fast_executemany = True
            cur.executemany(
                """
                INSERT INTO cur.person_identity_map
                (canvas_user_id, email_normalized, match_method, match_confidence)
                VALUES (?, ?, ?, ?)
                """,
                primaries,
            )

        # insert dq warnings
        if dq_rows:
            cur.fast_executemany = True
            cur.executemany(
                """
                INSERT INTO cur.person_identity_map_dq
                (canvas_user_id, email_normalized, reason, severity, primary_canvas_user_id)
                VALUES (?, ?, ?, ?, ?)
                """,
                dq_rows,
            )

        conn.commit()
        print(f"OK: inserted {len(primaries)} primaries, {len(dq_rows)} dq rows")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

