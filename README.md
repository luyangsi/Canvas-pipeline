
# Canvas Platform Data Pipeline (Azure SQL · Incremental Ingestion · Curated Model · DQ/Observability)

A production-style data pipeline that ingests **platform-like** datasets (Canvas-style JSONL), lands them in **Azure SQL** for traceability, transforms them into **analytics-ready curated tables**, and records **run auditing + data quality + schema drift** metadata for operational monitoring.

This repo focuses on:
- **data architecture** (layered schemas, stable keys)
- **integrations** (JSONL ingestion via ODBC)
- **workflow operations** (incremental runs, rerun-safe loads)
- **data standards & integrity** (DQ checks, sample keys, schema change detection)
- **documentation** (architecture + settings templates)

---

## Highlights (Day 1–Day 2)

### Incremental raw ingestion (JSONL → Azure SQL)
- Minimal-change incremental loader using `pyodbc` upsert (UPDATE → INSERT if missing) into the `raw` layer.
- CLI controls for source-aware replay:
    - `--source-name` (e.g., `raw.canvas_courses`) to scope watermark state
    - `--incremental` to enable watermark filtering
    - `--updated-field` (default `updated_at`) to read record timestamps from JSON (missing → `NULL`)
- Incremental semantics:
    - Read `meta.watermark.last_updated_at` for `source_name`
    - For each record: parse `record_updated_at`; **skip** if `record_updated_at <= watermark`
    - Upsert only passing records
    - On success: update watermark to **max(record_updated_at)** written this run
- Acceptance behavior verified:
    - First run writes > 0
    - Second run with unchanged data writes ~0
    - Bumping one record’s timestamp only upserts that record

> Note: Records with missing `updated_at` cannot be compared; in incremental mode they may still be processed (implementation-dependent). Prefer emitting `updated_at` where possible.

### Curated build: incremental by `ingested_at`
- Curated transforms are incremental **without relying on source `updated_at` quality** by driving increments from `raw.ingested_at`.
- A `since_ingested_at` concept (optional CLI or auto-resolved) avoids full-table scans:
    - Raw reads for `raw.canvas_courses` and `raw.canvas_submissions` can be filtered by `ingested_at`
    - Running twice with no new raw data yields near-zero parsed rows
- Curated tables are built via **staging tables + MERGE** to support idempotent reruns:
    - `cur.dim_student` from identity mapping
    - `cur.dim_course` from raw courses JSON
    - `cur.fact_submission` from raw submissions JSON with FK lookups

### Observability: job runs + metrics payload
- Enhanced `meta.job_run` for operational monitoring without breaking compatibility:
    - Added `duration_ms` and `metrics_json` (flexible metrics payload)
- Each run records structured metrics such as:
    - per-table read/write counts
    - warn/error counts
    - orphan / missing-FK counts
    - per-check summary snippets

### Data quality: severity + sample keys
- DQ output is **debuggable**, not just pass/fail:
    - Each check writes a row to `meta.dq_check_result`
    - `severity` is meaningful (`info/warn/error`)
    - `sample_keys` stores up to **10 representative IDs** for triage
- Verified: runs write **≥ 6 checks** per execution, and warnings/errors produce sample keys (e.g., missing email IDs, orphan submission IDs).

### Change control: schema snapshots + diff logging
- Automated schema drift detection prevents pipeline breaks due to table/column drift:
    - `meta.schema_snapshot`: point-in-time schema JSON per table
    - `meta.schema_change_log`: detected deltas (`table_added`, `table_missing`, `column_added`, `column_removed`, `column_changed`)
- `src/meta/schema_snapshot.py`:
    - Pulls `INFORMATION_SCHEMA.COLUMNS` → normalized `schema_json`
    - Diffs against latest snapshot per table
    - Writes change events + stores a new snapshot
- Verified by intentionally altering a raw table and confirming change appears in `meta.schema_change_log`.

### BI-friendly views (ops & quality surfacing)
- Lightweight views for dashboards and monitoring:
    - `meta.v_dq_latest_summary`: latest result per check
    - `meta.v_job_run_latest`: recent run status + duration + metrics JSON
    - `meta.v_schema_changes_latest`: recent schema changes

---

## Architecture (at a glance)

Medallion-style layout:

- `raw`: as-is landing (traceability, replay)
- `cur`: curated dims/facts (analytics-ready)
- `meta`: run auditing, DQ telemetry, watermarking, schema drift detection

See `docs/architecture.md` for details.

---

## Database objects

### Raw (`raw`)
- `raw.canvas_users`
- `raw.canvas_courses`
- `raw.canvas_enrollments`
- `raw.canvas_submissions`

### Curated (`cur`)
- `cur.person_identity_map`
- `cur.dim_student`
- `cur.dim_course`
- `cur.fact_submission`

### Operational metadata / DQ / change control (`meta`)
- `meta.job_run`
- `meta.dq_check_result`
- `meta.person_identity_map_dq`
- `meta.watermark`
- `meta.schema_snapshot`
- `meta.schema_change_log`

### Monitoring views (`meta`)
- `meta.v_job_run_latest`
- `meta.v_dq_latest_summary`
- `meta.v_schema_changes_latest`

---

## Setup

### Prerequisites
- Python 3.x (macOS: use `python3`)
- `pyodbc`
- Microsoft ODBC Driver 18 for SQL Server + unixODBC (macOS)
- VS Code + MSSQL extension (optional but recommended for running SQL)

### Environment variables
Copy `settings.example.env` to `.env` and fill in values, or export variables in your shell.

Required:
- `DB_CONN` (ODBC connection string)

Optional:
- `LOG_LEVEL`
- `OUT_DIR`

---

## Quickstart

### 1) Create schemas/tables
Run your DDL scripts against the **correct database** (not `master`).

Sanity check:
```sql
SELECT DB_NAME();
2) Generate JSONL (if using simulation)
python3 -m src.simulate.generate_canvas --out-dir "${OUT_DIR:-data/out}"
3) Load raw (full)
Examples (adjust --id-field to match your JSONL):
python3 -m src.load.load_raw --file data/out/canvas_users.jsonl       --conn "$DB_CONN" --table raw.canvas_users       --id-field canvas_user_id
python3 -m src.load.load_raw --file data/out/canvas_courses.jsonl     --conn "$DB_CONN" --table raw.canvas_courses     --id-field canvas_course_id
python3 -m src.load.load_raw --file data/out/canvas_enrollments.jsonl --conn "$DB_CONN" --table raw.canvas_enrollments --id-field enrollment_id
python3 -m src.load.load_raw --file data/out/canvas_submissions.jsonl --conn "$DB_CONN" --table raw.canvas_submissions --id-field submission_id
4) Load raw (incremental via watermark)
python3 -m src.load.load_raw \
    --file data/out/canvas_courses.jsonl \
    --conn "$DB_CONN" \
    --table raw.canvas_courses \
    --id-field canvas_course_id \
    --source-name raw.canvas_courses \
    --incremental \
    --updated-field updated_at
5) Build identity map
python3 -m src.transform.build_identity_map --conn "$DB_CONN"
6) Build curated (incremental by ingested_at)
python3 -m src.transform.build_curated --conn "$DB_CONN"
If your implementation supports an explicit boundary:
python3 -m src.transform.build_curated --conn "$DB_CONN" --since-ingested-at "2026-01-01T00:00:00Z"
7) Run schema snapshot + change log
python3 -m src.meta.schema_snapshot --conn "$DB_CONN"
8) Run DQ checks + job audit
python3 -m src.dq.run_checks --conn "$DB_CONN"
Operations: what to look at
Latest run status
SELECT * FROM meta.v_job_run_latest;
Latest DQ summary (latest result per check)
SELECT * FROM meta.v_dq_latest_summary ORDER BY severity DESC, check_name;
Latest schema changes
SELECT TOP (50) * FROM meta.v_schema_changes_latest ORDER BY detected_at DESC;
Watermark state
SELECT * FROM meta.watermark ORDER BY source_name;
Security note
Never commit secrets. Store credentials in environment variables or local .env (gitignored).

---writer above content into the readme file