
# Canvas Platform Data Pipeline (Azure SQL · Incremental Ingestion · Curated Model · DQ/Observability)

A production-style data pipeline that ingests **platform-like** datasets (Canvas-style JSONL), lands them in **Azure SQL** for traceability, transforms them into **analytics-ready curated tables**, and records **run auditing + data quality + schema drift** metadata for operational monitoring.

This repo focuses on:
- **data architecture** (layered schemas, stable keys)
- **integrations** (JSONL ingestion via ODBC)
- **workflow operations** (incremental runs, rerun-safe loads)
- **data standards & integrity** (DQ checks, sample keys, schema change detection)
- **documentation** (architecture + settings templates)

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

### Environment variables
Copy `settings.example.env` to `.env` and fill in values, or export variables in your shell.

Required:
- `DB_CONN` (ODBC connection string)
---

## Quickstart

### 1) Create schemas/tables
Run your DDL scripts against the **correct database** (not `master`).
### 2) Generate JSONL (if using simulation)
```bash
python3 -m src.simulate.generate_canvas --out-dir "${OUT_DIR:-data/out}"
```

### 3) Load raw (full)
Examples (adjust --id-field to match your JSONL):
```bash
python3 -m src.load.load_raw --file data/out/canvas_users.jsonl       --conn "$DB_CONN" --table raw.canvas_users       --id-field canvas_user_id
python3 -m src.load.load_raw --file data/out/canvas_courses.jsonl     --conn "$DB_CONN" --table raw.canvas_courses     --id-field canvas_course_id
python3 -m src.load.load_raw --file data/out/canvas_enrollments.jsonl --conn "$DB_CONN" --table raw.canvas_enrollments --id-field enrollment_id
python3 -m src.load.load_raw --file data/out/canvas_submissions.jsonl --conn "$DB_CONN" --table raw.canvas_submissions --id-field submission_id
```
### 4) Load raw (incremental via watermark)
```bash
python3 -m src.load.load_raw \
    --file data/out/canvas_courses.jsonl \
    --conn "$DB_CONN" \
    --table raw.canvas_courses \
    --id-field canvas_course_id \
    --source-name raw.canvas_courses \
    --incremental \
    --updated-field updated_at
```
### 5) Build identity map
```bash
python3 -m src.transform.build_identity_map --conn "$DB_CONN"
```
### 6) Build curated (incremental by ingested_at)
```bash
python3 -m src.transform.build_curated --conn "$DB_CONN"
```
If your implementation supports an explicit boundary:
```bash
python3 -m src.transform.build_curated --conn "$DB_CONN" --since-ingested-at "2026-01-01T00:00:00Z"
```
### 7) Run schema snapshot + change log
```bash
python3 -m src.meta.schema_snapshot --conn "$DB_CONN"
```
### 8) Run DQ checks + job audit
```bash
python3 -m src.dq.run_checks --conn "$DB_CONN"
```
