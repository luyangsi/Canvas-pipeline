# Canvas Platform Data Pipeline (Azure SQL + Identity Mapping + DQ/Auditing)

An end-to-end data engineering prototype that ingests Canvas-style JSONL, transforms it to analytics-ready structures, and loads into Azure SQL. Emphasizes data architecture, integrations, workflow operations, documentation, and data integrity with run auditing and data quality checks.

---

## Overview

Workflow:

- Ingest semi-structured platform records (JSONL)
- Land into a governed raw layer preserving original payloads
- Standardize & curate into dimensional/fact tables (curated layer)
- Maintain pipeline metadata and data quality outcomes (meta layer)

---

## Architecture

High-level flow:

```
Platform extracts (JSONL)
|
v
Azure SQL (raw layer)  <-- raw.canvas_* tables (raw_payload JSON storage)
|
v
Identity mapping (cur.person_identity_map) -----> Curated analytics (cur.dim_*, cur.fact_*)
|
v
Pipeline metadata & DQ (meta.job_run, meta.dq_results)  --> Reporting / visualization
```

Layered schemas:

- raw: landed as-is (traceable, re-loadable)
- stg: cleaned/validated (extension)
- cur: curated outputs for analytics/ML/reporting (dim/fact)
- meta: pipeline metadata, run logs, dq results

---

## Data model (curated layer)

Identity & key mapping:

- canvas_user_id → gies_person_id via cur.person_identity_map
- canvas_course_id → course_key via cur.dim_course

Curated facts join through these keys (e.g., cur.fact_submission mapped to gies_person_id and course_key). Coverage gaps are tracked and surfaced by DQ checks.

Curated tables (overview):

- cur.person_identity_map — unified identity mapping (email_normalized, canvas_user_id, match metadata, match_method, match_confidence, timestamps, active flag)
- cur.dim_student — student dimension from identity map
- cur.dim_course — course dimension from raw course records
- cur.fact_submission — submission fact from raw submissions

---

## Idempotency

Design supports safe re-runs without duplicates:

- Raw ingestion: upsert keyed on a stable identifier (--id-field)
- Curated transforms: upsert dims/facts by natural keys (canvas_course_id, submission_id)

Supports reprocessing, backfills, and repeated scheduled executions.

---

## Repo structure

```
Canvas-pipeline/
├─ README.md
├─ sql/
│  └─ 01_ddl.sql                 # schemas + tables (raw/cur/meta; stg optional)
├─ src/
│  ├─ simulate/
│  │  └─ generate_canvas.py      # generates platform-like JSONL
│  ├─ load/
│  │  └─ load_raw.py             # JSONL -> raw.* tables (upsert)
│  ├─ transform/
│  │  ├─ build_identity_map.py
│  │  └─ build_curated.py
│  └─ dq/
│     └─ run_checks.py           # writes meta.job_run + meta.dq_results
├─ data/
│  └─ out/                       # generated JSONL outputs (NOT committed)
└─ docs/
    └─ architecture.md
```

---

## Setup

Prerequisites:

- VS Code + MSSQL extension (connect via: Command Palette → MS SQL: Connect)
- Python 3.x (macOS: use python3)
- ODBC + pyodbc (macOS): unixodbc, msodbcsql18
- Python package: pyodbc

Install example:
```bash
python3 -m pip install pyodbc
```

---

## Azure SQL connectivity (VS Code + MSSQL extension)

- Server endpoint: <server>.database.windows.net
- Prefer SQL Authentication
- Verify with:
```sql
SELECT @@VERSION;
SELECT DB_NAME();
```

Firewall fix (if needed): Azure Portal → SQL server → Networking / Firewall rules → add your public IP.

---

## Configuration

Environment variable: DB_CONN

Example:
```bash
export DB_CONN="Driver={ODBC Driver 18 for SQL Server};Server=tcp:<server>.database.windows.net,1433;Database=<db>;Uid=<sql_user>;Pwd=<password>;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
```

Do not commit credentials. Use environment variables or a gitignored .env.

---

## Quickstart (end-to-end)

Run from repo root.

1) Create schemas/tables
```sql
-- open and execute
sql/01_ddl.sql
-- verify DB
SELECT DB_NAME();
```

2) Generate platform-like JSONL
```bash
python3 -m src.simulate.generate_canvas --out-dir data/out
# outputs: data/out/canvas_users.jsonl, canvas_courses.jsonl, canvas_enrollments.jsonl, canvas_submissions.jsonl
```

3) Load extracts into raw layer
```bash
python3 -m src.load.load_raw --file data/out/canvas_users.jsonl       --conn "$DB_CONN" --table raw.canvas_users       --id-field canvas_user_id
python3 -m src.load.load_raw --file data/out/canvas_courses.jsonl     --conn "$DB_CONN" --table raw.canvas_courses     --id-field canvas_course_id
python3 -m src.load.load_raw --file data/out/canvas_enrollments.jsonl --conn "$DB_CONN" --table raw.canvas_enrollments --id-field enrollment_id
python3 -m src.load.load_raw --file data/out/canvas_submissions.jsonl --conn "$DB_CONN" --table raw.canvas_submissions --id-field submission_id
```

Raw table pattern per resource:
- ..._id (PK), raw_payload (NVARCHAR(MAX)), updated_at (nullable), ingested_at (SYSUTCDATETIME())

4) Build identity map
```bash
python3 -m src.transform.build_identity_map --conn "$DB_CONN"
# writes cur.person_identity_map (deterministic match by email_normalized)
```

5) Build curated analytics tables
```bash
python3 -m src.transform.build_curated --conn "$DB_CONN"
# writes cur.dim_student, cur.dim_course, cur.fact_submission
```

6) Run DQ checks and audit logging
```bash
python3 -m src.dq.run_checks --conn "$DB_CONN"
# writes meta.job_run, meta.dq_results
```

Note: when using python -m do not include .py

---

## Data quality checks

Each run writes to meta.dq_results: check_name, table_name, severity, result, failed_rows, sample_keys, timestamps.

Current checks:
1. Raw users PK uniqueness
2. Raw users email missing rate
3. Identity map email duplication
4. Submission → person coverage (unresolved identities)
5. Submission → course coverage (unresolved courses)
6. Reconciliation between raw submissions and curated facts

---

## Run auditing (meta schema)

Workflow records:

- Insert meta.job_run with status='running' at start
- Update to status='success' with end_time, records_read, records_written on completion
- On failure, update to status='failed' and persist error_message

Provides traceability and operational visibility.

---

## Operational notes / pitfalls

- Run DDL in correct database: verify with SELECT DB_NAME()
- Keep DDL versioned in sql/01_ddl.sql and make idempotent
- IF OBJECT_ID(...) IS NULL won’t fix an existing incorrect table; alter or recreate as needed
- Avoid smart quotes; use ASCII quotes in scripts
- macOS: prefer python3
- pyodbc requires system libs (unixODBC)
- Connectivity issues are often firewall-related
- Use one raw table per resource type to avoid ID collisions

---

## Troubleshooting

- python: command not found → use python3
- Can't open lib 'ODBC Driver 18 for SQL Server' → install unixodbc and msodbcsql18
- Login timeout / Client IP not allowed → add your public IP to SQL server firewall
- Invalid object name raw.canvas_courses → run sql/01_ddl.sql in the correct DB
- Running modules: use python3 -m src.dq.run_checks (do not include .py)

Example queries to validate results:
```sql
SELECT COUNT(*) FROM raw.canvas_courses;
SELECT COUNT(*) FROM cur.person_identity_map;
SELECT COUNT(*) FROM cur.fact_submission;

SELECT TOP (10) * FROM meta.job_run ORDER BY start_time DESC;
SELECT TOP (20) * FROM meta.dq_results ORDER BY created_at DESC;
```

---

## Roadmap / next improvements

- Incremental processing via updated_at watermarks
- Schema snapshot & change detection logs (meta_schema_snapshot, meta_schema_change_log)
- Orchestrated scheduling/monitoring (managed triggers)
- Extend to more platform sources and unify identities across systems
- Enhance identity resolution with candidate matching and manual overrides

---
