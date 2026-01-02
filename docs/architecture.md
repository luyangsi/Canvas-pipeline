# Architecture

## Purpose

This repository implements a small, production-style data pipeline that ingests **platform-like** records (Canvas-style JSONL), lands them in **Azure SQL** for traceability, transforms them into **analytics-ready curated tables**, and records **run auditing + data quality** results for operational visibility.

The design emphasizes:
- clear data lifecycle separation (landing vs curated vs operational metadata)
- idempotent reruns (safe to reprocess)
- integrity and governance hooks (DQ + audit logs)

---

## High-level data flow

(JSONL extracts) (Operational metadata)
data/out/*.jsonl ------------------> meta.job_run
| meta.dq_check_result
| meta.person_identity_map_dq
v
+-------------------+
| raw layer | raw.canvas_users
| (as-is landing) | raw.canvas_courses
| raw_payload JSON | raw.canvas_enrollments
+-------------------+ raw.canvas_submissions
|
v
+-------------------+
| identity mapping | cur.person_identity_map
+-------------------+
|
v
+-------------------+
| curated layer | cur.dim_student
| (dim/fact) | cur.dim_course
+-------------------+ cur.fact_submission
|
v
Reporting / BI (Power BI / Tableau)


---

## Data layers and schemas

This project uses schemas to separate stages in the data lifecycle:

### `raw` — landed as-is (traceable)
- Stores each record in a consistent raw shape:
  - primary identifier
  - `raw_payload` (JSON string)
  - timestamps (`updated_at`, `ingested_at`)
- Raw is designed to be **reloadable** and supports debugging/backfills.

### `cur` — curated outputs (analytics-ready)
- Parses and normalizes raw JSON into relational tables suitable for reporting.
- Implements key mapping to support consistent joins:
  - students/person identities
  - course keys

### `meta` — pipeline metadata and data quality
- Captures run-level audit information and DQ results.
- Supports operational workflows: monitoring, troubleshooting, and trend tracking.

### `stg` — reserved (optional extension)
- Not required for Day 1.
- Intended for future: validated/typed staging tables to isolate parsing + validation from curated modeling.

---

## Current tables

### Raw layer (`raw`)
- `raw.canvas_users`
  - as-is user records (one row per platform user)
- `raw.canvas_courses`
  - as-is course records (one row per course)
- `raw.canvas_enrollments`
  - as-is enrollment records (user ↔ course membership)
- `raw.canvas_submissions`
  - as-is submission records (assessment submissions)

> All raw tables store the original JSON in `raw_payload` and are keyed by a stable identifier.

### Curated layer (`cur`)
- `cur.person_identity_map`
  - unified identity mapping table used to connect platform identifiers to a consistent person key
- `cur.dim_student`
  - student dimension built from identity mapping (reporting-friendly attributes)
- `cur.dim_course`
  - course dimension built from raw course records
- `cur.fact_submission`
  - submission fact table mapped to student and course keys

### Metadata / DQ (`meta`)
- `meta.job_run`
  - one row per pipeline/DQ execution (status, timestamps, row counts)
- `meta.dq_check_result`
  - one row per DQ check execution (pass/fail + counts + samples)
- `meta.person_identity_map_dq`
  - DQ exceptions specific to identity mapping (e.g., duplicate email, missing email)

---

## Key transformations and business rules

### Identity mapping (person resolution)
Goal: produce a consistent person key for analytics.

- Normalize email (e.g., lowercased, trimmed) into `email_normalized`.
- Deterministic match strategy: email-based primary identity.
- Output:
  - `cur.person_identity_map` includes:
    - `gies_person_id` (surrogate key)
    - `canvas_user_id`
    - `email_normalized`
    - `match_method`, `match_confidence`
- Exceptions (e.g., missing email, duplicate email mapping) are written to:
  - `meta.person_identity_map_dq`

### Course mapping
Goal: stable course join key for reporting.

- `cur.dim_course` is keyed by the platform course id (e.g., `canvas_course_id`) and provides a surrogate `course_key` for joins.

### Fact submissions
Goal: analytics-ready submission records.

- Parse raw submissions into relational fields (submitted_at, score, etc.)
- Map:
  - `user_id` → `gies_person_id` (via identity map)
  - `course_id` → `course_key` (via course dimension)
- If mapping fails (e.g., submission user_id not found in identity map), it is tracked as a coverage/integrity issue and surfaced in DQ outputs.

---

## Workflow (Day 1 run sequence)

1. **DDL / Setup**
   - Create schemas and tables in Azure SQL (`raw`, `cur`, `meta`)

2. **Generate extracts (simulation)**
   - Produce JSONL files in `data/out/` (one JSON object per line)

3. **Load raw**
   - Upsert JSONL records into `raw.canvas_*` tables
   - Store original record in `raw_payload`

4. **Build identity map**
   - Create/update `cur.person_identity_map`
   - Write identity-related exceptions to `meta.person_identity_map_dq` (if any)

5. **Build curated tables**
   - Create/update `cur.dim_student`, `cur.dim_course`, `cur.fact_submission`

6. **Run DQ checks + job audit**
   - Insert/update `meta.job_run`
   - Write results to `meta.dq_check_result`

---

## Idempotency and reruns

The pipeline is designed to be safe to rerun:

- Raw ingestion uses upsert keyed by a stable identifier (last write wins for the same id).
- Curated tables are built with upsert semantics to avoid duplicates.
- DQ and job auditing append new run records for traceability.

---

## Operational notes (common failure modes)

- Azure SQL connectivity issues are often firewall-related:
  - `Client IP is not allowed` → add your public IP to SQL server firewall rules
- ODBC driver / pyodbc dependencies:
  - Missing Driver 18 / unixODBC can prevent connections
- Object not found errors:
  - `Invalid object name ...` typically means DDL wasn’t run in the correct database
  - Verify active DB with `SELECT DB_NAME();`
- macOS Python invocation:
  - use `python3` (not `python`) unless you set an alias

---

## Next extensions (not required for Day 1)

- Add `stg` layer for typed staging and stricter validation
- Add incremental processing with watermarks (avoid full reprocessing)
- Add schema snapshot + change log (change control)
- Add reporting views for BI consumption
