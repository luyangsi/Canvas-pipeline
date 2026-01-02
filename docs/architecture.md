
---

```markdown
# docs/architecture.md

# Architecture

## Purpose
This repository implements a production-style data pipeline that ingests **platform-like** datasets (Canvas-style JSONL), lands them in **Azure SQL** for traceability and replay, transforms them into **analytics-ready curated tables**, and records **run auditing + data quality + schema drift detection** metadata for operational monitoring.

The design emphasizes:
- clear data lifecycle separation (landing vs curated vs operational metadata)
- incremental processing and rerun safety
- integrity and governance hooks (DQ + auditing + schema drift)

---

## High-level data flow

```text
(1) Platform extracts (JSONL)
    - data/out/canvas_users.jsonl
    - data/out/canvas_courses.jsonl
    - data/out/canvas_enrollments.jsonl
    - data/out/canvas_submissions.jsonl
    |
    v
(2) Raw landing (Azure SQL: traceable / re-loadable)
    - raw.canvas_users
    - raw.canvas_courses
    - raw.canvas_enrollments
    - raw.canvas_submissions
    |
    v
(3) Identity mapping (person resolution)
    - cur.person_identity_map
    - meta.person_identity_map_dq  (identity-related exceptions)
    |
    v
(4) Curated analytics layer (dim/fact)
    - cur.dim_student
    - cur.dim_course
    - cur.fact_submission
    |
    v
(5) Operational monitoring & governance metadata
    - meta.job_run              (status, duration, metrics_json)
    - meta.dq_check_result      (severity, failed_rows, sample_keys)
    - meta.watermark            (incremental state per source)
    - meta.schema_snapshot      (schema JSON)
    - meta.schema_change_log    (schema diffs)
    |
    v
(6) Downstream consumption
    - Reporting / Visualization (Power BI / Tableau)
    - Monitoring views (meta.v_* for ops)
```

## Schemas and responsibilities

- raw — landed as‑is (traceable)
    - Stores records in a consistent raw shape including a raw_payload JSON for replay/debugging.
    - Designed for reprocessing/backfills and for separating ingestion from modeling.

- cur — curated outputs (analytics‑ready)
    - Parses and normalizes raw JSON into relational tables suitable for reporting.
    - Applies stable keying and join strategy:
        - canvas_user_id → gies_person_id (via identity mapping)
        - canvas_course_id → course_key (via course dimension)

- meta — operational metadata, quality, and change control
    - Provides observability and governance:
        - run auditing (meta.job_run)
        - data quality results (meta.dq_check_result)
        - incremental state (meta.watermark)
        - schema snapshots + diffs (meta.schema_snapshot, meta.schema_change_log)
    - Exposes monitoring views for triage:
        - meta.v_job_run_latest
        - meta.v_dq_latest_summary
        - meta.v_schema_changes_latest

---

## Incremental processing strategy

- Incremental raw ingestion (watermark + updated_at)
    - Loader: src/load/load_raw.py supports:
        - --source-name (e.g., raw.canvas_courses)
        - --incremental
        - --updated-field (default: updated_at)
    - Behavior per run:
        - Read meta.watermark.last_updated_at for the source_name.
        - Parse record_updated_at from each JSON record.
        - Skip records where record_updated_at <= watermark.
        - Upsert passing records into the raw table.
        - Update meta.watermark to the max written record_updated_at for the run.

- Incremental curated builds (ingested_at boundary)
    - Curated transforms avoid full scans by driving incrementality from raw.ingested_at.
    - A since_ingested_at concept (CLI or auto-resolved) filters raw reads to the changed window.
    - Curated outputs are applied via staging tables + MERGE to ensure idempotence and rerun safety.

---

## Observability and data quality

- Run auditing (meta.job_run)
    - Records status transitions (running → success/failed), start/end times, duration_ms.
    - Includes a flexible metrics_json payload for per‑table counts and diagnostics.

- Data quality (meta.dq_check_result)
    - Each check writes: severity (info/warn/error), failed_rows, sample_keys (up to 10 IDs).
    - Identity exceptions captured in meta.person_identity_map_dq for targeted triage.

- Change control (schema drift detection)
    - Snapshot + diff model:
        - meta.schema_snapshot stores point‑in‑time schema JSON per table.
        - meta.schema_change_log records deltas: table_added, table_missing, column_added, column_removed, column_changed.
    - Snapshot script (src/meta/schema_snapshot.py):
        - Reads INFORMATION_SCHEMA.COLUMNS, normalizes schema representation, diffs against the latest snapshot, writes change log events and a new snapshot.
    - Purpose: protect the pipeline from silent drift that can cause runtime breaks (missing columns, type mismatches, invalid object names).
