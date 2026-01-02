# file: src/transform/build_curated.py
import argparse
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pyodbc


# -------------------------
# helpers
# -------------------------
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def jget(d: Dict[str, Any], key: str, default=None):
    v = d.get(key, default)
    return default if v is None else v


def parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        if isinstance(s, str) and s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None


def chunked(rows: List[Tuple], n: int) -> Iterable[List[Tuple]]:
    for i in range(0, len(rows), n):
        yield rows[i : i + n]


# -------------------------
# DDL
# -------------------------
DDL = r"""
IF SCHEMA_ID('cur') IS NULL EXEC('CREATE SCHEMA cur');

-- dim_student: from cur.person_identity_map (canvas_user_id, email_normalized, match_method, match_confidence)
IF OBJECT_ID('cur.dim_student','U') IS NULL
BEGIN
  CREATE TABLE cur.dim_student (
    gies_person_id    BIGINT IDENTITY(1,1) NOT NULL,
    canvas_user_id    BIGINT NOT NULL,
    email_normalized  NVARCHAR(320) NULL,
    match_method      NVARCHAR(100) NULL,
    match_confidence  FLOAT NULL,
    updated_at        DATETIME2 NULL,
    ingested_at       DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_cur_dim_student PRIMARY KEY (gies_person_id),
    CONSTRAINT UQ_cur_dim_student_canvas_user UNIQUE (canvas_user_id)
  );
END

-- dim_course: parsed from raw.canvas_courses (id, raw_payload, updated_at)
IF OBJECT_ID('cur.dim_course','U') IS NULL
BEGIN
  CREATE TABLE cur.dim_course (
    course_key       INT IDENTITY(1,1) NOT NULL,
    canvas_course_id BIGINT NOT NULL,     -- maps to raw.canvas_courses.id
    name             NVARCHAR(400) NULL,
    course_code      NVARCHAR(200) NULL,
    workflow_state   NVARCHAR(50)  NULL,
    start_at         DATETIME2 NULL,
    end_at           DATETIME2 NULL,
    term_name        NVARCHAR(200) NULL,
    sis_course_id    NVARCHAR(200) NULL,
    account_id       BIGINT NULL,
    raw_updated_at   DATETIME2 NULL,
    updated_at       DATETIME2 NULL,
    ingested_at      DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_cur_dim_course PRIMARY KEY (course_key),
    CONSTRAINT UQ_cur_dim_course_canvas UNIQUE (canvas_course_id)
  );
END

-- fact_submission: parsed from raw.canvas_submissions (id, raw_payload, updated_at)
IF OBJECT_ID('cur.fact_submission','U') IS NULL
BEGIN
  CREATE TABLE cur.fact_submission (
    canvas_submission_id BIGINT NOT NULL,  -- maps to raw.canvas_submissions.id
    gies_person_id       BIGINT NULL,
    course_key           INT NULL,

    canvas_user_id       BIGINT NULL,
    canvas_course_id     BIGINT NULL,
    assignment_id        BIGINT NULL,

    submitted_at         DATETIME2 NULL,
    graded_at            DATETIME2 NULL,
    score                FLOAT NULL,
    attempt              INT NULL,

    late_flag            BIT NULL,
    missing_flag         BIT NULL,

    due_at               DATETIME2 NULL,
    on_time_flag         BIT NULL,

    workflow_state       NVARCHAR(50) NULL,
    raw_updated_at       DATETIME2 NULL,

    updated_at           DATETIME2 NULL,
    ingested_at          DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),

    CONSTRAINT PK_cur_fact_submission PRIMARY KEY (canvas_submission_id)
  );
END
"""


# -------------------------
# dim_student from person_identity_map
# -------------------------
def read_person_identity_map(cur: pyodbc.Cursor) -> List[Tuple]:
    rows = cur.execute("""
      SELECT canvas_user_id, email_normalized, match_method, match_confidence
      FROM cur.person_identity_map
      WHERE canvas_user_id IS NOT NULL
    """).fetchall()

    out: List[Tuple] = []
    now = utcnow()
    for canvas_user_id, email_norm, match_method, match_conf in rows:
        out.append((
            int(canvas_user_id),
            email_norm,
            match_method,
            float(match_conf) if match_conf is not None else None,
            now,
        ))
    return out


def merge_dim_student(conn: pyodbc.Connection, rows: List[Tuple], batch_size: int = 2000) -> None:
    if not rows:
        return
    cur = conn.cursor()
    cur.execute("IF OBJECT_ID('tempdb..#stg_student') IS NOT NULL DROP TABLE #stg_student;")
    cur.execute("""
      CREATE TABLE #stg_student (
        canvas_user_id   BIGINT NOT NULL,
        email_normalized NVARCHAR(320) NULL,
        match_method     NVARCHAR(100) NULL,
        match_confidence FLOAT NULL,
        updated_at       DATETIME2 NULL
      );
    """)

    cur.fast_executemany = True
    ins = """
      INSERT INTO #stg_student (canvas_user_id, email_normalized, match_method, match_confidence, updated_at)
      VALUES (?,?,?,?,?)
    """
    for part in chunked(rows, batch_size):
        cur.executemany(ins, part)

    cur.execute("""
      MERGE cur.dim_student WITH (HOLDLOCK) AS tgt
      USING #stg_student AS src
        ON tgt.canvas_user_id = src.canvas_user_id
      WHEN MATCHED THEN UPDATE SET
        tgt.email_normalized = src.email_normalized,
        tgt.match_method = src.match_method,
        tgt.match_confidence = src.match_confidence,
        tgt.updated_at = src.updated_at
      WHEN NOT MATCHED THEN INSERT
        (canvas_user_id, email_normalized, match_method, match_confidence, updated_at)
      VALUES
        (src.canvas_user_id, src.email_normalized, src.match_method, src.match_confidence, src.updated_at);
    """)
    conn.commit()


def load_student_key_map(cur: pyodbc.Cursor) -> Dict[int, int]:
    m: Dict[int, int] = {}
    for canvas_user_id, gies_person_id in cur.execute("""
        SELECT canvas_user_id, gies_person_id
        FROM cur.dim_student
    """).fetchall():
        m[int(canvas_user_id)] = int(gies_person_id)
    return m


# -------------------------
# dim_course from raw.canvas_courses
# -------------------------
def read_raw_courses(cur: pyodbc.Cursor) -> List[Tuple]:
    rows = cur.execute("""
      SELECT id, raw_payload, updated_at
      FROM raw.canvas_courses
    """).fetchall()

    out: List[Tuple] = []
    for course_id, raw_payload, raw_updated_at in rows:
        try:
            d = json.loads(raw_payload)
        except Exception:
            continue

        name = jget(d, "name")
        course_code = jget(d, "course_code")
        workflow_state = jget(d, "workflow_state")
        start_at = parse_dt(jget(d, "start_at"))
        end_at = parse_dt(jget(d, "end_at"))
        account_id = jget(d, "account_id")
        sis_course_id = jget(d, "sis_course_id")

        term_name = None
        term = d.get("term")
        if isinstance(term, dict):
            term_name = term.get("name")

        out.append((
            int(course_id),  # raw.canvas_courses.id
            name,
            course_code,
            workflow_state,
            start_at,
            end_at,
            term_name,
            sis_course_id,
            int(account_id) if account_id is not None else None,
            raw_updated_at,
            utcnow(),
        ))
    return out


def merge_dim_course(conn: pyodbc.Connection, rows: List[Tuple], batch_size: int = 2000) -> None:
    if not rows:
        return
    cur = conn.cursor()
    cur.execute("IF OBJECT_ID('tempdb..#stg_course') IS NOT NULL DROP TABLE #stg_course;")
    cur.execute("""
      CREATE TABLE #stg_course (
        canvas_course_id BIGINT NOT NULL,
        name             NVARCHAR(400) NULL,
        course_code      NVARCHAR(200) NULL,
        workflow_state   NVARCHAR(50)  NULL,
        start_at         DATETIME2 NULL,
        end_at           DATETIME2 NULL,
        term_name        NVARCHAR(200) NULL,
        sis_course_id    NVARCHAR(200) NULL,
        account_id       BIGINT NULL,
        raw_updated_at   DATETIME2 NULL,
        updated_at       DATETIME2 NULL
      );
    """)

    cur.fast_executemany = True
    ins = """
      INSERT INTO #stg_course
      (canvas_course_id,name,course_code,workflow_state,start_at,end_at,term_name,sis_course_id,account_id,raw_updated_at,updated_at)
      VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """
    for part in chunked(rows, batch_size):
        cur.executemany(ins, part)

    cur.execute("""
      MERGE cur.dim_course WITH (HOLDLOCK) AS tgt
      USING #stg_course AS src
        ON tgt.canvas_course_id = src.canvas_course_id
      WHEN MATCHED THEN UPDATE SET
        tgt.name = src.name,
        tgt.course_code = src.course_code,
        tgt.workflow_state = src.workflow_state,
        tgt.start_at = src.start_at,
        tgt.end_at = src.end_at,
        tgt.term_name = src.term_name,
        tgt.sis_course_id = src.sis_course_id,
        tgt.account_id = src.account_id,
        tgt.raw_updated_at = src.raw_updated_at,
        tgt.updated_at = src.updated_at
      WHEN NOT MATCHED THEN INSERT
        (canvas_course_id,name,course_code,workflow_state,start_at,end_at,term_name,sis_course_id,account_id,raw_updated_at,updated_at)
      VALUES
        (src.canvas_course_id,src.name,src.course_code,src.workflow_state,src.start_at,src.end_at,src.term_name,src.sis_course_id,src.account_id,src.raw_updated_at,src.updated_at);
    """)
    conn.commit()


def load_course_key_map(cur: pyodbc.Cursor) -> Dict[int, int]:
    m: Dict[int, int] = {}
    for canvas_course_id, course_key in cur.execute("""
        SELECT canvas_course_id, course_key FROM cur.dim_course
    """).fetchall():
        m[int(canvas_course_id)] = int(course_key)
    return m


# -------------------------
# fact_submission from raw.canvas_submissions
# -------------------------
def read_raw_submissions(
    cur: pyodbc.Cursor,
    student_key_map: Dict[int, int],
    course_key_map: Dict[int, int],
) -> Tuple[List[Tuple], int, int]:
    rows = cur.execute("""
      SELECT id, raw_payload, updated_at
      FROM raw.canvas_submissions
    """).fetchall()

    out: List[Tuple] = []
    skip_user = 0
    skip_course = 0

    for submission_id, raw_payload, raw_updated_at in rows:
        try:
            d = json.loads(raw_payload)
        except Exception:
            continue

        canvas_user_id = jget(d, "user_id")
        canvas_course_id = jget(d, "course_id")
        assignment_id = jget(d, "assignment_id")

        gies_person_id = None
        course_key = None

        if canvas_user_id is not None:
            gies_person_id = student_key_map.get(int(canvas_user_id))
            if gies_person_id is None:
                skip_user += 1

        if canvas_course_id is not None:
            course_key = course_key_map.get(int(canvas_course_id))
            if course_key is None:
                skip_course += 1

        submitted_at = parse_dt(jget(d, "submitted_at"))
        graded_at = parse_dt(jget(d, "graded_at"))
        score = jget(d, "score")
        attempt = jget(d, "attempt")
        workflow_state = jget(d, "workflow_state")

        late = jget(d, "late")
        missing = jget(d, "missing")

        # may not exist in submission payload
        due_at = parse_dt(jget(d, "due_at"))

        # on_time_flag:
        on_time_flag = None
        if due_at and submitted_at:
            on_time_flag = 1 if submitted_at <= due_at else 0
        else:
            if late is True or missing is True:
                on_time_flag = 0
            else:
                on_time_flag = None

        out.append((
            int(submission_id),  # raw.canvas_submissions.id
            int(gies_person_id) if gies_person_id is not None else None,
            int(course_key) if course_key is not None else None,
            int(canvas_user_id) if canvas_user_id is not None else None,
            int(canvas_course_id) if canvas_course_id is not None else None,
            int(assignment_id) if assignment_id is not None else None,
            submitted_at,
            graded_at,
            float(score) if score is not None else None,
            int(attempt) if attempt is not None else None,
            1 if late is True else 0 if late is False else None,
            1 if missing is True else 0 if missing is False else None,
            due_at,
            on_time_flag,
            workflow_state,
            raw_updated_at,
            utcnow(),
        ))

    return out, skip_user, skip_course


def merge_fact_submission(conn: pyodbc.Connection, rows: List[Tuple], batch_size: int = 2000) -> None:
    if not rows:
        return
    cur = conn.cursor()
    cur.execute("IF OBJECT_ID('tempdb..#stg_submission') IS NOT NULL DROP TABLE #stg_submission;")
    cur.execute("""
      CREATE TABLE #stg_submission (
        canvas_submission_id BIGINT NOT NULL,
        gies_person_id       BIGINT NULL,
        course_key           INT NULL,

        canvas_user_id       BIGINT NULL,
        canvas_course_id     BIGINT NULL,
        assignment_id        BIGINT NULL,

        submitted_at         DATETIME2 NULL,
        graded_at            DATETIME2 NULL,
        score                FLOAT NULL,
        attempt              INT NULL,

        late_flag            BIT NULL,
        missing_flag         BIT NULL,

        due_at               DATETIME2 NULL,
        on_time_flag         BIT NULL,

        workflow_state       NVARCHAR(50) NULL,
        raw_updated_at       DATETIME2 NULL,
        updated_at           DATETIME2 NULL
      );
    """)

    cur.fast_executemany = True
    ins = """
      INSERT INTO #stg_submission
      (canvas_submission_id,gies_person_id,course_key,canvas_user_id,canvas_course_id,assignment_id,
       submitted_at,graded_at,score,attempt,late_flag,missing_flag,due_at,on_time_flag,workflow_state,raw_updated_at,updated_at)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    for part in chunked(rows, batch_size):
        cur.executemany(ins, part)

    cur.execute("""
      MERGE cur.fact_submission WITH (HOLDLOCK) AS tgt
      USING #stg_submission AS src
        ON tgt.canvas_submission_id = src.canvas_submission_id
      WHEN MATCHED THEN UPDATE SET
        tgt.gies_person_id   = src.gies_person_id,
        tgt.course_key       = src.course_key,
        tgt.canvas_user_id   = src.canvas_user_id,
        tgt.canvas_course_id = src.canvas_course_id,
        tgt.assignment_id    = src.assignment_id,
        tgt.submitted_at     = src.submitted_at,
        tgt.graded_at        = src.graded_at,
        tgt.score            = src.score,
        tgt.attempt          = src.attempt,
        tgt.late_flag        = src.late_flag,
        tgt.missing_flag     = src.missing_flag,
        tgt.due_at           = src.due_at,
        tgt.on_time_flag     = src.on_time_flag,
        tgt.workflow_state   = src.workflow_state,
        tgt.raw_updated_at   = src.raw_updated_at,
        tgt.updated_at       = src.updated_at
      WHEN NOT MATCHED THEN INSERT
        (canvas_submission_id,gies_person_id,course_key,canvas_user_id,canvas_course_id,assignment_id,
         submitted_at,graded_at,score,attempt,late_flag,missing_flag,due_at,on_time_flag,workflow_state,raw_updated_at,updated_at)
      VALUES
        (src.canvas_submission_id,src.gies_person_id,src.course_key,src.canvas_user_id,src.canvas_course_id,src.assignment_id,
         src.submitted_at,src.graded_at,src.score,src.attempt,src.late_flag,src.missing_flag,src.due_at,src.on_time_flag,src.workflow_state,src.raw_updated_at,src.updated_at);
    """)
    conn.commit()


# -------------------------
# main
# -------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--conn", required=True, help="ODBC connection string")
    ap.add_argument("--batch-size", type=int, default=2000)
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    conn = pyodbc.connect(args.conn, autocommit=False)
    cur = conn.cursor()

    logging.info("Ensuring curated schema/tables...")
    cur.execute(DDL)
    conn.commit()

    logging.info("Reading cur.person_identity_map...")
    student_rows = read_person_identity_map(cur)
    logging.info("person_identity_map rows=%d", len(student_rows))

    logging.info("Upserting cur.dim_student...")
    merge_dim_student(conn, student_rows, batch_size=args.batch_size)

    logging.info("Loading student_key map (canvas_user_id -> gies_person_id)...")
    student_key_map = load_student_key_map(cur)
    logging.info("student_key_map size=%d", len(student_key_map))

    logging.info("Reading raw courses...")
    course_rows = read_raw_courses(cur)
    logging.info("parsed courses=%d", len(course_rows))

    logging.info("Upserting cur.dim_course...")
    merge_dim_course(conn, course_rows, batch_size=args.batch_size)

    logging.info("Loading course_key map (canvas_course_id -> course_key)...")
    course_key_map = load_course_key_map(cur)
    logging.info("course_key_map size=%d", len(course_key_map))

    logging.info("Reading raw submissions...")
    sub_rows, skip_user, skip_course = read_raw_submissions(cur, student_key_map, course_key_map)
    logging.info("parsed submissions=%d (no_student_key=%d, no_course_key=%d)", len(sub_rows), skip_user, skip_course)

    logging.info("Upserting cur.fact_submission...")
    merge_fact_submission(conn, sub_rows, batch_size=args.batch_size)

    logging.info("DONE")


if __name__ == "__main__":
    main()

