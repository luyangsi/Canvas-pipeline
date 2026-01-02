
#!/usr/bin/env python3
# file: /Users/luyangsi/Desktop/Canvas-pipeline/src/load/load_raw.py
"""
简易 JSONL -> SQL Server raw 层上报脚本（按主键 upsert: 先 UPDATE 再 IF @@ROWCOUNT=0 INSERT）

新增：
- --source-name（例如 canvas_courses）
- --incremental（flag）
- --updated-field（默认 updated_at；表示“从 JSON 里提取 updated_at 的字段名/路径”，支持点号路径如 data.updated_at）

增量逻辑：
如果 --incremental：
1) 先查 meta.watermark 取 last_updated_at
2) 读 JSONL 每行，解析 updated_at（从 JSON 中提取；没有就设为 NULL）
3) 若 updated_at 不为空 且 updated_at <= last_watermark：跳过（不写库）
4) 只对通过筛选的记录做 upsert
5) 成功后：把本次写入记录中的 max(updated_at) 更新到 meta.watermark

表结构示例（仅参考）:
    CREATE TABLE meta.watermark (
        source_name     NVARCHAR(255) NOT NULL PRIMARY KEY,
        last_updated_at DATETIME2 NULL,
        updated_at      DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );

    CREATE TABLE dbo.raw_payloads (
        id NVARCHAR(255) PRIMARY KEY,
        raw_payload NVARCHAR(MAX),
        updated_at DATETIME2 NULL
    );
"""

import argparse
import datetime
import json
import sys
from typing import Any, Optional

import pyodbc


def extract_by_path(obj: Any, path: str) -> Any:
    """
    从 dict 中按点号路径取值：
      - path="updated_at" 或 "data.updated_at"
    找不到返回 None
    """
    if obj is None or path is None:
        return None
    if not isinstance(obj, dict):
        return None
    cur: Any = obj
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


def parse_iso_datetime(val: Any) -> Optional[datetime.datetime]:
    """
    解析常见 ISO8601 时间字符串；支持末尾 Z。
    返回 naive datetime（去掉 tzinfo，统一用 UTC 表示）
    解析失败返回 None
    """
    if val is None:
        return None
    if isinstance(val, datetime.datetime):
        # 统一转 naive
        return val.replace(tzinfo=None)
    if not isinstance(val, str):
        return None

    s = val.strip()
    if not s:
        return None

    try:
        # 处理 Z
        if s.endswith("Z"):
            dt = datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.datetime.fromisoformat(s)
        # 去 tzinfo，按 UTC naive 存
        if dt.tzinfo is not None:
            dt = dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        return None


def get_last_watermark(cursor: Any, source_name: str) -> Optional[datetime.datetime]:
    cursor.execute(
        """
        SELECT last_updated_at
        FROM meta.watermark
        WHERE source_name = ?
        """,
        source_name,
    )
    row = cursor.fetchone()
    if not row:
        return None
    return row[0]  # datetime2 -> python datetime or None


def upsert_watermark(cursor: Any, source_name: str, last_updated_at: datetime.datetime) -> None:
    """
    更新/插入 meta.watermark
    """
    sql = """
UPDATE meta.watermark
SET last_updated_at = ?,
    updated_at = SYSUTCDATETIME()
WHERE source_name = ?;

IF @@ROWCOUNT = 0
BEGIN
    INSERT INTO meta.watermark (source_name, last_updated_at, updated_at)
    VALUES (?, ?, SYSUTCDATETIME());
END
"""
    cursor.execute(sql, last_updated_at, source_name, source_name, last_updated_at)


def upsert_record(
    cursor: Any,
    table: str,
    id_val: str,
    payload: str,
    updated_at: Optional[datetime.datetime],
) -> None:
    """
    使用 UPDATE ... IF @@ROWCOUNT=0 INSERT 的方式做 upsert（防止重复写入）
    假定主键列名为 id，payload 列名为 raw_payload，时间列名为 updated_at（允许 NULL）。
    """
    sql = f"""
DECLARE @id NVARCHAR(255) = ?;
DECLARE @payload NVARCHAR(MAX) = ?;
DECLARE @updated_at DATETIME2 = ?;

UPDATE {table}
SET raw_payload = @payload,
    updated_at = @updated_at
WHERE id = @id;

IF @@ROWCOUNT = 0
BEGIN
    INSERT INTO {table} (id, raw_payload, updated_at)
    VALUES (@id, @payload, @updated_at);
END
"""
    cursor.execute(sql, id_val, payload, updated_at)


def load_jsonl_to_db(
    file_path: str,
    conn_str: str,
    table: str,
    id_field: str = "id",
    source_name: Optional[str] = None,
    incremental: bool = False,
    updated_field: str = "updated_at",
) -> None:
    conn = pyodbc.connect(conn_str, autocommit=True)
    cursor = conn.cursor()

    last_watermark: Optional[datetime.datetime] = None
    if incremental:
        if not source_name:
            raise ValueError("--source-name is required when --incremental is set")
        last_watermark = get_last_watermark(cursor, source_name)

    total = 0          # upsert 成功条数
    skipped = 0        # 增量跳过条数
    failed = 0

    max_written_updated_at: Optional[datetime.datetime] = None

    with open(file_path, "r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)

                if id_field not in obj or obj[id_field] is None:
                    raise ValueError(f"missing id field '{id_field}' in line {line_no}")
                id_val = str(obj[id_field])

                raw_payload = json.dumps(obj, ensure_ascii=False)

                # 从 JSON 按路径提取 updated_at；没有就 NULL
                src_updated = extract_by_path(obj, updated_field)
                updated_at = parse_iso_datetime(src_updated)  # None if missing/unparseable

                # 增量过滤：updated_at 不为空 且 <= watermark => 跳过
                if incremental and last_watermark is not None:
                    if updated_at is not None and updated_at <= last_watermark:
                        skipped += 1
                        continue

                upsert_record(cursor, table, id_val, raw_payload, updated_at)
                total += 1

                if updated_at is not None:
                    if max_written_updated_at is None or updated_at > max_written_updated_at:
                        max_written_updated_at = updated_at

            except Exception as e:
                failed += 1
                print(f"[WARN] line {line_no} upsert failed: {e}", file=sys.stderr)

    # 成功后更新 watermark
    if incremental and source_name and max_written_updated_at is not None:
        upsert_watermark(cursor, source_name, max_written_updated_at)

    cursor.close()
    conn.close()

    msg = f"Finished. success={total}, skipped={skipped}, failed={failed}"
    if incremental:
        msg += f", last_watermark={last_watermark}, new_watermark={max_written_updated_at}"
    print(msg)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load JSONL into SQL Server raw layer (upsert by id).")
    parser.add_argument("--file", required=True, help="输入 JSONL 文件路径")
    parser.add_argument(
        "--conn",
        required=True,
        help="ODBC 连接字符串，例如: DRIVER={ODBC Driver 18 for SQL Server};SERVER=...;DATABASE=...;UID=...;PWD=...",
    )
    parser.add_argument("--table", required=True, help="目标表，带 schema，例如 raw.canvas_courses")
    parser.add_argument("--id-field", default="id", help="JSON 中作为主键的字段名，默认 'id'")

    # 新增参数
    parser.add_argument("--source-name", help="增量水位表中的 source_name，例如 canvas_courses")
    parser.add_argument("--incremental", action="store_true", help="启用增量加载（基于 meta.watermark）")
    parser.add_argument(
        "--updated-field",
        default="updated_at",
        help="从 JSON 中提取 updated_at 的字段名/路径（点号路径），默认 'updated_at'",
    )

    args = parser.parse_args()

    load_jsonl_to_db(
        file_path=args.file,
        conn_str=args.conn,
        table=args.table,
        id_field=args.id_field,
        source_name=args.source_name,
        incremental=args.incremental,
        updated_field=args.updated_field,
    )


if __name__ == "__main__":
    main()
