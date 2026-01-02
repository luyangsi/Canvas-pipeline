
import argparse
import json
import datetime
import pyodbc
import sys
from typing import Any

#!/usr/bin/env python3
# file: /Users/luyangsi/Desktop/Canvas-pipeline/src/load/load_raw.py
"""
简易 JSONL -> SQL Server raw 层上报脚本（按主键 upsert: 先 UPDATE 再 IF @@ROWCOUNT=0 INSERT）
用法示例：
    python load_raw.py --file data.jsonl --conn "DRIVER={ODBC Driver 18 for SQL Server};SERVER=...;DATABASE=...;UID=...;PWD=..." --table dbo.raw_payloads
表结构示例（仅参考）:
    CREATE TABLE dbo.raw_payloads (
        id NVARCHAR(255) PRIMARY KEY,
        raw_payload NVARCHAR(MAX),
        updated_at DATETIME2
    );
"""


def upsert_record(cursor: Any, table: str, id_val: str, payload: str, updated_at: datetime.datetime):
        """
        使用 UPDATE ... IF @@ROWCOUNT=0 INSERT 的方式做 upsert（防止重复写入）
        假定主键列名为 id，payload 列名为 raw_payload，时间列名为 updated_at。
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

def load_jsonl_to_db(file_path: str, conn_str: str, table: str, id_field: str = "id"):
        conn = pyodbc.connect(conn_str, autocommit=True)
        cursor = conn.cursor()
        total = 0
        failed = 0
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
                                raw_payload = json.dumps(obj, ensure_ascii=False)  # 保存原始记录 JSON 字符串
                                src = obj.get("updated_at")
                                updated_at = datetime.datetime.fromisoformat(src.replace("Z", "+00:00")).replace(tzinfo=None) if src else datetime.datetime.utcnow()
                                upsert_record(cursor, table, id_val, raw_payload, updated_at)
                                total += 1
                        except Exception as e:
                                # 记录失败但继续处理后续行
                                failed += 1
                                print(f"[WARN] line {line_no} upsert failed: {e}", file=sys.stderr)
        cursor.close()
        conn.close()
        print(f"Finished. success={total}, failed={failed}")

def main():
        parser = argparse.ArgumentParser(description="Load JSONL into SQL Server raw layer (upsert by id).")
        parser.add_argument("--file", required=True, help="输入 JSONL 文件路径")
        parser.add_argument("--conn", required=True, help="ODBC 连接字符串，例如: DRIVER={ODBC Driver 18 for SQL Server};SERVER=...;DATABASE=...;UID=...;PWD=...")
        parser.add_argument("--table", required=True, help="目标表，带 schema，例如 dbo.raw_payloads")
        parser.add_argument("--id-field", default="id", help="JSON 中作为主键的字段名，默认 'id'")
        args = parser.parse_args()
        load_jsonl_to_db(args.file, args.conn, args.table, args.id_field)

if __name__ == "__main__":
        main()