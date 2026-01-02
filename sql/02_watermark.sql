IF OBJECT_ID('meta.watermark','U') IS NULL
BEGIN
  CREATE TABLE meta.watermark (
    source_name NVARCHAR(200) NOT NULL PRIMARY KEY,
    last_updated_at DATETIME2 NULL,
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
  );
END

MERGE meta.watermark AS t
USING (VALUES
 ('canvas_users'),
 ('canvas_courses'),
 ('canvas_enrollments'),
 ('canvas_submissions')
) AS s(source_name)
ON t.source_name = s.source_name
WHEN NOT MATCHED THEN
  INSERT (source_name, last_updated_at) VALUES (s.source_name, NULL);

SELECT * FROM meta.watermark;
