SELECT name AS schema_name
FROM sys.schemas
WHERE name IN ('raw','stg','cur','meta')
ORDER BY name;



