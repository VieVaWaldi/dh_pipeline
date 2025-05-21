----------------------------------------
-- Get size of all tables in DB

SELECT
    table_schema || '.' || table_name AS table_full_name,
    pg_size_pretty(pg_total_relation_size('"' || table_schema || '"."' || table_name || '"')) AS total_size,
    pg_total_relation_size('"' || table_schema || '"."' || table_name || '"') / 1024.0 / 1024.0 / 1024.0 AS size_in_gb
FROM information_schema.tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY pg_total_relation_size('"' || table_schema || '"."' || table_name || '"') DESC;

SELECT
    SUM(pg_total_relation_size('"' || table_schema || '"."' || table_name || '"') / 1024.0 / 1024.0 / 1024.0) as total_sum
FROM information_schema.tables 
WHERE table_schema NOT IN ('pg_catalog', 'information_schema');