-----------------------------------------------
-- CORE ENTITIES

-- MIA
-- * Looks like i am parsing author names wrong, i am seperating where no separation is needed

select count(*) from coreac.work;
-- 196_614
select count(*) from coreac.link;
-- 800_610
select count(*) from coreac.reference;
-- 368_309
select count(*) from coreac.data_provider;
-- 0

-----------------------------------------------
-- JUNCTIONS

select count(*) from coreac.j_work_link;
-- 800_738
select count(*) from coreac.j_work_reference;
-- 382_183
select count(*) from coreac.j_work_data_provider;
-- 0

-----------------------------------------------
-- SIZE

SELECT 'work' as table_name, 
       pg_size_pretty(pg_total_relation_size('coreac.work')) as size
UNION ALL
SELECT 'link' as table_name, 
       pg_size_pretty(pg_total_relation_size('coreac.link')) as size
UNION ALL
SELECT 'reference' as table_name, 
       pg_size_pretty(pg_total_relation_size('coreac.reference')) as size
UNION ALL
SELECT 'data_provider' as table_name, 
       pg_size_pretty(pg_total_relation_size('coreac.data_provider')) as size
UNION ALL
SELECT 'j_work_link' as table_name, 
       pg_size_pretty(pg_total_relation_size('coreac.j_work_link')) as size
UNION ALL
SELECT 'j_work_reference' as table_name, 
       pg_size_pretty(pg_total_relation_size('coreac.j_work_reference')) as size
UNION ALL
SELECT 'j_work_data_provider' as table_name, 
       pg_size_pretty(pg_total_relation_size('coreac.j_work_data_provider')) as size
UNION ALL
-- Get total size of all tables
SELECT 'Total (all tables)' as table_name,
       pg_size_pretty(sum(pg_total_relation_size(c.oid))) as size
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'coreac' AND c.relkind = 'r'
UNION ALL
-- Calculate work table without fulltext
SELECT 'work (without fulltext)' as table_name,
       pg_size_pretty(
         pg_total_relation_size('coreac.work') - 
         (SELECT sum(pg_column_size(fulltext))::bigint FROM coreac.work WHERE fulltext IS NOT NULL)
       ) as size
ORDER BY size DESC;