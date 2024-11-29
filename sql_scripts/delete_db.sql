drop database test_digicher;

SELECT pg_terminate_backend(pid) 
FROM pg_stat_activity 
WHERE datname = 'test_digicher' AND pid <> pg_backend_pid();

create database test_digicher;