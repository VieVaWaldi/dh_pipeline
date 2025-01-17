SELECT pg_terminate_backend(pid) 
FROM pg_stat_activity 
WHERE datname = 'test_digicher' AND pid <> pg_backend_pid();

drop database test_digicher;
create database test_digicher;