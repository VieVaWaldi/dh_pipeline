SELECT pg_terminate_backend(pid) 
FROM pg_stat_activity 
WHERE datname = 'db_digicher' AND pid <> pg_backend_pid();

drop database db_digicher;
create database db_digicher;