-- Cria o banco `clipping` quando ainda não existe.
--
-- Requer privilégio de criação de banco (o usuário do PostgreSQL do Airflow
-- é superusuário neste ambiente). Aplicar conectado ao banco padrão:
--   psql -U airflow -d postgres -f /clipping-sql/00-create-database.sql

SELECT 'CREATE DATABASE clipping'
WHERE NOT EXISTS (
    SELECT 1 FROM pg_database WHERE datname = 'clipping'
)\gexec
