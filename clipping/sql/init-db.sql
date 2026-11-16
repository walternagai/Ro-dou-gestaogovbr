-- Inicialização do banco e do schema do Rodou Clipping (fase F1).
--
-- Aplicar com psql, a partir da raiz do repositório:
--   docker exec -i ro-dou-postgres-1 psql -U airflow -d postgres \
--       -f /clipping-sql/init-db.sql
--
-- O diretório `clipping/sql` é montado em `/clipping-sql` no serviço
-- `postgres` do docker-compose.yml.
--
-- O script é idempotente: criar o banco, o schema, as tabelas e as roles
-- pode ser repetido sem erro.

\ir 00-create-database.sql
\connect clipping
\ir 01-schema.sql
\ir 02-roles.sql
