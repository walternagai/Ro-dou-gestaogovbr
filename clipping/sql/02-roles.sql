-- Roles do Rodou Clipping.
--
-- As roles são criadas sem LOGIN e sem senha: a senha deve ser definida fora
-- do repositório, por exemplo:
--   ALTER ROLE clipping_ro LOGIN PASSWORD '<senha-forte>';
--   ALTER ROLE clipping_rw LOGIN PASSWORD '<senha-forte>';
--
-- O Airflow usa `clipping_rw` (ou o próprio usuário do banco do Airflow);
-- a API usa `clipping_ro`.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'clipping_rw') THEN
        CREATE ROLE clipping_rw NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'clipping_ro') THEN
        CREATE ROLE clipping_ro NOLOGIN;
    END IF;
END
$$;

GRANT CONNECT ON DATABASE clipping TO clipping_rw, clipping_ro;
GRANT USAGE ON SCHEMA clipping TO clipping_rw, clipping_ro;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA clipping TO clipping_rw;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA clipping TO clipping_rw;

GRANT SELECT ON ALL TABLES IN SCHEMA clipping TO clipping_ro;
-- A API usa `clipping_ro` para leitura e grava apenas a auditoria.
GRANT INSERT ON clipping.api_request TO clipping_ro;

ALTER DEFAULT PRIVILEGES IN SCHEMA clipping
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO clipping_rw;
ALTER DEFAULT PRIVILEGES IN SCHEMA clipping
    GRANT USAGE, SELECT ON SEQUENCES TO clipping_rw;
ALTER DEFAULT PRIVILEGES IN SCHEMA clipping
    GRANT SELECT ON TABLES TO clipping_ro;
