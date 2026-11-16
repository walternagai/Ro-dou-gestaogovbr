-- Schema do Rodou Clipping (fase F1).
--
-- Aplicar conectado ao banco `clipping`:
--   psql -U airflow -d clipping -f /clipping-sql/01-schema.sql
--
-- O script é idempotente (IF NOT EXISTS) e pode ser reaplicado.

CREATE SCHEMA IF NOT EXISTS clipping;

CREATE TABLE IF NOT EXISTS clipping.run (
    id                text PRIMARY KEY,
    dag_id            text NOT NULL,
    dag_run_id        text,
    reference_date    date NOT NULL,
    status            text NOT NULL CHECK (
                          status IN ('queued', 'running', 'success', 'partial', 'failed')
                      ),
    sources_requested text[] NOT NULL DEFAULT '{}',
    sources_ok        text[] NOT NULL DEFAULT '{}',
    sources_failed    jsonb NOT NULL DEFAULT '[]'::jsonb,
    items_count       integer NOT NULL DEFAULT 0,
    started_at        timestamptz NOT NULL DEFAULT now(),
    finished_at       timestamptz
);

CREATE TABLE IF NOT EXISTS clipping.source_run (
    run_id       text NOT NULL REFERENCES clipping.run (id) ON DELETE CASCADE,
    source       text NOT NULL,
    status       text NOT NULL CHECK (status IN ('ok', 'empty', 'failed')),
    items_count  integer NOT NULL DEFAULT 0,
    error        text,
    duration_ms  integer,
    PRIMARY KEY (run_id, source)
);

CREATE TABLE IF NOT EXISTS clipping.item (
    id             text PRIMARY KEY,
    run_id         text NOT NULL REFERENCES clipping.run (id),
    source         text NOT NULL,
    uf             char(2),
    gazette        text,
    external_id    text,
    published_date date NOT NULL,
    section        text,
    org            text,
    title          text,
    abstract       text,
    url            text NOT NULL,
    matched_terms  text[] NOT NULL DEFAULT '{}',
    content_hash   text,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS item_date_source_idx
    ON clipping.item (published_date, source);

CREATE INDEX IF NOT EXISTS item_uf_date_idx
    ON clipping.item (uf, published_date);

CREATE INDEX IF NOT EXISTS item_org_idx
    ON clipping.item (lower(org));

CREATE INDEX IF NOT EXISTS item_terms_gin
    ON clipping.item USING gin (matched_terms);

CREATE INDEX IF NOT EXISTS item_search_fts
    ON clipping.item USING gin (
        to_tsvector(
            'portuguese',
            coalesce(title, '') || ' ' || coalesce(abstract, '')
        )
    );

-- Chaves de API (usadas a partir da fase F4).
CREATE TABLE IF NOT EXISTS clipping.api_key (
    id         text PRIMARY KEY,
    key_hash   text NOT NULL UNIQUE,
    label      text NOT NULL,
    scope      text[] NOT NULL DEFAULT '{clipping:read}',
    active     boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    revoked_at timestamptz
);

-- Auditoria de requisições da API (sem payload).
CREATE TABLE IF NOT EXISTS clipping.api_request (
    id         bigserial PRIMARY KEY,
    key_id     text,
    route      text NOT NULL,
    status     integer NOT NULL,
    latency_ms integer,
    request_id text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS api_request_created_idx
    ON clipping.api_request (created_at);
