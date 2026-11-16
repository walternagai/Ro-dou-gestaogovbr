# Rodou Clipping API

Serviço HTTP somente leitura sobre o banco `clipping`, com disparo de execução
sob demanda.

- Endpoints: `https://rodou.ninja-apps.work/clipping/v1`
- Healthcheck: `/clipping/healthz`
- OpenAPI: `/clipping/openapi.json` · Swagger: `/clipping/docs`
- Autenticação: `X-API-Key` (ou `Authorization: Bearer`), escopos
  `clipping:read` e `clipping:trigger`.

## Variáveis de ambiente

| Variável | Obrigatória | Descrição |
|---|---|---|
| `CLIPPING_DB_URL` | sim | conexão com o banco `clipping` (usuário `clipping_ro`) |
| `AIRFLOW_API_URL` | não | base da API do Airflow (default `http://airflow-api-server:8080`) |
| `AIRFLOW_API_TOKEN` | para `POST /runs` | token com permissão de disparar DAGs |
| `CLIPPING_DAG_ID` | não | DAG disparada (default `rodou_clipping`) |
| `CLIPPING_READ_LIMIT_PER_MINUTE` | não | limite de leitura por chave (default 120) |
| `CLIPPING_TRIGGER_LIMIT_PER_HOUR` | não | limite de disparos por chave (default 10) |
| `CLIPPING_CACHE_MAX_AGE` | não | `Cache-Control` em segundos (default 60) |

## Execução local

```bash
docker build -t rodoui-clipping-api services/clipping_api
docker run --rm -p 8081:8000 \
  -e CLIPPING_DB_URL='postgresql://airflow:airflow@host.docker.internal:5432/clipping' \
  rodoui-clipping-api
curl http://localhost:8081/clipping/healthz
```

O serviço também está no `docker-compose.yml` sob o profile `clipping`.

## Publicação da imagem

```bash
docker build -t ghcr.io/gestaogovbr/ro-dou-clipping-api:<tag> services/clipping_api
docker push ghcr.io/gestaogovbr/ro-dou-clipping-api:<tag>
```

## Chaves de API

```bash
CLIPPING_DB_URL='postgresql://airflow:senha@postgres:5432/clipping' \
  python scripts/create_key.py --label "agente-ia" \
  --scope clipping:read --scope clipping:trigger
```

A chave em texto puro aparece uma única vez; o banco guarda apenas o `sha256`.

## Backfill e retenção

```bash
# backfill de até 31 dias (suprime notificações; idempotente)
python scripts/backfill.py --from 2026-10-01 --to 2026-10-31

# expurgo por retenção (dry-run primeiro)
python scripts/purge.py --months 24 --dry-run
python scripts/purge.py --months 24 --vacuum
```

As variáveis e o passo a passo estão em
`docs/docs/como_utilizar/backfill_retencao.md`.

## Testes

```bash
cd services/clipping_api && pytest -q tests
```

Os testes de API usam um banco falso em memória. Os testes de integração com
PostgreSQL só rodam com `CLIPPING_TEST_DSN` definido.
