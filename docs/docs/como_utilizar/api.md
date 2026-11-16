# API do Rodou Clipping

A API expõe o clipping persistido e permite disparar uma execução sob demanda.
Ela é destinada a integrações máquina-a-máquina — inclusive agentes de IA — e
não substitui as notificações por e-mail/Slack/Discord.

- Base: `https://rodou.ninja-apps.work/clipping/v1`
- Healthcheck: `https://rodou.ninja-apps.work/clipping/healthz`
- OpenAPI: `https://rodou.ninja-apps.work/clipping/openapi.json`
- Swagger: `https://rodou.ninja-apps.work/clipping/docs`

## Autenticação

Envie a chave no header `X-API-Key` (ou `Authorization: Bearer <chave>`).

| Escopo | Permite |
|---|---|
| `clipping:read` | consultar itens, execuções e fontes |
| `clipping:trigger` | `POST /runs` |

Cada consumidor deve ter a **própria chave**. A chave é exibida uma única vez na
criação; o banco guarda apenas o `sha256`:

```bash
CLIPPING_DB_URL='postgresql://clipping_rw:senha@postgres:5432/clipping' \
  python services/clipping_api/scripts/create_key.py \
  --label "agente-ia" --scope clipping:read --scope clipping:trigger
```

Limites: 120 requisições por minuto para leitura e 10 disparos por hora, por
chave. As respostas trazem `X-RateLimit-Limit`, `X-RateLimit-Remaining` e
`X-RateLimit-Reset`; em `429`, use `Retry-After`.

## Consultar itens

```http
GET /clipping/v1/items?date=2026-11-16&source=DOU,DOE:MG&q=licitação&page=1&size=50
```

| Parâmetro | Descrição |
|---|---|
| `date` | dia de publicação (`AAAA-MM-DD`) |
| `from` / `to` | intervalo (máximo de 31 dias) |
| `source` | fontes separadas por vírgula (`DOU`, `QD`, `INLABS`, `DOE:SP`, `DOE:MG`) |
| `uf` | sigla de duas letras |
| `q` | busca textual em título e resumo |
| `since` | delta por atualização, em ISO-8601 |
| `page` / `size` | paginação (`size` até 200; default 50) |

```json
{
  "reference": { "from": "2026-11-16", "to": "2026-11-16" },
  "sources_requested": ["DOU", "DOE:MG"],
  "sources_failed": [],
  "total": 128,
  "page": 1,
  "size": 50,
  "next_page": 2,
  "items": [
    {
      "id": "9f2c1a7e5b...",
      "source": "DOE:MG",
      "uf": "MG",
      "gazette": null,
      "published_date": "2026-11-16",
      "section": "DOE-MG - Diário do Executivo",
      "org": null,
      "title": "Diário do Executivo - página 5",
      "abstract": "…trecho com o termo pesquisado…",
      "url": "https://www.jornalminasgerais.mg.gov.br/edicao-do-dia?dados=…",
      "matched_terms": ["licitação"],
      "updated_at": "2026-11-16T05:12:41-03:00"
    }
  ]
}
```

`GET /clipping/v1/items/{id}` devolve um item ou `404`.

A resposta traz `ETag`. Reenvie-o em `If-None-Match` para receber `304` sem
corpo quando nada mudou; o `Cache-Control` é curto (`private, max-age=60`).

## Execuções

```http
GET /clipping/v1/runs?date=2026-11-16
GET /clipping/v1/runs/{run_id}
```

`run_id` tem o formato `<dag_id>:<airflow_run_id>`. O status pode ser
`queued`, `running`, `success`, `partial` ou `failed`; `sources_failed` lista as
fontes que não responderam.

### Disparar uma execução

```http
POST /clipping/v1/runs
X-API-Key: <chave com clipping:trigger>
Idempotency-Key: clipping:2026-11-15

{ "reference_date": "2026-11-15", "sources": ["DOU", "DOE:MG"] }
```

Resposta `202`:

```json
{ "run_id": "rodou_clipping:manual__2026-11-15T…", "dag_run_id": "manual__2026-11-15T…", "status": "queued" }
```

- `Idempotency-Key` evita disparos duplicados: a mesma chave devolve o mesmo
  `run_id`.
- `409` indica que já existe execução em andamento para a data.
- `503` indica que o disparo não está configurado ou o Airflow está indisponível.

## Fontes

```http
GET /clipping/v1/sources
```

Lista fontes, UF, cadernos, limite de requisições por segundo e o último status
conhecido de cada fonte.

## Erros

Erros usam `application/problem+json`:

```json
{
  "type": "about:blank",
  "title": "Fonte inválida",
  "status": 400,
  "detail": "Fonte(s) inválida(s): XPTO. Valores aceitos: DOU, QD, INLABS, DOE:SP, DOE:MG.",
  "allowed": ["DOU", "QD", "INLABS", "DOE:SP", "DOE:MG"]
}
```

| Código | Situação |
|---|---|
| 304 | `If-None-Match` sem alteração |
| 400 | parâmetro inválido |
| 401 | chave ausente |
| 403 | chave inválida/inativa ou escopo insuficiente |
| 404 | item ou execução inexistente |
| 409 | execução concorrente |
| 429 | limite de requisições |
| 503 | banco ou Airflow indisponível |

## Operação

A API é opcional e sobe sob o profile `clipping` do Compose:

```bash
docker compose --env-file deploy/prod/.env.production \
  -f docker-compose.yml -f deploy/prod/docker-compose.override.yml \
  -f deploy/prod/docker-compose.traefik-host.yml \
  --profile clipping up -d clipping-api
```

Ela depende do schema criado por `clipping/sql/init-db.sql` e das variáveis
`CLIPPING_DB_URL`, `AIRFLOW_API_TOKEN` e `CLIPPING_DAG_ID` (ver
`deploy/prod/README.md`).
