# Checklist de produção — Rodou Clipping

Use este checklist antes de liberar a API para consumidores e a cada atualização
relevante. Marque apenas o que foi observado de fato.

## 1. Pré-requisitos

- [ ] Schema aplicado: `clipping.run`, `source_run`, `item`, `api_key` e
      `api_request` existem no banco `clipping`.
- [ ] Roles criadas e com senha definida fora do repositório:
      `clipping_rw` (Airflow) e `clipping_ro` (API, com `INSERT` apenas em
      `api_request`).
- [ ] `AIRFLOW_CONN_CLIPPING_DB` configurada (persistência das DAGs).
- [ ] `AIRFLOW_API_TOKEN` definido no `.env.production` e com permissão de
      disparar a DAG.
- [ ] `CLIPPING_DAG_ID` aponta para a DAG correta.
- [ ] `CLIPPING_DB_URL` definida (recomendado: `clipping_ro`) ou usando o
      padrão do Compose.
- [ ] Imagem da API publicada com tag imutável (não `latest`).
- [ ] Imagem do Airflow reconstruída a partir do commit atual (o override não
      monta `src` no contêiner).
- [ ] DAG `rodou_clipping` carregada e desativada da pausa (ou pausada de
      propósito durante indisponibilidade do DOU).
- [ ] `AIRFLOW__API_AUTH__JWT_EXPIRATION_TIME` definido e token de serviço
      rotacionado.

## 2. Subida e roteamento

- [ ] `docker compose ... config --quiet` válido com os arquivos de produção.
- [ ] Serviço sobe com `--profile clipping up -d clipping-api`.
- [ ] `GET https://<host>/clipping/healthz` retorna 200.
- [ ] `GET https://<host>/clipping/openapi.json` retorna 200.
- [ ] Airflow continua acessível no mesmo host (`/login`, `/api/v2/monitor/health`).
- [ ] Prioridade do router `/clipping` é maior que a do router do Airflow.
- [ ] Nenhuma porta da API publicada diretamente no host de produção.

## 3. Autenticação e limites

- [ ] `GET /clipping/v1/items` sem chave retorna 401.
- [ ] Chave inválida/inativa retorna 403.
- [ ] Chave só com `clipping:read` recebe 403 em `POST /runs`.
- [ ] Chave com `clipping:trigger` recebe 202 em `POST /runs`.
- [ ] Limite de leitura e de disparo conferidos (`X-RateLimit-*`).
- [ ] `429` traz `Retry-After`.
- [ ] Auditoria grava em `clipping.api_request` (sem payload).

## 4. Funcional

- [ ] `GET /clipping/v1/items?date=...` retorna itens e `sources_requested`.
- [ ] `sources_failed` reflete as fontes que falharam na execução.
- [ ] `ETag`/`If-None-Match` devolve 304 sem corpo.
- [ ] `GET /clipping/v1/items/{id}` retorna 200 e 404 quando não existe.
- [ ] `GET /clipping/v1/runs?date=...` e `/runs/{id}` retornam os status.
- [ ] `POST /runs` com `Idempotency-Key` não duplica execução.
- [ ] Execução concorrente retorna 409.
- [ ] Sem token configurado, `POST /runs` retorna 503.
- [ ] Reprocessar a mesma data não duplica itens.
- [ ] `POST /runs` cria a run como `queued` e ela aparece em
      `GET /runs?date=...` antes do término.
- [ ] Com a DAG pausada, o disparo retorna 202 mas a run permanece `queued`;
      a operação sabe que precisa desativar a pausa.

### MCP (se habilitado)

- [ ] `GET /clipping/mcp/healthz` retorna 200.
- [ ] `initialize` responde com `protocolVersion` e `serverInfo`.
- [ ] `tools/list` lista as 6 ferramentas.
- [ ] `tools/call` repassa `X-API-Key`/`Bearer` para a API.
- [ ] `list_items` limita a 50 itens e trunca `abstract` em 1.000 caracteres.
- [ ] Respostas textuais trazem o aviso de conteúdo não confiável.
- [ ] `trigger_run` sem escopo `clipping:trigger` retorna erro de permissão.
- [ ] `trigger_run` só executa se a DAG estiver desativada da pausa.
- [ ] Transporte `stdio` funciona com `CLIPPING_API_KEY` local.

## 5. Observabilidade

- [ ] `GET /clipping/metrics` expõe `clipping_api_requests_total`,
      `clipping_api_throttled_total` e `clipping_api_source_status`.
- [ ] Logs da API em JSON, sem payload/query string.
- [ ] Alerta para `clipping_api_source_status{status="failed"} == 1` por 2 dias.
- [ ] Alerta para taxa de 5xx e para 429 acima do esperado.
- [ ] Alerta de falha da DAG (`email_admin`) funcionando.

## 6. Backup e rollback

- [ ] Backup do banco `clipping` (`pg_dump`) executado e testado.
- [ ] Backup do banco do Airflow e de `mnt/airflow-logs` documentado.
- [ ] Rollback: parar o serviço da API (`--profile clipping stop clipping-api`)
      mantém a coleta e as notificações funcionando.
- [ ] Migrações do banco são idempotentes e testadas em cópia.

## 7. Critérios de aceite do PRD (§16)

| # | Critério | Evidência |
|---|---|---|
| 1 | QD fora do ar não impede o DOU | execução `partial` com `sources_failed` |
| 2 | Idempotência no reprocessamento | contagem de itens estável |
| 3 | DOE-MG coleta Executivo/Municípios | itens com `uf=MG` |
| 4 | Falha de TLS do MG é explícita | `source_run.status=failed` |
| 5 | Dia sem publicação do MG é `empty` | run sem erro |
| 6 | `DOESP` e `DOE:SP` equivalentes | teste de paridade |
| 7 | `sources` inválido falha claro | mensagem com valores aceitos |
| 8 | API responde 401/400 e declara `sources_failed` | smoke test |
| 9 | Trigger 202 e conflito 409 | smoke test |
| 10 | Persistência sobrevive a falha de SMTP | execução com SMTP desligado |
| 11 | Suíte permanece verde | `make tests` |
| 12 | OpenAPI com `operationId` estáveis | `/clipping/openapi.json` |
| 13 | 304 com `If-None-Match` | smoke test |
| 14 | Escopo de trigger aplicado | 403/202 |
| 15 | Erro acionável com `allowed` | 400 de fonte inválida |
| 16 | Rotas do Airflow intactas | `/login` e `/api/v2/monitor/health` |
