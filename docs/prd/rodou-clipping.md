# PRD — Rodou Clipping

**Persistência de resultados e adaptadores de DOE estaduais (SP e MG)**

| Campo | Valor |
|---|---|
| Versão | 1.2 (draft) |
| Data | 16/11/2026 |
| Produto | Rodou Clipping |
| Endpoint público | `https://rodou.ninja-apps.work/clipping/v1` (ver Decisão D1) |
| Base de implantação | `rodou.ninja-apps.work` (ambiente atual, mesmo host do Airflow) |
| Consumidor principal | Agente de IA em desenvolvimento por outro time (máquina-a-máquina) |
| Responsável técnico | A definir |
| Stakeholders | Operação de clipping, TI (TresIA), dev do agente de IA, times consumidores da API |
| Status | Para revisão |

---

## 1. Resumo executivo

O Ro-DOU hoje coleta e notifica, mas **não persiste** o resultado do clipping: os
itens vivem apenas em XCom entre tasks e desaparecem após a execução. Além disso,
as fontes são fixas no código (DOU, INLABS, QD, DOE-SP) e uma falha de fonte
derruba a execução inteira.

Este PRD define o **Rodou Clipping**: uma camada de persistência consultável e um
conjunto de **adaptadores de DOE estaduais** com contrato comum, começando por
**São Paulo (SP)** e **Minas Gerais (MG)**. O resultado é exposto em
`https://rodou.ninja-apps.work/clipping/v1`, uma API HTTP de leitura com disparo
de coleta sob demanda, desenhada para consumo máquina-a-máquina pelo **agente de
IA de outro time** e sem depender de resumos por IA nesta fase.

**Resultado esperado:** todo clipping diário fica armazenado, idempotente e
consultável por data, fonte, UF e termo; falhas de fonte são isoladas e
declaradas explicitamente, em vez de zerar a execução; o agente consome um
contrato estável, cacheável e com erros acionáveis.

---

## 2. Contexto e problema

### 2.1 Estado atual (evidências de 16/11/2026)

| Observação | Evidência |
|---|---|
| Resultado não persiste | Fluxo `exec_search → XCom → notificação`; nenhuma escrita em banco (`grep` por `to_sql`/`INSERT` em `src/` só encontra a DAG do INLABS) |
| Fontes fixas | `src/dou_dag_generator.py:147` — dicionário com `DOU`, `QD`, `INLABS`, `DOESP` |
| Mistura limitada | `src/dou_dag_generator.py:344-414` — `if/elif` para DOU/INLABS e branches de merge por par |
| `sources` não validado | `src/schemas.py:148` — `Optional[list[str]]`, sem `Literal`/validator |
| Falha de fonte derruba tudo | `QDSearcher._get_with_retry` propaga após 3 tentativas; a task falha e o resultado do DOU é perdido |
| QD indisponível | `GET https://api.queridodiario.org.br/gazettes` → **503 "no available server"** (global) |
| INLABS indisponível | `inlabs.in.gov.br` em **manutenção programada**; `logar.php`/`index.php` → 404 |
| DOE-SP funcional | `do-api-web-search.doe.sp.gov.br/v2/advanced-search/publications` |
| DOE-MG funcional com ressalvas | API `jornalminasgerais.mg.gov.br/api/v1/` (token anônimo); cadeia TLS incompleta (ver §11.3) |

### 2.2 Dores do usuário

1. **Perda de histórico**: não há como consultar o clipping de ontem, da semana
   passada ou de um período para auditoria.
2. **Integração frágil**: consumidores dependem de e-mail/Slack; não há contrato
   programático estável.
3. **Cobertura limitada**: só há adaptador estadual para SP; MG, que tem API
   pública, não é suportada.
4. **Falha em cascata**: uma fonte fora do ar invalida o clipping inteiro,
   inclusive o que funcionou.
5. **Reprocessamento sem controle**: sem chave idempotente, retries podem
   duplicar itens ou notificações.

---

## 3. Objetivos e métricas de sucesso

### 3.1 Objetivos

| ID | Objetivo |
|---|---|
| O1 | Persistir todo clipping coletado, com idempotência e rastreabilidade por execução |
| O2 | Isolar falhas por fonte, expondo status parcial em vez de falha total |
| O3 | Criar contrato comum de adaptadores estaduais e implementar SP e MG |
| O4 | Expor os resultados por API HTTP autenticada, sem resumos por IA |
| O5 | Manter compatibilidade de YAML, DAGs e notificações existentes |
| O6 | Entregar, na fase 2, servidor MCP para o agente de IA sobre a mesma API |

### 3.2 Métricas

| Métrica | Meta | Medição |
|---|---|---|
| Itens persistidos por execução | 100% dos itens entregues na notificação | contagem `clipping.run.items` vs. payload notificado |
| Duplicidade em reprocessamento | 0 itens duplicados | teste de idempotência + constraint `UNIQUE` |
| Execução diária concluída | até 06:30 (America/Sao_Paulo) | `clipping.run.finished_at` |
| Ciclo diário do agente | concluído em 1 rodada, sem retry manual | logs de `key_id` + runs do dia |
| Disponibilidade da API | ≥ 99,5% mensal (exclui indisponibilidade de fonte) | healthcheck + logs |
| Latência de leitura | p95 ≤ 300 ms para 50 itens | métricas do serviço |
| Falha isolada | execução `partial`, nunca `failed` por fonte única | status da run |
| Cobertura de testes das novas unidades | ≥ 80% nos módulos novos | `pytest --cov` no CI |

---

## 4. Não objetivos

- **Resumos por IA** (`use_ai_summary`, `use_ai_executive_summary`,
  `ai_search_config`): permanecem como estão; não são pré-requisito desta fase.
- **Novas UFs além de SP e MG**: o contrato nasce extensível, mas a entrega é
  limitada às duas.
- **Interface web própria** do Rodou Clipping (frontend): fora de escopo; a
  entrega é API + dados.
- **Substituir notificações** por e-mail/Slack/Discord: continuam funcionando em
  paralelo.
- **Reescrever o INLABS** ou contornar sua manutenção.
- **Autenticação OIDC/gov.br**: nesta fase, chave de API (ver §12.3).
- **Servidor MCP**: não faz parte da v1; é a **fase 2 (F7)**, especificada em
  §10.8, sobre os mesmos endpoints e a mesma base de dados.

---

## 5. Personas e casos de uso

| Persona | Necessidade | Caso de uso |
|---|---|---|
| Operador de clipping | Conferir o que saiu hoje e ontem | `GET /clipping/v1/items?date=...` |
| Analista de dados | Baixar recorte por UF/termo | `GET /clipping/v1/items?uf=MG&q=licitação&from=...&to=...` |
| Integrador de sistema | Receber clipping de forma programática | API key + polling diário |
| **Agente de IA (outro time)** | Consultar e disparar o clipping de forma autônoma, **uma vez por dia**, com contrato previsível | `GET /clipping/v1/items` + `GET /clipping/v1/runs/{id}`; `POST /clipping/v1/runs` com `clipping:trigger`; descoberta via OpenAPI; MCP na fase 2 |
| Operação de TI | Reprocessar um dia com falha | `POST /clipping/v1/runs` com `reference_date` |
| Auditor | Rastrear por qual execução um item entrou | `GET /clipping/v1/runs/{id}` |

---

## 6. Requisitos funcionais

### 6.1 Persistência (RF-P)

| ID | Requisito | Prioridade |
|---|---|---|
| RF-P1 | Toda execução de busca grava itens em `clipping.item`, com chave determinística | Must |
| RF-P2 | Reprocessar a mesma data **não** duplica itens (upsert por `id`) | Must |
| RF-P3 | Cada execução gera registro em `clipping.run` com status `queued/running/success/partial/failed` | Must |
| RF-P4 | Cada fonte gera registro em `clipping.source_run` com status, erro e duração | Must |
| RF-P5 | Itens guardam `source`, `uf`, `gazette`, `published_date`, `section`, `org`, `title`, `abstract`, `url`, `matched_terms` | Must |
| RF-P6 | Item é imutável em conteúdo; mudança de recorte atualiza `updated_at` e `content_hash` | Should |
| RF-P7 | Histórico preservado com retenção configurável (padrão 24 meses) | Should |
| RF-P8 | Persistência ocorre mesmo quando a notificação falha | Must |

### 6.2 Adaptadores estaduais (RF-A)

| ID | Requisito | Prioridade |
|---|---|---|
| RF-A1 | Existir interface única `StateGazetteAdapter` com `search()`, `normalize()`, `fetch_by_date()` | Must |
| RF-A2 | `sources` aceitar `DOE:SP` e `DOE:MG`; `DOESP` continua aceito como alias | Must |
| RF-A3 | `sources` ser validado; valor desconhecido falha no parse do YAML com mensagem clara | Must |
| RF-A4 | O adaptador de SP reutilizar `DOESPHook` (endpoints `v2/journals` e `v2/advanced-search/publications`) | Must |
| RF-A5 | O adaptador de MG usar a API `jornalminasgerais.mg.gov.br/api/v1/` | Must |
| RF-A6 | MG suportar cadernos `Executivo` e `Municipios`; `Terceiros`/`EdicaoExtra` configuráveis, default desligado | Must |
| RF-A7 | MG usar bundle CA com o intermediário Sectigo; falha de TLS vira erro explícito e monitorado | Must |
| RF-A8 | Adaptadores respeitarem timeout (10 s connect/30 s read), retry limitado e backoff | Must |
| RF-A9 | Cada adaptador declara `rate_limit_rps`; execução não excede (padrão 1 req/s) | Should |
| RF-A10 | Adaptador normaliza para o formato `grupo → termo → departamento → itens` já usado pelo projeto | Must |
| RF-A11 | Adaptador de MG não baixa PDF quando a busca textual já responde | Should |
| RF-A12 | Cobertura de cadernos e disponibilidade do dia documentadas na saída da run | Should |

### 6.3 API (RF-API)

| ID | Requisito | Prioridade |
|---|---|---|
| RF-API1 | `GET /clipping/v1/items` com filtros `date|from|to`, `source`, `uf`, `q`, `page`, `size` | Must |
| RF-API2 | `GET /clipping/v1/items/{id}` | Must |
| RF-API3 | Resposta declara `sources_requested` e `sources_failed` | Must |
| RF-API4 | `GET /clipping/v1/runs/{id}` e `GET /clipping/v1/runs?date=...` | Must |
| RF-API5 | `POST /clipping/v1/runs` dispara DAG (`reference_date`, `sources`) e devolve `run_id`; aceita `Idempotency-Key` | Must |
| RF-API6 | `GET /clipping/v1/sources` lista fontes, UFs, cadernos e último status | Should |
| RF-API7 | Paginação com `total`, `page`, `size` (máx. 200) e `next_page` | Must |
| RF-API8 | Erros em `application/problem+json` com `type`, `title`, `status`, `detail` e `allowed` quando aplicável | Must |
| RF-API9 | OpenAPI 3.1 em `/clipping/openapi.json` e UI em `/clipping/docs` | Must |
| RF-API10 | `ETag` + `If-None-Match` retornando `304`; `Cache-Control` curto para leitura | Should |
| RF-API11 | Headers `X-RateLimit-Limit/Remaining/Reset` e `Retry-After` em `429` | Must |
| RF-API12 | Escopos de chave: `clipping:read` e `clipping:trigger`; trigger exigido só no `POST` | Must |
| RF-API13 | Ordenação determinística (`published_date DESC, id ASC`) e IDs estáveis | Must |
| RF-API14 | Resposta sem HTML e sem campos internos; timestamps ISO-8601 com fuso explícito | Must |
| RF-API15 | `GET /clipping/healthz` sem autenticação, para healthcheck do Traefik | Must |
| RF-API16 | Parâmetro `since` (delta por `updated_at`) para polling eficiente do agente | Should |

### 6.4 Resiliência e agendamento (RF-R)

| ID | Requisito | Prioridade |
|---|---|---|
| RF-R1 | Falha de uma fonte não impede coleta/persistência das demais | Must |
| RF-R2 | Run com fonte falha é `partial`; run sem nenhum item e com falha é `failed` | Must |
| RF-R3 | Retry por task conforme default do Airflow; sem retry infinito | Must |
| RF-R4 | Reprocessamento manual por data (`POST /clipping/v1/runs`) sem duplicar itens | Must |
| RF-R5 | Backfill de período por comando administrativo, com limite de janela | Should |
| RF-R6 | Janela de publicação por fonte respeitada (ex.: D-1) | Should |

### 6.5 Notificações (RF-N)

| ID | Requisito | Prioridade |
|---|---|---|
| RF-N1 | Notificações atuais continuam funcionando sem alteração de YAML | Must |
| RF-N2 | Notificação passa a informar fontes indisponíveis no corpo do relatório | Should |
| RF-N3 | `email_admin` recebe falha de execução e de fonte | Should |

### 6.6 Servidor MCP (fase 2 — RF-MCP)

| ID | Requisito | Prioridade |
|---|---|---|
| RF-MCP1 | Servidor MCP stateless que envolve a API v1, sem acesso direto ao banco | Must |
| RF-MCP2 | Ferramentas: `list_items`, `get_item`, `list_runs`, `get_run`, `list_sources`, `trigger_run` | Must |
| RF-MCP3 | `trigger_run` exige chave com `clipping:trigger` e propaga `Idempotency-Key` | Must |
| RF-MCP4 | Transporte Streamable HTTP em `/clipping/mcp`, atrás do mesmo router Traefik | Must |
| RF-MCP5 | Autenticação por `X-API-Key` ou `Authorization: Bearer` repassada à API; sem chave própria | Must |
| RF-MCP6 | Limite de contexto por resposta: máx. 50 itens e `abstract` ≤ 1.000 caracteres | Must |
| RF-MCP7 | Conteúdo dos diários é tratado como **dado**, nunca como instrução (mitigação de prompt injection) | Must |
| RF-MCP8 | Transporte `stdio` para desenvolvimento local, sem autenticação | Should |
| RF-MCP9 | Registro das chamadas de ferramenta por `key_id` e nome da ferramenta | Should |

---

## 7. Requisitos não funcionais (RNF)

| ID | Requisito |
|---|---|
| RNF1 | Compatibilidade Linux/macOS/Windows no código; sem `shell=True`, sem caminhos montados com separador manual |
| RNF2 | Nenhum segredo em log, resposta da API, YAML versionado ou imagem |
| RNF3 | API lê o banco com `clipping_ro`, que tem `SELECT` e `INSERT` apenas em `clipping.api_request` (auditoria) |
| RNF4 | Tempo máximo de execução diária: 30 min para DOU + SP + MG |
| RNF5 | Armazenamento estimado: < 500 MB/mês com `abstract` (sem texto completo) |
| RNF6 | Consultas da API não podem varrer tabela inteira (índices obrigatórios) |
| RNF7 | Testes determinísticos, sem rede, com fixtures das APIs SP/MG |
| RNF8 | Deploy em Compose e Helm sem quebrar o perfil local (`make run`) |
| RNF9 | Documentação pública atualizada (fonte, YAML, API) |
| RNF10 | Compatibilidade com Airflow 3.3 e Postgres 17.5 já em uso |
| RNF11 | API publicada em `/clipping` no host existente, sem alterar rotas do Airflow |
| RNF12 | Contrato versionado em `/clipping/v1`; mudança incompatível exige `/v2` |
| RNF13 | Respostas consumíveis por agente de IA: JSON estável, sem HTML, com `operationId` documentados |

---

## 8. Arquitetura

### 8.1 Visão de componentes

```
┌──────────────────────────────────────────────────────────────────┐
│ Airflow (DAG diária)                                             │
│  exec_search ──► persist_results ──► notify                      │
│      │                │                                          │
│      │                └──────────────┐                           │
│  searchers/adapters                  │                           │
│   ├─ DOU                             ▼                           │
│   ├─ DOE:SP  ───────────────►  Postgres                          │
│   └─ DOE:MG  ───────────────►  clipping DB                       │
└──────────────────────────────────────────────────────────────────┘
                                      ▲
                                      │ SELECT (clipping_ro)
                              ┌───────┴────────────┐
                              │ Rodou Clipping API │  FastAPI
                              │ /clipping/v1/...   │  Traefik + TLS
                              └────────────────────┘
                                      ▲
                                      │ X-API-Key (agente de IA e integradores)
                              consumidores máquina-a-máquina
```

Na fase 2, um serviço `clipping-mcp` envolve a API pelo mesmo host
(`/clipping/mcp`) e pela mesma credencial, sem acesso direto ao banco.

### 8.2 Componentes

| Componente | Responsabilidade | Estado |
|---|---|---|
| `src/searchers.py` | Adaptadores de fonte, normalização | existente, a estender |
| `src/hooks/doesp_hook.py` | Cliente SP | existente, a encapsular |
| `src/hooks/doemg_hook.py` | Cliente MG | novo |
| `src/dou_dag_generator.py` | Orquestração e nova task `persist_results` | existente, a estender |
| Postgres `clipping` DB | Persistência e histórico | novo |
| `services/clipping_api/` | API FastAPI | novo |
| `services/clipping_mcp/` | Servidor MCP sobre a API (fase 2) | novo |
| Traefik | TLS e roteamento | existente |
| Airflow REST API | Trigger de run sob demanda | existente |

### 8.3 Decisão D1 — endpoint público no host existente

- **Produto:** *Rodou Clipping*.
- **Endpoint canônico:** `https://rodou.ninja-apps.work/clipping/v1`, no mesmo
  host do Airflow, com prefixo de caminho dedicado.
- **Justificativa:** o consumidor principal é um **agente de IA de outro time**,
  que já foi apontado para esse endereço; usar o host existente dispensa DNS
  novo, reaproveita o certificado Let's Encrypt em produção e simplifica o
  provisionamento. O prefixo `/clipping` mantém a API separada das rotas do
  Airflow (`/login`, `/api`, `/dags`).
- **Roteamento (Traefik):**
  - router `clipping` com ``PathPrefix(`/clipping`)`` e `priority` explícita
    **maior** que a do router `ro-dou` (que usa `Host(...)` e captura qualquer
    caminho); sem prioridade explícita a regra mais longa vence, mas não se
    depende disso;
  - mesmo entrypoint `websecure` e mesmo `certresolver` do Airflow;
  - **sem** `stripPrefix`: o serviço recebe `/clipping/v1/...` e o FastAPI
    declara as rotas já com o prefixo `/clipping` (equivale a `root_path`, sem
    depender de o proxy remover o prefixo);
  - healthcheck do serviço em `/clipping/healthz` (fora da versão).
- **Isolamento na mesma origem:**
  - a API **não** define cookies de sessão e não compartilha domínio de cookie
    com o Airflow; autenticação é por header `X-API-Key`;
  - CORS desabilitado por padrão;
  - middlewares de autenticação do Traefik aplicados apenas ao router
    `clipping`, sem afetar o Airflow e vice-versa.
- **Riscos da escolha:** qualquer WAF, rate limit ou middleware global do host
  passa a valer para os dois serviços; mudanças no router `ro-dou` precisam ser
  revisadas quanto à prioridade para não capturar `/clipping`.
- **Nome técnico dos recursos:** projeto Compose `rodou-clipping`, serviço
  `clipping-api`, banco `clipping`, DAG `rodou_clipping`, router `clipping`.

---

## 9. Modelo de dados

Banco novo `clipping` no cluster Postgres existente (isolado do metadado do
Airflow e da base `inlabs`).

```sql
CREATE SCHEMA IF NOT EXISTS clipping;

CREATE TABLE clipping.run (
    id               text PRIMARY KEY,             -- '<dag_id>:<airflow_run_id>'
    dag_id           text NOT NULL,
    dag_run_id       text,                         -- run_id do Airflow
    reference_date   date NOT NULL,
    status           text NOT NULL CHECK (status IN
                       ('queued','running','success','partial','failed')),
    sources_requested text[] NOT NULL,
    sources_ok        text[] NOT NULL DEFAULT '{}',
    sources_failed    jsonb  NOT NULL DEFAULT '[]', -- [{source,error,at}]
    items_count      integer NOT NULL DEFAULT 0,
    started_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz
);

CREATE TABLE clipping.source_run (
    run_id       text NOT NULL REFERENCES clipping.run(id) ON DELETE CASCADE,
    source       text NOT NULL,
    status       text NOT NULL CHECK (status IN ('ok','empty','failed')),
    items_count  integer NOT NULL DEFAULT 0,
    error        text,
    duration_ms  integer,
    PRIMARY KEY (run_id, source)
);

CREATE TABLE clipping.item (
    id               text PRIMARY KEY,             -- sha256(source|uf|external_id)
    run_id           text NOT NULL REFERENCES clipping.run(id),
    source           text NOT NULL,                -- DOU | DOE:SP | DOE:MG
    uf               char(2),
    gazette          text,                         -- caderno/seção
    external_id      text,
    published_date   date NOT NULL,
    section          text,
    org              text,
    title            text,
    abstract         text,
    url              text NOT NULL,
    matched_terms    text[] NOT NULL DEFAULT '{}',
    content_hash     text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX item_date_source_idx  ON clipping.item (published_date, source);
CREATE INDEX item_uf_date_idx      ON clipping.item (uf, published_date);
CREATE INDEX item_org_idx          ON clipping.item (lower(org));
CREATE INDEX item_terms_gin        ON clipping.item USING gin (matched_terms);
CREATE INDEX item_search_fts       ON clipping.item
    USING gin (to_tsvector('portuguese', coalesce(title,'') || ' ' || coalesce(abstract,'')));
```

Regras:

- `id = sha256(f"{source}|{uf or '-'}|{external_id}")`; se a fonte não expuser ID,
  usar `url` canônica como `external_id`.
- O `id` da run é `<dag_id>:<run_id do Airflow>`, porque o `run_id` do Airflow
  não é único entre DAGs diferentes agendadas no mesmo horário.
- `INSERT ... ON CONFLICT (id) DO UPDATE SET updated_at = now(),
  content_hash = EXCLUDED.content_hash, abstract = EXCLUDED.abstract` (upsert).
- `run_id` da primeira inserção é preservado; reprocessamento não reescreve a
  origem histórica.
- `sources_failed` guarda `[{source, error, at}]` sem stack trace e sem payload.

---

## 10. Contrato da API

Base: `https://rodou.ninja-apps.work/clipping/v1`

Os caminhos abaixo são **relativos à base**; o caminho público completo sempre
começa com `/clipping/v1`. Autenticação por `X-API-Key`, com escopo
`clipping:read` na leitura e `clipping:trigger` no disparo de execução.

### 10.1 `GET /items`

Parâmetros: `date` **ou** `from`/`to` (máx. 31 dias), `source`, `uf`, `q`,
`since` (opcional, ISO-8601), `page` (default 1), `size` (default 50, máx. 200).

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
      "gazette": "Executivo",
      "published_date": "2026-11-16",
      "section": "Atos do Executivo",
      "org": "Secretaria de Estado de Saúde",
      "title": "Portaria nº 123/2026",
      "abstract": "…trecho com o termo pesquisado…",
      "url": "https://www.jornalminasgerais.mg.gov.br/…",
      "matched_terms": ["licitação"],
      "updated_at": "2026-11-16T05:12:41-03:00"
    }
  ]
}
```

Resposta inclui `ETag`; `If-None-Match` correspondente devolve `304` sem corpo.

### 10.2 `GET /items/{id}`

Retorna o item ou `404` em `application/problem+json`.

### 10.3 `GET /runs/{id}` e `GET /runs?date=`

```json
{
  "id": "0d1c…",
  "reference_date": "2026-11-16",
  "status": "partial",
  "sources_ok": ["DOU", "DOE:MG"],
  "sources_failed": [{ "source": "QD", "error": "HTTP 503", "at": "2026-11-16T05:12:03-03:00" }],
  "items_count": 128,
  "started_at": "2026-11-16T05:00:00-03:00",
  "finished_at": "2026-11-16T05:12:41-03:00"
}
```

### 10.4 `POST /runs`

```json
{ "reference_date": "2026-11-15", "sources": ["DOU", "DOE:SP"] }
```

Resposta `202 Accepted` com `run_id` e `dag_run_id`; `409` se já houver run
`running` para a mesma data. Aceita `Idempotency-Key`: repetição com a mesma
chave devolve o mesmo `run_id` sem novo disparo.

O disparo usa a API REST do Airflow (`/api/v2/dags/{dag}/dagRuns`) com
`logical_date` ao meio-dia de Brasília e `conf` contendo `reference_date` e
`sources`. O cache de `Idempotency-Key` é mantido **em memória, por instância**
(24 h); em múltiplas réplicas, a garantia exige armazenamento compartilhado.

### 10.5 `GET /sources`

Lista `source`, `uf`, `cadernos`, `last_status`, `last_run_at`, `rate_limit_rps`.

### 10.6 Códigos de erro e headers

| Código | Situação |
|---|---|
| 304 | `If-None-Match` sem alteração |
| 400 | parâmetro inválido (data, janela > 31 dias, `source` desconhecido) |
| 401 | `X-API-Key` ausente |
| 403 | chave inválida/inativa ou sem o escopo exigido |
| 404 | item ou run inexistente |
| 409 | execução concorrente para a mesma data |
| 429 | limite de requisições (120 req/min por chave; 10/hora no `POST /runs`) |
| 503 | banco ou Airflow indisponível |

Headers de resposta: `ETag`, `Cache-Control: private, max-age=60`,
`X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`; em `429`,
`Retry-After` em segundos.

### 10.7 Consumo por agente de IA

O consumidor principal é um agente autônomo mantido por outro time; o contrato
é desenhado para chamadas de ferramenta (function calling), não para navegação
humana:

- **Credencial e escopo**: o agente tem chave própria com **`clipping:read` e
  `clipping:trigger`**; todo `POST /runs` fica auditado por `key_id`.
- **Cadência diária**: o agente consulta **uma vez por dia**, após a janela de
  coleta (06:30 BRT). Fluxo recomendado:
  1. `GET /clipping/v1/runs?date=<D-1>` — se `status` for `success`, seguir;
     se `failed`/ausente, `POST /clipping/v1/runs` com
     `{"reference_date":"<D-1>"}` e `Idempotency-Key` fixo por data;
  2. `GET /clipping/v1/items?date=<D-1>&size=200` — paginar por `next_page`;
  3. em caso de `partial`, ler `sources_failed` e registrar a lacuna, sem
     repetir a coleta das fontes que já retornaram.
- **Trigger idempotente**: como a cadência é diária, o `Idempotency-Key`
  recomendado é `clipping:<reference_date>`; repetições no mesmo dia devolvem o
  mesmo `run_id`, e runs concorrentes recebem `409`.
- **Custo por chamada**: `size` padrão 50 (máx. 200), `abstract` truncado em
  1.000 caracteres, resposta sem HTML e sem campos internos; `since` e `ETag`
  evitam rebaixar o mesmo conteúdo.
- **Descoberta**: `GET /clipping/openapi.json` (público, sem dados) com
  `operationId` estáveis (`listItems`, `getItem`, `getRun`, `triggerRun`,
  `listSources`) para mapeamento direto em ferramentas.
- **Determinismo**: ordenação estável (`published_date DESC, id ASC`), IDs
  imutáveis, timestamps ISO-8601 com offset explícito.
- **Erros acionáveis**: `problem+json` com `detail` legível e `allowed`
  listando valores válidos (ex.: `sources`), para o agente corrigir a chamada
  em vez de repeti-la às cegas.
- **Isolamento de credencial**: revogação e throttling da chave do agente não
  afetam operadores nem outros integradores.
- **MCP**: fase 2 (§10.8), mesma credencial e mesmos escopos.

### 10.8 Servidor MCP (fase 2)

O time do agente quer consumir o clipping por **Model Context Protocol**. O
servidor MCP é um invólucro fino sobre esta API, não uma segunda
implementação de dados:

- **Ferramentas** (nomes estáveis):

  | Ferramenta | Equivalente REST | Escopo |
  |---|---|---|
  | `list_items` | `GET /items` | `clipping:read` |
  | `get_item` | `GET /items/{id}` | `clipping:read` |
  | `list_runs` | `GET /runs` | `clipping:read` |
  | `get_run` | `GET /runs/{id}` | `clipping:read` |
  | `list_sources` | `GET /sources` | `clipping:read` |
  | `trigger_run` | `POST /runs` | `clipping:trigger` |

- **Transporte**: HTTP JSON-RPC em `https://rodou.ninja-apps.work/clipping/mcp`,
  SSE em `/clipping/mcp/sse` + `/clipping/mcp/messages` e `stdio` local.
- **Autenticação**: a chave do agente é repassada como `X-API-Key` (ou
  `Authorization: Bearer`) ao servidor MCP, que a repassa à API; o MCP não tem
  store de chaves próprio.
- **Sem estado**: nenhuma tabela nova; o MCP lê a API v1 e devolve o mesmo
  contrato.
- **Limites de contexto**: no máximo 50 itens por resposta, `abstract` ≤ 1.000
  caracteres, paginação explícita; nunca despejar o dia inteiro numa única
  resposta.
- **Segurança de conteúdo**: títulos e abstracts são dados não confiáveis;
  as respostas textuais são precedidas de aviso e o servidor nunca os
  interpreta como instrução (mitigação de prompt injection).
- **Observabilidade**: cada chamada de ferramenta registra `key_id`,
  ferramenta, duração e status; erros 5xx alertam como os da API.
- **Critério de saída da fase 2**: o agente executa o ciclo diário completo
  (checar run, disparar se necessário, listar itens) usando apenas ferramentas
  MCP.

---

## 11. Especificação dos adaptadores

### 11.1 Interface

```python
class StateGazetteAdapter(Protocol):
    source_id: str          # "DOE:SP" | "DOE:MG"
    uf: str                 # "SP" | "MG"
    cadernos: dict[str, str]
    rate_limit_rps: float

    def search(self, terms, reference_date, search_date,
               cadernos=None) -> dict[str, list[dict]]: ...
    def normalize(self, raw, term) -> dict: ...
    def fetch_by_date(self, day) -> list[dict]: ...  # backfill
```

`reference_date` e `search_date` seguem os presets do Ro-DOU
(`DIA`/`SEMANA`/`MES`/`ANO`); `search` devolve os itens agrupados por termo.
`fetch_by_date` pode levantar `NotImplementedError` quando a API não permitir
listar por data sem termos. O registry fica em `src/state_adapters.py`
(`STATE_ADAPTERS` e `STATE_SOURCE_SEARCHERS`).

`RawItem`/`ClippingItem` usam o dicionário canônico já consumido por
`_group_results` (`title`, `abstract`, `href`, `section`, `arttype`, `pubdate`…),
garantindo que templates e notificadores atuais não mudem.

### 11.2 SP (DOE-SP)

| Item | Valor |
|---|---|
| Base | `https://do-api-web-search.doe.sp.gov.br/v2/` |
| Cadernos | `GET /journals` |
| Busca | `GET /advanced-search/publications` |
| Autenticação | nenhuma |
| Cadernos padrão | `Executivo` (configurável via `journals`) |
| Reaproveitamento | `DOESPHook` e `DOESPSearcher` atuais viram implementação da interface |
| Compatibilidade | `DOESP` permanece aceito como alias de `DOE:SP` |

### 11.3 MG (DOE-MG)

| Item | Valor |
|---|---|
| Base | `https://www.jornalminasgerais.mg.gov.br/api/v1/` |
| Busca | `GET Pesquisa/PesquisarJornaisPaginados?DataPublicacaoInicial=&DataPublicacaoFinal=&TextoPesquisa=&DiarioExecutivo=&DiarioMunicipios=&DiarioTerceiros=&EdicaoExtra=` |
| Edição por data | `GET Jornal/ObterEdicaoPorDataPublicacao?dataPublicacao=YYYY-MM-DD` |
| Conteúdo | `GET Caderno/ObterConteudoCadernoPorId` (texto) |
| PDF | `GET Caderno/ObterArquivoCadernoPorId` (uso opcional) |
| Cadernos | Executivo, Municípios Mineiros, Terceiros, Edição Extra |
| Defaults | Executivo e Municípios ligados; Terceiros e Extra desligados |
| Paginação | por parâmetro de página; limite de páginas por execução |
| TLS | cadeia incompleta (folha emitida por `Sectigo Public Server Authentication CA DV R36`, intermediário ausente) |
| Mitigação TLS | bundle versionado `src/hooks/certs/sectigo_dv_r36.pem` com folha + intermediário; `verify=<bundle>`; **proibido** `verify=False` |
| Rate limit | 1 req/s, backoff exponencial (2 s, 4 s, 8 s), máx. 3 tentativas |
| Risco | API não documentada, descoberta no bundle JS; mudanças devem ser detectadas por teste de contrato |

### 11.4 Vocabulário de `sources`

| Valor | Situação |
|---|---|
| `DOU` | existente |
| `INLABS` | existente (sujeito à disponibilidade do portal) |
| `QD` | existente |
| `DOESP` | existente, **alias depreciado** de `DOE:SP` |
| `DOE:SP` | novo canônico |
| `DOE:MG` | novo |

Regra de validação: união explícita; valor fora da lista gera erro de schema com
a lista aceita. Combinações permitidas: qualquer subconjunto, com a restrição de
que `DOU` e `INLABS` continuam mutuamente exclusivos por decisão de produto
(documentar e validar, em vez de ignorar silenciosamente).

### 11.5 Exemplo de YAML

```yaml
dag:
  id: clipping_mg_sp
  description: "Clipping DOU + DOE-MG + DOE-SP"
  search:
    sources: [DOU, DOE:MG, DOE:SP]
    terms: [licitação, contrato]
    journals: [Executivo, Municipios]
    date: DIA
  report:
    emails: [dev@tresia.com.br]
```

---

## 12. Segurança, privacidade e operação

### 12.1 Persistência de segredos

- `CLIPPING_API_KEY_*` em Secret/`.env.production` (nunca versionado).
- Credenciais do banco em `AIRFLOW_CONN_CLIPPING_DB` e `CLIPPING_DB_URL`.
- `X-API-Key` nunca logada; log apenas do `key_id` (prefixo de 8 caracteres).

### 12.2 Banco

- Usuário de escrita: `clipping_rw` (usado pelo Airflow).
- Usuário da API: `clipping_ro`, com `SELECT` em `clipping.*` e `INSERT`
  apenas em `clipping.api_request` (auditoria).
- `pg_dump` diário do banco `clipping` junto ao backup do Airflow.
- Retenção padrão 24 meses; expurgo por job mensal (`scripts/purge.py` ou
  CronJob `clippingRetention`) com `DELETE` em lote e `VACUUM`.

### 12.3 Autenticação da API

- Chave de API por consumidor, armazenada como `sha256` em
  `clipping.api_key(id, key_hash, label, scope, active, created_at, revoked_at)`.
- Header `X-API-Key`; comparação em tempo constante.
- Escopos:
  - `clipping:read` — leitura de itens, runs e fontes;
  - `clipping:trigger` — `POST /runs`.
- **Chave do agente de IA**: `clipping:read` + `clipping:trigger` (decisão
  aprovada), com cadência diária e `Idempotency-Key` por data; o trigger cobre
  recuperação de execução ausente ou falha do dia.
- **Uma chave por consumidor**: o agente de IA recebe chave própria, para que
  revogação ou throttling não afetem operadores nem outros integradores.
- Limite 120 req/min por chave; `POST /runs` limitado a 10/hora.
- Auditoria em `clipping.api_request` (`key_id`, rota, status, latência,
  `request_id`), sem payload e sem `abstract`; retenção de 90 dias.
- CORS desabilitado por padrão; habilitar apenas para origens explícitas.

### 12.4 LGPD e conteúdo

- Atos oficiais são públicos, mas podem conter dados pessoais: não expor
  `raw_payload`; não registrar `abstract` em log.
- `abstract` limitado a 1.000 caracteres por item.
- Direito de eliminação não se aplica a ato oficial; ainda assim, prever expurgo
  por retenção.

### 12.5 Termos de uso das fontes

- SP e MG: uso automatizado deve respeitar `robots.txt`, limite de 1 req/s e
  cache; registrar contato e finalidade no README operacional.
- MG: API não documentada — avaliar contato com a Imprensa Oficial antes de
  elevar volume.

---

## 13. Observabilidade e SLOs

| Sinal | Implementação |
|---|---|
| Healthcheck da API | `GET /clipping/healthz` (processo + conexão de leitura) |
| Métricas Prometheus | `GET /clipping/metrics` com requisições por rota/status, latência, 429, disparos e status por fonte |
| Métrica de itens | contador por fonte/run em log estruturado |
| Falha de fonte | log `WARNING` + `source_run.status=failed` + alerta se 2 dias consecutivos |
| Duração | `clipping.run.finished_at - started_at` |
| API | log de acesso sem payload, latência p50/p95 |
| Agente de IA | requisições por `key_id`, taxa de 304 (cache), taxa de 429, latência p95 |
| MCP (fase 2) | chamadas por ferramenta, erros de permissão, latência p95 |
| Alerta | e-mail para `email_admin` em `failed` e em `partial` com fonte federal |

SLOs: conclusão diária até 06:30 BRT; API p95 ≤ 300 ms; disponibilidade 99,5%
mensal; zero perda de item já persistido.

---

## 14. Compatibilidade e migração

- YAMLs existentes permanecem válidos; `DOESP` continua funcionando.
- Nenhum campo existente muda de nome ou default.
- A DAG só passa a persistir quando `clipping` estiver configurado
  (`AIRFLOW_CONN_CLIPPING_DB`); sem a connection, comportamento atual é mantido e
  um aviso é registrado (degradação controlada).
- Migração do banco via `dag_load_inlabs/sql`-style script versionado em
  `clipping/sql/` + `CREATE ... IF NOT EXISTS`.
- Rollback: remover a task `persist_results` do grafo e a API do Compose; dados
  permanecem no banco para análise.

---

## 15. Plano de entrega

| Fase | Escopo | Esforço | Critério de saída |
|---|---|---|---|
| F0 | Isolamento por fonte + validação de `sources` | 2 d | QD fora do ar não impede DOU; YAML inválido falha claro |
| F1 | Banco `clipping`, DDL, `persist_results`, run tracking | 3 d | Itens persistidos, idempotentes, com run `success/partial` |
| F2 | Adapter MG (hook + searcher + TLS bundle + testes) | 4 d | `sources: [DOE:MG]` coleta Executivo e Municípios |
| F3 | Adapter SP sobre a interface comum (mantendo `DOESP`) | 2 d | `DOE:SP` e `DOESP` produzem o mesmo resultado |
| F4 | API FastAPI, auth, paginação, OpenAPI | 5 d | Contrato de §10 validado por testes de integração |
| F5 | Deploy (Compose/Helm), Traefik, docs, observabilidade | 2 d | `https://rodou.ninja-apps.work/clipping/healthz` = 200 |
| F6 | Backfill opcional de período + expurgo | 2 d | Backfill de 31 dias sem duplicidade |
| **F7 (fase 2)** | Servidor MCP sobre a API v1 (ferramentas, auth passthrough, `stdio`) | 3 d | Agente completa o ciclo diário só com ferramentas MCP |

Total da v1: ~20 dias-pessoa (1 dev), ~4 sprints de 1 semana.
Fase 2 (MCP): +3 dias-pessoa, após a v1 em produção.

---

## 16. Critérios de aceite (testáveis)

1. **Isolamento**: com QD respondendo 503, a execução termina `partial`, os itens
   do DOU são persistidos e `sources_failed` contém `QD`.
2. **Idempotência**: executar `POST /clipping/v1/runs` duas vezes para a mesma
   data mantém `count(item)` inalterado e não duplica notificação; repetir com o
   mesmo `Idempotency-Key` devolve o mesmo `run_id`.
3. **MG feliz**: `sources: [DOE:MG]` para uma data com publicação retorna itens
   com `uf=MG`, `gazette` em `{Executivo, Municipios}` e `url` válida.
4. **MG sem TLS**: sem o bundle CA, o adapter falha com erro explícito
   (`TLS verify failed`) e `source_run.status=failed`; com o bundle, sucede.
5. **MG sem publicação**: dia sem edição retorna `source_run.status=empty`, run
   `success` (ou `partial` se outra fonte falhou), sem erro.
6. **SP compatível**: `DOESP` e `DOE:SP` geram o mesmo conjunto de itens para a
   mesma data e termos.
7. **Schema**: `sources: [XPTO]` falha no parse com mensagem que lista os valores
   aceitos.
8. **API**: `GET /clipping/v1/items?date=...` sem chave → 401; chave inválida →
   403; `size=500` → 400; resposta declara `sources_failed`.
9. **Trigger**: `POST /clipping/v1/runs` devolve `202` e `run_id`; execução
   concorrente devolve `409`.
10. **Persistência mesmo com falha de notificação**: simular SMTP indisponível e
    verificar itens persistidos.
11. **Sem regressão**: suíte atual (`make tests`) permanece verde; DAGs de
    exemplo carregam sem erro.
12. **Agente — descoberta**: `GET /clipping/openapi.json` retorna 200 com
    `operationId` estáveis (`listItems`, `getItem`, `getRun`, `triggerRun`,
    `listSources`), sem exigir dados.
13. **Agente — cache**: segunda chamada com o `ETag` anterior em
    `If-None-Match` retorna 304 e corpo vazio.
14. **Agente — escopo**: chave apenas com `clipping:read` recebe 403 em
    `POST /clipping/v1/runs`; chave com `clipping:trigger` recebe 202.
15. **Agente — erro acionável**: `source=XPTO` retorna 400 em `problem+json`
    com `allowed` listando as fontes válidas.
16. **Roteamento**: `GET /clipping/healthz` responde 200 e as rotas do Airflow no
    mesmo host (`/login`, `/api/v2/monitor/health`) continuam inalteradas.

**Fase 2 — MCP** (aceite separado, após a v1 em produção):

17. **Paridade**: `list_items` retorna os mesmos itens que
    `GET /clipping/v1/items` para a mesma data e filtros.
18. **Trigger**: `trigger_run` com chave válida cria run; sem
    `clipping:trigger` retorna erro de permissão e nenhuma run é criada.
19. **Contexto**: nenhuma ferramenta devolve mais de 50 itens nem `abstract`
    acima de 1.000 caracteres.
20. **Prompt injection**: um item cujo título contém instruções é devolvido como
    dado; o servidor MCP não executa nem dispara ação a partir do conteúdo.

---

## 17. Riscos e mitigações

| Risco | Impacto | Prob. | Mitigação |
|---|---|---|---|
| APIs estaduais mudarem sem aviso | Alto | Média | teste de contrato diário com fixture; alerta em mudança de schema |
| TLS de MG permanecer quebrado | Alto | Alta | bundle CA versionado; monitorar expiração e cadeia |
| QD/INLABS seguirem instáveis | Médio | Alta | isolamento por fonte; status `partial`; não bloquear DOU |
| Volume de dados crescer | Baixo | Média | retenção 24 meses; só `abstract`; índice FTS |
| Duplicidade por ID instável | Médio | Média | `external_id` canônico; `content_hash`; teste de idempotência |
| Bloqueio por uso automatizado | Alto | Baixa | rate limit 1 req/s; contato institucional; cache |
| Mudança de fuso/data de referência | Médio | Média | `America/Sao_Paulo` fixo; D-1 documentado |
| Exposição da API sem auth | Alto | Baixa | chave obrigatória; teste 401/403 no CI |
| Custo de IA | — | — | fora de escopo nesta fase |

---

## 18. Dependências e premissas

- Airflow 3.3, Postgres 17.5 e Traefik 3.6.15 já operacionais (verificado).
- O host `rodou.ninja-apps.work` já tem certificado Let's Encrypt válido e
  router do Airflow; basta um router `clipping` com prioridade explícita maior.
- API de MG permanece acessível; bundle TLS obtível do emissor.
- `DOESP` continua estável.
- Um responsável institucional para contato com Imprensa Oficial MG.
- Sem necessidade de aprovação de orçamento (sem IA nesta fase).
- O time do agente de IA aceita o contrato de §10 e opera com **uma chave
  própria** com `clipping:read` e `clipping:trigger`, em cadência **diária**.
- O MCP (§10.8) é fase 2, com o mesmo time e a mesma chave; não bloqueia a v1.

---

## 19. Questões abertas

1. **Endpoint**: resolvido — `https://rodou.ninja-apps.work/clipping/v1` (D1).
2. **Escopo do agente**: resolvido — `clipping:read` + `clipping:trigger`.
3. **Polling do agente**: resolvido — cadência **diária**, após 06:30 BRT.
4. **MCP**: resolvido — servidor MCP na **fase 2** (§10.8).
5. **Autenticação futura**: chave de API basta ou haverá OIDC/gov.br?
6. **Retenção**: 24 meses é adequado ou há exigência de guarda maior?
7. **MG**: habilitar Terceiros e Edição Extra por padrão?
8. **Backfill**: qual data inicial (2020, início do INLABS, outra)?
9. **Conteúdo**: persistir só `abstract` ou também texto completo de MG?
10. **MCP — transporte**: apenas Streamable HTTP + stdio ou também SSE legado?
11. **MCP — credencial**: mesma chave da API ou chave MCP dedicada por cliente?

---

## 20. Anexos

### 20.1 Evidências técnicas (16/11/2026)

```
QD:        GET https://api.queridodiario.org.br/gazettes → 503 "no available server"
INLABS:    https://inlabs.in.gov.br → "Estamos em manutenção"
DOU:       GET https://www.in.gov.br/consulta/-/buscar/dou → 200 (209 KB)
DOESP:     https://do-api-web-search.doe.sp.gov.br/v2/journals → 200
DOE-MG:    https://www.jornalminasgerais.mg.gov.br/api/v1/Jornal/ObterEdicaoPorDataPublicacao → 200
           TLS: 1 certificado na cadeia; emissor Sectigo DV R36 (intermediário ausente)
Airflow:   3.3.0, health 200, 25 DAGs, 0 runs
Postgres:  17.5, bancos airflow e inlabs
Traefik:   3.6.15, modo host, certificado Let's Encrypt válido até 14/02/2027
```

### 20.2 Referências

- `src/searchers.py` (classes `DOUSearcher`, `QDSearcher`, `INLABSSearcher`, `DOESPSearcher`)
- `src/hooks/doesp_hook.py`
- `src/dou_dag_generator.py` (`searchers`, `exec_search`, `merge_results`)
- `src/schemas.py` (`SearchConfig.sources`)
- `deploy/prod/README.md`, `docker-compose.override.yml`
- `AGENTS.md` (regras de testes, segurança e documentação)
