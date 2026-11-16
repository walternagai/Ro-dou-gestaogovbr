# Backlog de implementação — Rodou Clipping

Derivado do PRD `docs/prd/rodou-clipping.md` (v1.2). Cada tarefa é executável de
forma independente, com critério de aceite verificável.

| Campo | Valor |
|---|---|
| Base | PRD Rodou Clipping v1.2 |
| Data | 16/11/2026 |
| Escopo da v1 | Fases F0–F6 (itens A–G) |
| Fase 2 | F7 (itens H) — MCP |
| Unidade de esforço | horas (h); 8 h = 1 dia-pessoa |
| Ordem | Respeitar a coluna **Depende de** |

---

## 0. Como usar

1. **Trabalhe um épico por vez**, na ordem A → H. Dentro do épico, siga a ordem da
   tabela (as dependências já estão declaradas).
2. **Uma tarefa = um PR pequeno** (ou um commit coeso). Não misture tarefas de
   épicos diferentes no mesmo commit.
3. **Marque `[x]`** apenas quando o critério de aceite foi observado e a
   definição de pronto (§1) passou.
4. **Não feche tarefa com teste ausente**: toda mudança de comportamento exige
   teste próximo ao código alterado.
5. Dúvidas de contrato se resolvem no PRD (§19); dúvidas de escopo, com o
   responsável técnico antes de codar.

---

## 1. Definição de pronto (global)

Uma tarefa só está pronta quando:

- [ ] `ruff` limpo nos arquivos tocados;
- [ ] testes focados passam no contêiner (`docker exec airflow-api-server ...`);
- [ ] `make tests` verde quando a mudança afeta schemas, parser, geração ou DAGs;
- [ ] nenhum segredo, cache ou arquivo local entrou no diff;
- [ ] documentação/exemplo atualizado quando há mudança observável;
- [ ] YAMLs existentes continuam válidos;
- [ ] o critério de aceite específico da tarefa foi demonstrado (saída, log ou teste).

Comandos de referência (ambiente ativo):

```bash
# testes focados
docker exec airflow-api-server sh -c "cd /opt/airflow/tests && pytest -q <arquivo>::<teste>"

# suíte oficial
make tests

# lint
docker exec airflow-api-server ruff check src tests

# DAGs carregam
docker exec airflow-api-server sh -c "airflow dags list-import-errors"
```

> O repositório **não** possui `pytest-cov` nem gate de cobertura; não invente
> percentual. Cobertura só pode ser citada se medida com ferramenta instalada
> para isso.

---

## 2. Grafo de dependências

```
A1 ─┬─► A2 ─► A3 ─► A4
    └─► A5

B1 ─► B2 ─┬─► B3 ─► B4 ─┬─► B6
            ├─► B5 ──────┘
            └─► B7

D1 ─► D2 ─► D3          (D1 vem antes de C3)

C1 ─► C2 ─► C3 ─► C4 ─► C5 ─► C6
                             └─► C7

E1 ─┬─► E2 ─► E3
     ├─► E4 ─► E5
     └─► E6
E2..E6 ─► E9
E2..E9 ─► E11
E2..E9 ─► E12
E1 ─► E10 ─► F1 ─► F2 ─► F5
              └─► F3

B3 ─► G1
B4 ─► G2

E9 ─► H1 ─► H2..H6     (fase 2, após produção)
```

---

## 3. Épico A — Resiliência de fontes e validação (F0) · 16 h

Objetivo: uma fonte fora do ar não pode zerar o clipping nem produzir YAML
silenciosamente inválido.

| ID | Tarefa | Depende de | Est. | Evidência de conclusão |
|---|---|---|---|---|
| A1 | Validar `sources` em `SearchConfig` com união explícita (`DOU`, `INLABS`, `QD`, `DOESP`, `DOE:SP`, `DOE:MG`), mensagem com valores aceitos e regra para `DOU`+`INLABS` | — | 4 h | Teste: `sources: [XPTO]` falha com lista de valores; combinação proibida tem erro dedicado |
| A2 | Isolar falha por fonte em `exec_search` (`try/except` por fonte, log estruturado, seguir com as demais) | A1 | 4 h | Teste: QD levanta exceção e DOU ainda retorna itens |
| A3 | Propagar status por fonte (ok/empty/failed + erro) do `exec_search` até o consumidor, sem quebrar o retorno atual | A2 | 3 h | XCom/retorno inclui status por fonte; templates antigos seguem funcionando |
| A4 | Testes de isolamento e validação (QDSearcher mockado, schema) | A2, A3 | 3 h | Testes novos verdes; `make tests` verde |
| A5 | Documentar valores de `sources`, alias `DOESP` e semântica de falha parcial | A1 | 2 h | Página `docs/docs/como_funciona/parametros.md` atualizada |

**Aceite do épico:** com QD em 503, a DAG persiste/entrega o resultado do DOU e
declara a falha; `sources` inválido falha no parse.

**Status:** concluído em 16/11/2026 (A1–A5). Evidências: validação em
`src/schemas.py`; isolamento e `sources_status` em `src/dou_dag_generator.py`;
testes em `tests/sources_resilience_test.py`; documentação em
`docs/docs/como_funciona/parametros.md`.

**Arquivos previstos:** `src/schemas.py`, `src/dou_dag_generator.py`,
`src/searchers.py`, `tests/parsers_test.py`, `tests/test_validate_yaml_schemas.py`,
`docs/docs/como_funciona/parametros.md`.

---

## 4. Épico B — Persistência e rastreabilidade (F1) · 25 h

Objetivo: todo clipping fica gravado, idempotente e auditável por execução.

| ID | Tarefa | Depende de | Est. | Evidência de conclusão |
|---|---|---|---|---|
| B1 | DDL `clipping`: `run`, `source_run`, `item`, `api_key`, `api_request`; índices de data/UF/termos/FTS | — | 4 h | Script aplicado em base limpa cria tudo; `IF NOT EXISTS` permite reexecução |
| B2 | Roles `clipping_rw`/`clipping_ro` e script de migração versionado em `clipping/sql/` | B1 | 3 h | `clipping_ro` só lê `clipping.*`; `clipping_rw` escreve |
| B3 | Task `persist_results` no gerador: `id = sha256(source\|uf\|external_id)`, upsert, `run_id`, `matched_terms` | B2 | 6 h | Execução grava itens; rodar duas vezes mantém contagem |
| B4 | Ciclo de vida de `run`/`source_run` (`queued` → `running` → `success/partial/failed`) | B3 | 4 h | Run com fonte falha fica `partial`; sem itens e com falha, `failed` |
| B5 | `AIRFLOW_CONN_CLIPPING_DB` opcional, com degradação controlada e aviso | B2 | 2 h | Sem a connection, comportamento atual é mantido e há log de aviso |
| B6 | Testes: idempotência, persistência com SMTP fora, status parcial | B3, B4 | 4 h | Testes verdes; nenhuma duplicidade |
| B7 | Documentar operação: backup do banco `clipping`, retenção e restauração | B2 | 2 h | `deploy/prod/README.md` atualizado |

**Aceite do épico:** reprocessar a mesma data não duplica itens; a notificação
pode falhar sem perder dados; cada item aponta para a run que o inseriu.

**Status:** concluído em 16/11/2026 (B1–B7). Evidências:
`clipping/sql/` (schema, roles e init idempotente), `src/clipping_persistence.py`,
task `persist_results` em `src/dou_dag_generator.py`, testes unitários e de
integração (`tests/clipping_persistence*_test.py`) e documentação em
`deploy/prod/README.md`.

**Arquivos previstos:** `clipping/sql/*.sql` (novo), `src/dou_dag_generator.py`,
`deploy/prod/README.md`, `tests/` (novo arquivo de persistência).

---

## 5. Épico C — Adaptador DOE-MG (F2) · 28 h

Objetivo: coletar Executivo e Municípios de MG com TLS válido e sem PDF.

| ID | Tarefa | Depende de | Est. | Evidência de conclusão |
|---|---|---|---|---|
| C1 | Criar bundle CA (`src/hooks/certs/`) com folha + intermediário Sectigo e teste de verificação | — | 2 h | `requests.get(..., verify=bundle)` retorna 200; `verify=False` ausente no código |
| C2 | `DOEMGHook`: `PesquisarJornaisPaginados` com paginação, timeout (10/30 s), retry 3× e backoff; mapear cadernos | C1 | 8 h | Hook retorna lista normalizada para data com publicação |
| C3 | `MGSearcher` sobre a interface de adapter (ou `BaseSearcher` enquanto D1 não chega), com `_group_results` | C2, D1 | 4 h | Resultado no formato `grupo → termo → departamento → itens` |
| C4 | Registrar `DOE:MG` no gerador e incluir no `merge_results` | C3 | 3 h | YAML com `[DOE:MG]` gera DAG válida |
| C5 | YAML de exemplo e documentação da fonte | C4 | 3 h | `dag_confs/examples_and_tests/doemg_example.yaml` + página de docs |
| C6 | Testes com fixtures: busca feliz, dia sem edição, TLS sem bundle, 429/5xx | C2, C4 | 6 h | Sem rede nos testes; os quatro cenários cobertos |
| C7 | Rate limit 1 req/s + cache por data/caderno na execução | C2 | 2 h | Duas buscas no mesmo run não repetem requisição idêntica |

**Aceite do épico:** `sources: [DOE:MG]` coleta Executivo e Municípios; dia sem
publicação resulta `empty` sem erro; falha de TLS é explícita e monitorada.

**Status:** concluído em 16/11/2026 (C1–C7). Evidências:
`src/hooks/doemg_hook.py` (autenticação anônima, paginação, retry, rate limit,
cache e bundle TLS), `MGSearcher` em `src/searchers.py`, registro em
`src/dou_dag_generator.py`, exemplo
`dag_confs/examples_and_tests/doemg_example.yaml`, testes em
`tests/test_doemg_hook.py` e documentação em `parametros.md`/`exemplos.md`.

**Riscos específicos:** API não documentada (teste de contrato com fixture);
cadeia TLS pode mudar (monitorar validade).

**Arquivos previstos:** `src/hooks/doemg_hook.py` (novo),
`src/hooks/certs/sectigo_dv_r36.pem` (novo), `src/searchers.py`,
`src/dou_dag_generator.py`, `dag_confs/examples_and_tests/doemg_example.yaml`,
`docs/docs/como_funciona/exemplos.md`, `tests/` (novo).

---

## 6. Épico D — Interface comum de adaptadores e SP (F3) · 14 h

Objetivo: um contrato único para DOE estaduais, com SP migrado sem regressão.

| ID | Tarefa | Depende de | Est. | Evidência de conclusão |
|---|---|---|---|---|
| D1 | Definir `StateGazetteAdapter` (search/normalize/fetch_by_date/rate_limit_rps) e registry | A1 | 4 h | Interface documentada; registry resolve `DOE:SP` e `DOE:MG` |
| D2 | Encapsular `DOESPHook`/`DOESPSearcher` na interface, sem mudar comportamento | D1 | 4 h | Testes atuais de SP passam sem alteração |
| D3 | Alias `DOESP` ↔ `DOE:SP` e teste de paridade | D2 | 4 h | Mesma data/termos produz o mesmo conjunto |
| D4 | Documentar a fonte SP e o contrato de adapter (para novas UFs) | D2 | 2 h | Página de docs com passo a passo de nova UF |

**Aceite do épico:** `DOESP` continua funcionando; `DOE:SP` é o nome canônico;
adicionar uma UF futura não exige tocar no gerador.

---

## 7. Épico E — API Rodou Clipping v1 (F4) · 46 h

Objetivo: expor os dados em `https://rodou.ninja-apps.work/clipping/v1`, pronta
para o agente de IA.

| ID | Tarefa | Depende de | Est. | Evidência de conclusão |
|---|---|---|---|---|
| E1 | Esqueleto do serviço (`services/clipping_api/`): settings, pool `clipping_ro`, `/clipping/healthz` | B1 | 4 h | `/clipping/healthz` 200 com banco de pé; 503 sem banco |
| E2 | `GET /items` com `date`, `from`/`to`, `source`, `uf`, `q`, `since`, paginação e `sources_failed` | E1, B4 | 6 h | Teste de integração com dados semeados |
| E3 | `GET /items/{id}` | E2 | 1 h | 200 e 404 |
| E4 | `GET /runs`, `GET /runs/{id}` | E1, B4 | 3 h | Status e `sources_failed` corretos |
| E5 | `POST /runs` com `Idempotency-Key`, `202`, `409` e disparo via REST do Airflow | E4 | 4 h | Mesma chave devolve mesmo `run_id`; concorrência 409 |
| E6 | `GET /sources` | E1, D1 | 2 h | Lista fontes, UFs, cadernos e último status |
| E7 | Auth por `X-API-Key`, escopos, rate limit e auditoria (`api_request`) | E1, B1 | 5 h | 401/403/429 cobertos; `key_id` auditado |
| E8 | `ETag`/`If-None-Match`/304, `Cache-Control` e `problem+json` com `allowed` | E2 | 4 h | 304 sem corpo; erro com lista de valores |
| E9 | OpenAPI 3.1 com `operationId` estáveis | E2–E6 | 2 h | `/clipping/openapi.json` válido |
| E10 | Dockerfile, compose de desenvolvimento e healthcheck | E1 | 3 h | Serviço sobe com `docker compose` e responde healthz |
| E11 | Testes unitários e de integração (Postgres efêmero), incluindo 401/403/409/429/304 | E2–E9 | 8 h | Suíte do serviço verde no CI |
| E12 | Documentação pública da API (endpoints, exemplos, limites, auth) | E2–E9 | 4 h | Página de docs + exemplos copiáveis |

**Aceite do épico:** contrato do PRD §10 validado; o agente consegue listar
itens, consultar run e disparar execução com a própria chave.

**Notas do épico:**

- E5 depende de token do Airflow (`AIRFLOW_API_TOKEN`) configurado como secret;
  sem ele, o `POST /runs` deve responder `503` com `detail` explícito, nunca
  `202` sem execução criada.
- Na v1 **não há endpoint de criação de chave**: chaves são criadas por script
  administrativo (SQL/CLI) e guardadas apenas como hash; incluir o script em
  `clipping/sql/` ou `services/clipping_api/scripts/`.
- E11 depende de Postgres disponível no CI; se o pipeline não tiver serviço de
  banco, usar Testcontainers ou um banco efêmero do próprio runner.

**Decisões pendentes que bloqueiam E5/E7:** nenhuma — escopo do agente já
definido (`clipping:read` + `clipping:trigger`).

---

## 8. Épicos F e G — Deploy e ciclo de vida (F5/F6) · 29 h

| ID | Tarefa | Depende de | Est. | Evidência de conclusão |
|---|---|---|---|---|
| F1 | Router Traefik `clipping` com `PathPrefix(/clipping)`, prioridade explícita, TLS e sem `stripPrefix` | E10 | 3 h | `https://rodou.ninja-apps.work/clipping/healthz` 200 e Airflow intacto |
| F2 | Override de produção (Compose) com serviço, secrets e limites de memória | E10, F1 | 3 h | `docker compose config` válido; segredos fora do repositório |
| F3 | Chart Helm do serviço (se o Helm for alvo de produção) | E10 | 4 h | `helm lint` e `helm template` limpos |
| F4 | Observabilidade: logs estruturados, métricas de `304`/`429`, alerta de falha de fonte | E7 | 3 h | Logs e alertas observáveis; sem payload |
| F5 | Smoke test de produção + critérios 16 do PRD + backup documentado | F1–F4 | 3 h | Checklist de produção assinado |
| G1 | Backfill administrativo por período (31 dias, idempotente) | B4, E5 | 6 h | Backfill sem duplicidade |
| G2 | Job de expurgo por retenção (padrão 24 meses) | B4 | 3 h | Expurgo não afeta itens fora da janela |
| G3 | Testes e documentação de backfill/retenção | G1, G2 | 4 h | Testes verdes; runbook atualizado |

---

## 9. Épico H — Servidor MCP (F7, fase 2) · 25 h

Só iniciar após a v1 em produção e o agente consumindo a API.

| ID | Tarefa | Depende de | Est. | Evidência de conclusão |
|---|---|---|---|---|
| H1 | Serviço `services/clipping_mcp/` com as 6 ferramentas sobre a API v1 | E9 | 8 h | Ferramentas respondem iguais ao REST |
| H2 | Auth passthrough (`X-API-Key`/`Bearer`) e enforcement de escopo no `trigger_run` | H1, E7 | 3 h | Sem `clipping:trigger`, trigger falha e nada é criado |
| H3 | Limites de contexto (≤ 50 itens, `abstract` ≤ 1.000) e conteúdo tratado como dado | H1 | 3 h | Teste com título malicioso não executa ação |
| H4 | Transporte Streamable HTTP em `/clipping/mcp` + `stdio` local | H1, F1 | 3 h | Cliente MCP conecta nos dois transportes |
| H5 | Testes: paridade REST, permissão, limites, injeção | H1–H4 | 5 h | Suíte MCP verde |
| H6 | Guia para o dev do agente (ferramentas, exemplos, erros) | H1–H4 | 3 h | Documento entregue ao time do agente |

---

## 10. Sequência sugerida (1 dev)

| Sprint | Conteúdo | Foco |
|---|---|---|
| 1 | A1–A5, B1–B2 | Resiliência + base do banco |
| 2 | B3–B7 | Persistência e rastreabilidade |
| 3 | D1, C1–C2 | Contrato de adapter + cliente MG |
| 4 | C3–C7, D2–D4 | Fonte MG ponta a ponta + migração de SP |
| 5 | E1–E9 | Endpoints, auth e contrato do agente |
| 6 | E10–E12, F1–F5 | Empacotamento e produção |
| 7 | G1–G3 | Backfill e retenção |
| 8 (fase 2) | H1–H6 | MCP |

Buffers: ~20% sobre a soma (158 h → ~190 h, ≈ 24 dias-pessoa com folga). A
ordem interna pode mudar; as dependências, não.

---

## 11. Gates de revisão

- **Gate 1 (fim do épico A):** demonstração de QD fora do ar sem perda do DOU.
- **Gate 2 (fim do épico B):** dupla execução sem duplicidade + restauração de backup.
- **Gate 3 (fim do épico C):** coleta real de MG por 3 dias consecutivos.
- **Gate 4 (fim do épico E):** contrato §10 validado por testes e por uma chamada
  real do agente de IA em homologação.
- **Gate 5 (fim do épico F):** checklist de produção e revisão de segurança.
- **Gate 6 (fase 2):** ciclo diário do agente só com MCP.

Cada gate exige revisão independente (papel "Agente de revisão de código novo"
do `AGENTS.md`) antes de seguir.

---

## 12. Riscos mapeados para tarefas

| Risco | Tarefa de mitigação |
|---|---|
| API de MG mudar | C6 (teste de contrato com fixture) + F4 (alerta) |
| TLS de MG quebrar | C1 + C6 (cenário sem bundle) |
| Falha em cascata | A2 + B4 |
| Duplicidade | B3 + B6 |
| Exposição da API | E7 + E8 + F5 |
| Contexto excessivo no agente | E2/E8 (`size`, `abstract`) + H3 |
| Prompt injection via conteúdo | H3 |
| Custo de armazenamento | B1 (índices) + G2 (retenção) |

---

## 13. Fora deste backlog

- Resumos por IA e provedores (`ai_*`).
- Novas UFs além de SP e MG.
- Frontend/web do Rodou Clipping.
- OIDC/gov.br.
- Migração do INLABS para fonte própria.
