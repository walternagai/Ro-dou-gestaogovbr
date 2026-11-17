# Rodou Clipping MCP

Servidor [Model Context Protocol](https://modelcontextprotocol.io/) que expõe o
Rodou Clipping como ferramentas para agentes de IA.

- Endpoint HTTP: `https://rodou.ninja-apps.work/clipping/mcp`
- Healthcheck: `/clipping/mcp/healthz`
- Transporte local: `stdio` via `scripts/stdio_server.py`
- Autenticação: a chave do agente é repassada como `X-API-Key` ou
  `Authorization: Bearer` para a API do clipping; escopos
  `clipping:read`/`clipping:trigger` são aplicados pela API.

## Ferramentas

| Ferramenta | Equivalente REST | Escopo |
|---|---|---|
| `list_items` | `GET /clipping/v1/items` | `clipping:read` |
| `get_item` | `GET /clipping/v1/items/{id}` | `clipping:read` |
| `list_runs` | `GET /clipping/v1/runs` | `clipping:read` |
| `get_run` | `GET /clipping/v1/runs/{id}` | `clipping:read` |
| `list_sources` | `GET /clipping/v1/sources` | `clipping:read` |
| `trigger_run` | `POST /clipping/v1/runs` | `clipping:trigger` |

Limites de contexto: no máximo 50 itens por resposta e `abstract` truncado em
1.000 caracteres. Todo conteúdo textual é devolvido como **dado**, precedido de
um aviso de segurança, para mitigar prompt injection.

## Configuração

| Variável | Default | Descrição |
|---|---|---|
| `CLIPPING_API_BASE_URL` | `http://clipping-api:8000` | base da API do clipping |
| `CLIPPING_API_KEY` | vazio | chave padrão para o modo `stdio` |
| `MCP_MAX_ITEMS_PER_CALL` | `50` | teto de itens por chamada |
| `MCP_MAX_ABSTRACT_LENGTH` | `1000` | truncamento do abstract |

## Execução

```bash
docker build -t ghcr.io/gestaogovbr/ro-dou-clipping-mcp:<tag> services/clipping_mcp
docker run --rm -p 8082:8000 \
  -e CLIPPING_API_BASE_URL=http://host.docker.internal:8001 \
  ghcr.io/gestaogovbr/ro-dou-clipping-mcp:<tag>
```

Modo local (`stdio`):

```bash
CLIPPING_API_BASE_URL=https://rodou.ninja-apps.work \
CLIPPING_API_KEY=<chave> \
  python scripts/stdio_server.py
```

## Testes

```bash
cd services/clipping_mcp && pytest -q tests
```
