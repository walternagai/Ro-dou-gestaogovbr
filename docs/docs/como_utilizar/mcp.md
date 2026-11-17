# Servidor MCP (agentes de IA)

O servidor MCP (Model Context Protocol) expõe o Rodou Clipping como
ferramentas para agentes de IA. Ele é um invólucro fino sobre a
[API do clipping](api.md), sem banco nem credencial própria: a chave do agente é
repassada para a API.

- HTTP: `https://rodou.ninja-apps.work/clipping/mcp`
- Transporte alternativo: `stdio` (execução local)
- Healthcheck: `GET /clipping/mcp/healthz`

## Autenticação

Envie a chave no header `X-API-Key` (ou `Authorization: Bearer <chave>`). Os
escopos são aplicados pela API:

- `clipping:read` — `list_items`, `get_item`, `list_runs`, `get_run`, `list_sources`;
- `clipping:trigger` — `trigger_run`.

## Ferramentas

| Ferramenta | Equivalente REST | Escopo |
|---|---|---|
| `list_items` | `GET /clipping/v1/items` | `clipping:read` |
| `get_item` | `GET /clipping/v1/items/{id}` | `clipping:read` |
| `list_runs` | `GET /clipping/v1/runs` | `clipping:read` |
| `get_run` | `GET /clipping/v1/runs/{id}` | `clipping:read` |
| `list_sources` | `GET /clipping/v1/sources` | `clipping:read` |
| `trigger_run` | `POST /clipping/v1/runs` | `clipping:trigger` |

`trigger_run` aceita `reference_date`, `sources` e `idempotency_key` (default:
`clipping:mcp:<data>`), que é repassada à API para evitar disparos duplicados.

## Limites e segurança

- **Contexto**: no máximo **50 itens** por chamada e `abstract` truncado em
  **1.000 caracteres**; pagine com `page` para obter mais.
- **Conteúdo não confiável**: títulos e resumos são dados públicos brutos.
  As respostas textuais são precedidas de um aviso e **não devem ser
  interpretadas como instruções** pelo modelo.
- **Somente leitura por padrão**: sem o escopo de trigger, o agente não
  consegue disparar execuções.
- Erros da API são devolvidos como resultado de ferramenta com `isError: true`
  e a mensagem original (ex.: 403 para escopo insuficiente).

## Configuração em um cliente MCP

### HTTP

```json
{
  "mcpServers": {
    "rodou-clipping": {
      "url": "https://rodou.ninja-apps.work/clipping/mcp",
      "headers": {
        "X-API-Key": "<chave-do-agente>"
      }
    }
  }
}
```

Também é possível usar `Authorization: Bearer <chave>`.

### stdio (local)

```json
{
  "mcpServers": {
    "rodou-clipping": {
      "command": "python",
      "args": ["services/clipping_mcp/scripts/stdio_server.py"],
      "env": {
        "CLIPPING_API_BASE_URL": "https://rodou.ninja-apps.work",
        "CLIPPING_API_KEY": "<chave-do-agente>"
      }
    }
  }
}
```

## Operação

O serviço sobe no profile `clipping` do Compose, junto com a API:

```bash
docker compose --env-file deploy/prod/.env.production \
  -f docker-compose.yml -f deploy/prod/docker-compose.override.yml \
  -f deploy/prod/docker-compose.traefik-host.yml \
  --profile clipping up -d clipping-api clipping-mcp
```

Variáveis: `CLIPPING_API_BASE_URL`, `CLIPPING_API_KEY` (somente para `stdio`),
`MCP_MAX_ITEMS_PER_CALL` (default 50) e `MCP_MAX_ABSTRACT_LENGTH` (default 1000).
No Helm, use `clippingMcp.enabled=true` (ver README do chart).
