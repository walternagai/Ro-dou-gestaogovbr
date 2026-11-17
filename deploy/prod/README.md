# Ro-DOU em produção com Docker Compose

Este diretório contém um override de produção para o `docker-compose.yml` da
raiz. Ele **não é carregado automaticamente** pelo `make run`, porque fica em
um subdiretório. O ambiente local continua usando o compose de desenvolvimento.
**Não use `make run` em produção:** ele sobe o compose de desenvolvimento, com
portas públicas e smtp4dev. Sempre informe os dois `-f` nos comandos abaixo;
sem o override, o compose da raiz publica as portas no host.

O override:

- remove a publicação pública de PostgreSQL (5432), OpenSearch (9200) e
  smtp4dev (25, 143 e 5001);
- publica o Airflow apenas em `127.0.0.1:8080`;
- exige segredos fortes no `.env.production`;
- desativa o smtp4dev no perfil padrão e usa o SMTP real;
- usa o código embutido na imagem, sem montar `src`, `tests` ou `tools` do host;
- monta `dag_confs/` como somente leitura;
- isola PostgreSQL e OpenSearch em uma rede interna sem saída para a internet;
- adiciona limites de memória e rotação de logs do Docker.

## Pré-requisitos

- Docker Compose 2.24 ou superior (as tags `!override` são usadas para
  substituir as portas do compose base).
- `vm.max_map_count` de pelo menos 262144 se o OpenSearch for usado.
- Diretórios de dados com permissão adequada.

## 1. Configurar segredos

```bash
cp deploy/prod/.env.production.example deploy/prod/.env.production
chmod 600 deploy/prod/.env.production
```

Gere os valores:

```bash
# Chave Fernet do Airflow
python3 -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"

# Demais segredos
openssl rand -hex 32
openssl rand -hex 24   # senha do banco, URL-safe
```

Preencha no mínimo:

- `AIRFLOW_FERNET_KEY`
- `AIRFLOW_WEBSERVER_SECRET_KEY`
- `AIRFLOW_API_JWT_SECRET`
- `AIRFLOW_API_SECRET_KEY`
- `AIRFLOW_DB_PASSWORD`
- `AIRFLOW_ADMIN_PASSWORD`
- `SMTP_HOST`, `SMTP_MAIL_FROM` e `AIRFLOW_CONN_SMTP_DEFAULT`
- `OPENSEARCH_PASS`

Não versione `.env.production`.

> **Banco já existente:** `AIRFLOW_DB_PASSWORD` não altera a senha de um
> volume PostgreSQL já inicializado, e trocar `AIRFLOW_FERNET_KEY` pode tornar
> ilegíveis conexões/variáveis criptografadas. Em uma instalação nova, use um
> `mnt/pgdata` vazio. Em uma migração, altere a senha do banco via SQL e
> mantenha a chave Fernet antiga.

## 2. Preparar os diretórios

```bash
mkdir -p mnt/airflow-logs mnt/pgdata
sudo chown -R 50000:0 mnt/airflow-logs
sudo chmod 750 mnt/airflow-logs
```

O container do Airflow roda com UID 50000 por padrão. Se você alterar
`AIRFLOW_UID`, ajuste o `chown` correspondente.

## 3. Validar a configuração

```bash
docker compose \
  --env-file deploy/prod/.env.production \
  -f docker-compose.yml \
  -f deploy/prod/docker-compose.override.yml \
  config >/dev/null
```

Se o comando falhar, confira as variáveis obrigatórias e a versão do Compose.

## 4. Subir a stack

```bash
docker compose \
  --env-file deploy/prod/.env.production \
  -f docker-compose.yml \
  -f deploy/prod/docker-compose.override.yml \
  up -d --build
```

Verifique:

```bash
docker compose \
  --env-file deploy/prod/.env.production \
  -f docker-compose.yml \
  -f deploy/prod/docker-compose.override.yml \
  ps

curl -fsS http://127.0.0.1:8080/api/v2/monitor/health
```

Acesse `http://127.0.0.1:8080` por um túnel SSH ou configure o Traefik.

As DAGs sobem pausadas por causa de `AIRFLOW_DAGS_PAUSED=true`. Ative apenas as
que já foram revisadas. O diretório `dag_confs/` é montado como somente leitura:
mantenha nele apenas os YAMLs de produção e gere novos arquivos fora do
servidor.

## 5. Publicar com Traefik (opcional)

Descubra o nome exato da rede do Traefik:

```bash
docker network ls
```

Defina `TRAEFIK_NETWORK`, `RO_DOU_HOST`, `TRAEFIK_ENTRYPOINT` e
`TRAEFIK_CERT_RESOLVER` no `.env.production`. Depois:

```bash
docker compose \
  --env-file deploy/prod/.env.production \
  -f docker-compose.yml \
  -f deploy/prod/docker-compose.override.yml \
  -f deploy/prod/docker-compose.traefik.yml \
  up -d
```

O arquivo do Traefik remove a publicação em `127.0.0.1:8080` e adiciona o
serviço à rede externa. Ajuste as labels se o seu Traefik usar outro
entrypoint, certificado manual ou middleware de autenticação.

## 6. Configuração pós-subida

- Crie as conexões `inlabs_db` e `inlabs_portal` no Airflow, se for usar o
  INLABS.
- Crie a variável `email_admin` se quiser receber falhas por e-mail.
- Se for usar o INLABS no modo SQL, crie a base e o schema:

```bash
docker compose \
  --env-file deploy/prod/.env.production \
  -f docker-compose.yml \
  -f deploy/prod/docker-compose.override.yml \
  exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -f /sql/init-db.sql'
```

O `inlabs_db` deve apontar para o host `postgres`, porta `5432`, banco
`inlabs` (ou o valor configurado) e usuário/senha do banco. O `inlabs_portal`
é uma conexão HTTP para `https://inlabs.in.gov.br/` com as credenciais do
portal.

### Persistência do clipping (fase F1)

Opcional: sem a conexão `clipping_db`, as DAGs funcionam como antes e apenas
registram um aviso no log. Para habilitar:

```bash
docker compose \
  --env-file deploy/prod/.env.production \
  -f docker-compose.yml \
  -f deploy/prod/docker-compose.override.yml \
  exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d postgres -f /clipping-sql/init-db.sql'
```

> O diretório `clipping/sql` é montado em `/clipping-sql` no serviço
> `postgres`. Em uma instalação já em execução, recrie apenas o serviço para
> o mount aparecer: `docker compose ... up -d postgres` (o volume de dados é
> preservado).

O script cria o banco `clipping`, o schema, as tabelas e as roles
`clipping_rw`/`clipping_ro` (sem login nem senha). Defina as senhas fora do
repositório quando a API entrar em operação:

```sql
ALTER ROLE clipping_ro LOGIN PASSWORD '<senha-forte>';
ALTER ROLE clipping_rw LOGIN PASSWORD '<senha-forte>';
```

Depois, defina `AIRFLOW_CONN_CLIPPING_DB` no `.env.production` apontando para
o banco `clipping` e recrie os serviços do Airflow. O usuário do banco do
Airflow já tem permissão de escrita; `clipping_rw` é a alternativa de menor
privilégio.

### API do clipping (fase F4)

Opcional, sob o profile `clipping` do Compose. Depende do schema do clipping já
aplicado e de um token da API do Airflow para o disparo sob demanda.

```bash
# 1. crie uma chave para o consumidor (o valor aparece uma única vez)
docker compose \
  --env-file deploy/prod/.env.production \
  -f docker-compose.yml \
  -f deploy/prod/docker-compose.override.yml \
  exec -T clipping-api python scripts/create_key.py \
    --label "agente-ia" --scope clipping:read --scope clipping:trigger

# 2. defina AIRFLOW_API_TOKEN e, se quiser, CLIPPING_DAG_ID no .env.production
# 3. suba o serviço (junto com o Traefik em uso)
docker compose \
  --env-file deploy/prod/.env.production \
  -f docker-compose.yml \
  -f deploy/prod/docker-compose.override.yml \
  -f deploy/prod/docker-compose.traefik-host.yml \
  --profile clipping up -d clipping-api

curl "https://SEU_HOST/clipping/healthz"
```

A API escuta em `/clipping` (router Traefik com prioridade maior que o do
Airflow) e usa:

- `CLIPPING_DB_URL` — conexão com o banco `clipping`; por padrão o Compose usa
  o usuário do banco do Airflow. Para a role de menor privilégio, defina
  `CLIPPING_DB_URL` no `.env.production` com a conexão de `clipping_ro`
  (leitura dos dados e `INSERT` apenas em `clipping.api_request`, para a
  auditoria);
- `AIRFLOW_API_TOKEN` — token com permissão de disparar DAGs;
- `CLIPPING_DAG_ID` — DAG disparada por `POST /clipping/v1/runs` (default
  `rodou_clipping`).

### Servidor MCP

O servidor MCP expõe as mesmas ferramentas para agentes de IA em
`https://<host>/clipping/mcp`, com transporte HTTP (JSON-RPC/SSE) e `stdio`
local. Ele sobe no mesmo profile `clipping`:

```bash
docker compose \
  --env-file deploy/prod/.env.production \
  -f docker-compose.yml \
  -f deploy/prod/docker-compose.override.yml \
  -f deploy/prod/docker-compose.traefik-host.yml \
  --profile clipping up -d clipping-api clipping-mcp
```

O MCP não tem banco nem credencial própria: repassa `X-API-Key` (ou
`Authorization: Bearer`) para a API. Para uso local em `stdio`, defina
`CLIPPING_API_BASE_URL` e `CLIPPING_API_KEY` e execute
`python services/clipping_mcp/scripts/stdio_server.py`.

A referência dos endpoints está em
[API do clipping](../../docs/docs/como_utilizar/api.md). O
[checklist de produção](CHECKLIST.md) cobre pré-requisitos, roteamento,
autenticação, observabilidade, backup e rollback.

Rotinas administrativas (backfill e expurgo) estão em
[Backfill e retenção](../../docs/docs/como_utilizar/backfill_retencao.md). No
Kubernetes, o chart pode criar o CronJob mensal com
`clippingRetention.enabled=true`.

## 7. Backup e atualização

Backup lógico do banco do Airflow:

```bash
docker compose \
  --env-file deploy/prod/.env.production \
  -f docker-compose.yml \
  -f deploy/prod/docker-compose.override.yml \
  exec -T postgres sh -c 'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  | gzip > airflow-$(date +%F).sql.gz
```

Se a persistência do clipping estiver habilitada, faça também o dump do banco
`clipping`:

```bash
docker compose \
  --env-file deploy/prod/.env.production \
  -f docker-compose.yml \
  -f deploy/prod/docker-compose.override.yml \
  exec -T postgres sh -c 'pg_dump -U "$POSTGRES_USER" -d clipping' \
  | gzip > clipping-$(date +%F).sql.gz
```

Retenção: o schema não expurga dados automaticamente. A política prevista é de
24 meses; até o job de expurgo (fase F6) entrar em operação, remova itens
antigos manualmente em lotes por `published_date`, sempre depois do backup.

Também proteja `mnt/airflow-logs` e, se usar OpenSearch, o volume
`opensearch-data`. Não copie `mnt/pgdata` com o PostgreSQL em execução.

Atualização:

```bash
git pull
docker compose \
  --env-file deploy/prod/.env.production \
  -f docker-compose.yml \
  -f deploy/prod/docker-compose.override.yml \
  build --pull
docker compose \
  --env-file deploy/prod/.env.production \
  -f docker-compose.yml \
  -f deploy/prod/docker-compose.override.yml \
  up -d
```

## Observações de segurança

- `AIRFLOW_ADMIN_PASSWORD` só é aplicado quando o banco é inicializado pela
  primeira vez. Se o banco já existe, redefina a senha:

  ```bash
  docker compose \
    --env-file deploy/prod/.env.production \
    -f docker-compose.yml \
    -f deploy/prod/docker-compose.override.yml \
    exec airflow-api-server \
    airflow users reset-password --username admin
  ```

- O OpenSearch do compose base mantém `DISABLE_SECURITY_PLUGIN=true`. Neste
  override ele não publica porta, mas continua sem TLS/autenticação. Para
  produção mais exigente, use um OpenSearch externo com TLS ou configure a
  segurança do OpenSearch.
- O container do OpenSearch sobe mesmo com
  `RO_DOU_INLABS_USE_OPENSEARCH=false`, porque o compose base o declara como
  dependência dos serviços do Airflow. Ele fica isolado e limitado a 3 GB por
  padrão. Para não subir, seria necessário um compose próprio sem esse serviço
  e sem o `depends_on`.
- Docker publica portas contornando regras do UFW. Mesmo com o UFW ativo,
  mantenha as portas em `127.0.0.1` ou use o firewall do provedor.
- O `smtp4dev` só sobe com `--profile dev`; em produção use o SMTP real.
- Fixe a versão do OpenSearch depois de validar; `OPENSEARCH_VERSION=2` não é
  uma versão imutável.
