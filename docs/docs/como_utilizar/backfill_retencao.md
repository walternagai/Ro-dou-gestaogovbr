# Backfill e retenção

Rotinas administrativas do Rodou Clipping: recoletar um período passado e
expurgar dados antigos. Ambas são **operacionais** e não fazem parte da API
consumida pelo agente.

## Backfill de um período

O script dispara uma execução manual da DAG para cada dia do intervalo, usando
a API REST do Airflow. Ele existe para recuperar dias em que a coleta falhou ou
para carregar um período sem esperar o agendamento diário.

```bash
docker compose \
  --env-file deploy/prod/.env.production \
  -f docker-compose.yml \
  -f deploy/prod/docker-compose.override.yml \
  --profile clipping \
  exec -T clipping-api python scripts/backfill.py \
    --from 2026-10-01 --to 2026-10-31
```

Regras:

- a janela máxima é de **31 dias**;
- sábados e domingos são pulados por padrão; use `--include-weekends` para
  incluí-los;
- `--sources` (repetível) restringe as fontes; sem ele, vale a configuração da
  DAG;
- `--dry-run` mostra o que seria disparado, sem chamar o Airflow;
- `--interval` (default 2 s) espaça os disparos.

Antes de disparar, o script consulta as execuções existentes daquele dia e
ignora datas que já têm run. Além disso, a persistência faz **upsert** pelo
`id = sha256(source|uf|external_id)`, então repetir um dia não duplica itens.

As execuções de backfill enviam `conf.backfill=true`, e a DAG **suprime as
notificações** nesse caso — o resultado é persistido, mas nenhum e-mail, Slack
ou Discord é enviado para datas antigas.

Variáveis usadas: `AIRFLOW_API_URL`, `AIRFLOW_API_TOKEN` e `CLIPPING_DAG_ID`
(as mesmas da API).

## Expurgo por retenção

O script remove dados mais antigos que a janela configurada, em lotes:

```bash
docker compose \
  --env-file deploy/prod/.env.production \
  -f docker-compose.yml \
  -f deploy/prod/docker-compose.override.yml \
  --profile clipping \
  exec -T clipping-api python scripts/purge.py --months 24 --dry-run
```

Sem `--dry-run`, o script:

1. remove itens com `published_date` anterior a `CURRENT_DATE - 24 meses`, em
   lotes de 1.000;
2. remove execuções finalizadas antes do corte que ficaram sem itens;
3. remove registros de auditoria (`clipping.api_request`) com mais de 90 dias;
4. opcionalmente executa `VACUUM ANALYZE clipping.item` com `--vacuum`.

Opções: `--months` (default 24), `--api-request-days` (default 90),
`--batch-size` (default 1000) e `--dsn` (default `CLIPPING_DB_URL`).

**Faça backup antes** do primeiro expurgo e confirme o resultado do `--dry-run`.
Chaves de API e itens dentro da janela não são afetados.

### CronJob no Kubernetes

Com o chart Helm, habilite o CronJob mensal:

```yaml
clippingRetention:
  enabled: true
  schedule: "0 3 1 * *"   # dia 1º, 03:00
  months: 24
  apiRequestDays: 90
```

O Job usa a mesma imagem e o mesmo Secret da API (`clipping-api`), portanto
requer `clippingApi.enabled=true` ou que o Secret já exista no release.
