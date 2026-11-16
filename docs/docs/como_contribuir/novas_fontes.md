# Adicionar uma nova fonte estadual (DOE)

O Ro-DOU já suporta o Diário Oficial do Estado de São Paulo (`DOE:SP`) e o de
Minas Gerais (`DOE:MG`). Uma nova unidade federativa entra como um **adaptador
estadual**, sem alterar o contrato dos YAMLs existentes.

## Contrato

Toda fonte estadual implementa `StateGazetteAdapter`, definido em
`src/state_adapters.py`:

```python
class StateGazetteAdapter(Protocol):
    source_id: str              # "DOE:UF" (nome canônico)
    uf: str                     # "UF"
    cadernos: dict[str, str]    # nome exibido -> identificador na API
    rate_limit_rps: float       # requisições por segundo

    def search(self, terms, reference_date, search_date, cadernos=None):
        """Retorna {termo: [itens já normalizados]}."""

    def normalize(self, raw, term) -> dict:
        """Converte um resultado bruto no item padrão do Ro-DOU."""

    def fetch_by_date(self, day) -> list[dict]:
        """Lista publicações de um dia (backfill) ou levanta NotImplementedError."""
```

O item normalizado usa as chaves consumidas pelos templates e pelo CSV:

```python
{
    "title": "...",
    "href": "https://...",
    "abstract": "...",
    "date": "dd/mm/aaaa",
    "section": "DOE-UF - Caderno",
    "display_date_sortable": "AAAA-MM-DDTHH:MM:SS",
    "hierarchyList": [],
    "hierarchyStr": "",
    "arttype": "",
}
```

## Passo a passo

1. **Hook** (`src/hooks/<uf>_hook.py`): cliente HTTP da API oficial. Defina
   timeout de conexão/leitura, retry com backoff apenas para falhas temporárias
   (429/5xx e erros de rede), limite de requisições e `User-Agent`
   identificável. Verificação TLS **sempre habilitada**; se o portal tiver
   cadeia incompleta, versione o intermediário e monte um bundle (ver
   `hooks/certs/` e `DOEMGHook._ca_bundle_path`).

2. **Adaptador** (`src/state_adapters.py`): envolva o hook e implemente
   `search`, `normalize` e `fetch_by_date`. Não coloque regras de agrupamento ou
   de template no adaptador. Se a API não permitir listar por data sem termos,
   `fetch_by_date` deve levantar `NotImplementedError` com explicação.

3. **Searcher** (`src/searchers.py`): subclasse de `BaseSearcher` com a mesma
   assinatura usada pelo gerador (`term_list`, `journals`, `search_date`,
   `department`, `department_ignore`, `terms_ignore`, `pubtype`,
   `reference_date`). Delegue ao adaptador e use `self._group_results(...)`.

4. **Registry**: registre a classe em `STATE_ADAPTERS` e o nome do searcher em
   `STATE_SOURCE_SEARCHERS`. O gerador monta `_SEARCHER_BY_SOURCE` a partir
   desse mapa, então não é preciso editar o gerador para cada UF. Instancie o
   searcher no dicionário `DouDigestDagGenerator.searchers` com a chave indicada
   em `STATE_SOURCE_SEARCHERS`.

5. **Schema**: adicione o nome canônico (`DOE:UF`) a `SUPPORTED_SOURCES` em
   `src/schemas.py`. Se houver um nome legado, registre o alias em
   `SOURCE_ALIASES` — o alias `DOESP` para `DOE:SP` é mantido por
   compatibilidade. A validação rejeita valores desconhecidos, listas vazias e
   combinações mutuamente exclusivas.

6. **Exemplo e documentação**: crie um YAML em
   `dag_confs/examples_and_tests/` (use `git add -f`, pois `**/dag_confs/` está
   no `.gitignore`), acrescente o exemplo em `como_funciona/exemplos.md` e
   atualize `como_funciona/parametros.md` com as fontes, cadernos e limitações.

7. **Testes**: cubra paginação, dia sem publicação, 429/5xx, 401, cache,
   agrupamento e o bundle TLS com `requests` mockado. A suíte não pode depender
   de rede nem de credenciais. Inclua um teste de paridade entre o nome canônico
   e o alias, quando existir.

## Regras de compatibilidade

- **Não altere** o formato `grupo → termo → departamento → itens`; templates,
  CSV e notificadores dependem dele.
- **Filtros não suportados** pela API (por exemplo `department`/`pubtype` no
  DOE-MG) devem ser ignorados com aviso no log — nunca aplicados parcialmente
  nem silenciados.
- **Falha de fonte** é registrada em `sources_status` e não impede as demais
  (ver `perform_searches`); se todas falharem, a execução falha.
- **Paginação** tem teto por termo para evitar execuções infinitas; documente o
  limite no hook.
- **Sem `verify=False`**, `shell=True` ou segredos em log.

## Exemplos de referência

- São Paulo: `DOESPAdapter` + `DOESPHook` + `DOESPSearcher`.
- Minas Gerais: `DOEMGAdapter` + `DOEMGHook` + `MGSearcher`, incluindo bundle
  TLS e autenticação anônima.
