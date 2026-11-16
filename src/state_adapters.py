"""Contrato comum dos adaptadores de DOE estaduais (fase F3).

Cada estado implementa ``StateGazetteAdapter``. O adaptador encapsula o hook
HTTP, expõe os cadernos disponíveis e normaliza o resultado para o formato
``grupo → termo → departamento → itens`` consumido pelo Ro-DOU.

O registry abaixo é a fonte de verdade para:

- ``STATE_ADAPTERS``: classe do adaptador por identificador canônico de fonte;
- ``STATE_SOURCE_SEARCHERS``: searcher do gerador correspondente a cada fonte.

Para adicionar uma UF, veja ``docs/docs/como_contribuir/novas_fontes.md``.
"""

from datetime import date
from typing import ClassVar, Protocol, runtime_checkable


@runtime_checkable
class StateGazetteAdapter(Protocol):
    """Interface de um Diário Oficial estadual."""

    source_id: str
    uf: str
    cadernos: dict[str, str]
    rate_limit_rps: float

    def search(
        self, terms: list[str], reference_date: date, search_date, cadernos=None
    ) -> dict[str, list[dict]]:
        """Pesquisa os termos e devolve itens agrupados por termo."""
        ...

    def normalize(self, raw: dict, term: str) -> dict:
        """Converte um resultado bruto no item padrão do Ro-DOU."""
        ...

    def fetch_by_date(self, day: date) -> list[dict]:
        """Lista publicações de um dia, para backfill.

        Adaptadores cuja API não permita listar por data sem termos devem
        levantar ``NotImplementedError`` com uma mensagem explicativa.
        """
        ...


class DOESPAdapter:
    """Adaptador do Diário Oficial do Estado de São Paulo."""

    source_id = "DOE:SP"
    uf = "SP"
    # Os cadernos do DOE-SP são dinâmicos (SectionDOESP.fetch_sections); o
    # mapeamento por nome é feito no searcher antes da consulta.
    cadernos: ClassVar[dict[str, str]] = {}
    rate_limit_rps = 1.0

    def __init__(self, hook=None):
        from hooks.doesp_hook import DOESPHook

        self.hook = hook or DOESPHook()

    def search(
        self, terms: list[str], reference_date: date, search_date, cadernos=None
    ) -> dict[str, list[dict]]:
        results = self.hook.search_text(
            search_term=terms,
            journals=cadernos,
            reference_date=reference_date,
            search_date=search_date,
        )
        key = ", ".join(terms) if terms else "all_publications"
        return {key: results or []}

    def normalize(self, raw: dict, term: str) -> dict:
        # O DOESPHook já devolve o item no formato do Ro-DOU.
        return raw

    def fetch_by_date(self, day: date) -> list[dict]:
        raise NotImplementedError(
            "DOE-SP: a API exige termos para listar publicações; use search()."
        )


class DOEMGAdapter:
    """Adaptador do Diário Oficial do Estado de Minas Gerais."""

    source_id = "DOE:MG"
    uf = "MG"
    cadernos: ClassVar[dict[str, str]] = {
        "Executivo": "DiarioExecutivo",
        "Municipios": "DiarioMunicipios",
        "Terceiros": "DiarioTerceiros",
        "EdicaoExtra": "EdicaoExtra",
    }
    rate_limit_rps = 1.0

    def __init__(self, hook=None):
        from hooks.doemg_hook import DOEMGHook

        self.hook = hook or DOEMGHook()

    def search(
        self, terms: list[str], reference_date: date, search_date, cadernos=None
    ) -> dict[str, list[dict]]:
        return self.hook.search(
            terms,
            reference_date,
            search_date=search_date,
            cadernos=cadernos,
        )

    def normalize(self, raw: dict, term: str) -> dict:
        return self.hook.normalize(raw, term)

    def fetch_by_date(self, day: date) -> list[dict]:
        """Metadados dos cadernos da edição (sem extrair o texto do PDF)."""
        edition = self.hook.fetch_edition(day)
        date_str = day.strftime("%d/%m/%Y")
        published = edition.get("dataPublicacao") or day.isoformat()

        return [
            {
                "title": caderno.get("descricao") or "DOE-MG",
                "href": self.hook.PORTAL_URL,
                "abstract": "",
                "date": date_str,
                "id": f"{published}:{caderno.get('id')}",
                "section": f"DOE-MG - {caderno.get('descricao') or 'DOE-MG'}",
                "display_date_sortable": published,
                "hierarchyList": [],
                "hierarchyStr": "",
                "arttype": "",
            }
            for caderno in edition.get("cadernos") or []
        ]


STATE_ADAPTERS: dict[str, type] = {
    "DOE:SP": DOESPAdapter,
    "DOE:MG": DOEMGAdapter,
}

#: Searcher do gerador (`DouDigestDagGenerator.searchers`) para cada fonte.
STATE_SOURCE_SEARCHERS: dict[str, str] = {
    "DOE:SP": "DOESP",
    "DOE:MG": "MG",
}


def get_state_adapter(source_id: str) -> StateGazetteAdapter:
    """Instancia o adaptador de uma fonte estadual canônica."""
    try:
        adapter_class = STATE_ADAPTERS[source_id]
    except KeyError as exc:
        raise ValueError(
            f"Fonte estadual desconhecida: {source_id}. "
            f"Fontes disponíveis: {', '.join(STATE_ADAPTERS)}."
        ) from exc

    return adapter_class()
