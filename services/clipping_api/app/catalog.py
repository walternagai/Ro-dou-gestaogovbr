"""Catálogo estático das fontes expostas pela API.

Mantido em sincronia com ``src/state_adapters.py`` (teste de paridade em
``tests/test_units.py``). O catálogo evita importar Airflow na API.
"""

from typing import Literal

SourceId = Literal["DOU", "QD", "INLABS", "DOE:SP", "DOE:MG"]
UfCode = Literal["MG", "SP"]
RunStatus = Literal["queued", "running", "success", "partial", "failed"]
SourceLastStatus = Literal["ok", "empty", "failed"]

SOURCES: dict[str, dict] = {
    "DOU": {
        "source": "DOU",
        "uf": None,
        "cadernos": ["SECAO_1", "SECAO_2", "SECAO_3", "TODOS"],
        "rate_limit_rps": 1.0,
    },
    "QD": {
        "source": "QD",
        "uf": None,
        "cadernos": [],
        "rate_limit_rps": 1.0,
    },
    "INLABS": {
        "source": "INLABS",
        "uf": None,
        "cadernos": [],
        "rate_limit_rps": 1.0,
    },
    "DOE:SP": {
        "source": "DOE:SP",
        "uf": "SP",
        "cadernos": ["Executivo"],
        "rate_limit_rps": 1.0,
    },
    "DOE:MG": {
        "source": "DOE:MG",
        "uf": "MG",
        "cadernos": ["Executivo", "Municipios", "Terceiros", "EdicaoExtra"],
        "rate_limit_rps": 1.0,
    },
}

SOURCE_NAMES = tuple(SOURCES)
SUPPORTED_UFS = tuple(
    sorted({entry["uf"] for entry in SOURCES.values() if entry["uf"]})
)


def sources_for_uf(
    uf: str, allowed_sources: tuple[str, ...] | list[str] | None = None
) -> list[str]:
    """Retorna as fontes associadas à UF informada."""
    pool = allowed_sources if allowed_sources is not None else SOURCE_NAMES
    return [
        source
        for source in pool
        if source in SOURCES and SOURCES[source].get("uf") == uf
    ]


def enabled_ufs(
    allowed_sources: tuple[str, ...] | list[str] | None = None,
) -> tuple[str, ...]:
    """Retorna as UFs atendidas pelo subconjunto de fontes habilitadas."""
    pool = allowed_sources if allowed_sources is not None else SOURCE_NAMES
    return tuple(
        sorted(
            {
                SOURCES[source]["uf"]
                for source in pool
                if source in SOURCES and SOURCES[source].get("uf")
            }
        )
    )


def validate_sources(
    sources: list[str],
    allowed_sources: tuple[str, ...] | list[str] | None = None,
) -> list[str]:
    """Valida e normaliza a lista de fontes informada na query/body."""
    pool = tuple(allowed_sources) if allowed_sources is not None else SOURCE_NAMES
    invalid = [source for source in sources if source not in pool]
    if invalid:
        raise ValueError(
            f"Fonte(s) inválida(s): {', '.join(invalid)}. "
            f"Valores aceitos: {', '.join(pool)}."
        )
    return sources
