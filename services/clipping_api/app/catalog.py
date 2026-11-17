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


def validate_sources(sources: list[str]) -> list[str]:
    """Valida e normaliza a lista de fontes informada na query/body."""
    invalid = [source for source in sources if source not in SOURCES]
    if invalid:
        raise ValueError(
            f"Fonte(s) inválida(s): {', '.join(invalid)}. "
            f"Valores aceitos: {', '.join(SOURCE_NAMES)}."
        )
    return sources
