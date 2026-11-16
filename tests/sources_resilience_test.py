"""Testes de resiliência e validação das fontes de pesquisa (F0).

Cobrem:

- validação de `sources` no schema (valores aceitos, lista vazia e
  combinações mutuamente exclusivas);
- isolamento de falha por fonte em `perform_searches`, com status por fonte;
- compatibilidade do alias `DOESP` com o nome canônico `DOE:SP`;
- fonte aceita no schema mas ainda não implementada (`DOE:MG`).
"""

from types import SimpleNamespace

import pendulum
import pytest
from dags.ro_dou_src.dou_dag_generator import DouDigestDagGenerator
from dags.ro_dou_src.schemas import RoDouConfig, canonical_source
from pydantic import ValidationError

SEARCH_RESULT = {
    "grupo": {"termo": {"single_department": [{"title": "resultado"}]}}
}


def _context():
    return {
        "dag_run": SimpleNamespace(run_type="scheduled"),
        "data_interval_end": pendulum.datetime(2026, 11, 16, 3, 0, tz="UTC"),
    }


def _search_kwargs(**overrides):
    kwargs = {
        "ai_config": None,
        "header": "Teste",
        "sources": ["DOU"],
        "territory_id": None,
        "term_list": ["teste"],
        "dou_sections": ["TODOS"],
        "journals": None,
        "search_date": "DIA",
        "field": "TUDO",
        "is_exact_search": True,
        "ignore_signature_match": False,
        "force_rematch": False,
        "full_text": False,
        "text_length": None,
        "use_summary": False,
        "ignore_attachments": False,
        "ignore_inline_tables": False,
        "min_table_rows": None,
        "ai_search_config": None,
        "show_relevancy": False,
        "result_as_email": True,
        "department": None,
        "department_ignore": None,
        "terms_ignore": None,
        "pubtype": None,
        "excerpt_size": None,
        "number_of_excerpts": None,
    }
    kwargs.update(overrides)
    return kwargs


def _config(sources, terms=("teste",)):
    return {
        "dag": {
            "id": "sources_test",
            "description": "DAG de teste de fontes",
            "search": {"sources": list(sources), "terms": list(terms)},
            "report": {"emails": ["destino@example.com"]},
        }
    }


@pytest.fixture()
def generator():
    return DouDigestDagGenerator()


def test_canonical_source_alias():
    assert canonical_source("DOESP") == "DOE:SP"
    assert canonical_source("DOE:SP") == "DOE:SP"
    assert canonical_source("DOU") == "DOU"


@pytest.mark.parametrize(
    "sources, expected_message",
    [
        (["XPTO"], "Fonte(s) de pesquisa inválida(s): XPTO"),
        (["dou"], "Fonte(s) de pesquisa inválida(s): dou"),
        ([], "A lista 'sources' não pode ser vazia"),
        (["DOU", "INLABS"], "mutuamente exclusivas"),
    ],
)
def test_invalid_sources_are_rejected(sources, expected_message):
    with pytest.raises(ValidationError) as excinfo:
        RoDouConfig(**_config(sources))

    assert expected_message in str(excinfo.value)


@pytest.mark.parametrize(
    "sources",
    [
        ["DOU"],
        ["QD"],
        ["INLABS"],
        ["DOESP"],
        ["DOE:SP"],
        ["DOU", "QD"],
        ["DOE:SP", "QD"],
    ],
)
def test_accepted_sources(sources):
    assert isinstance(RoDouConfig(**_config(sources)), RoDouConfig)


def test_source_failure_is_isolated(generator, mocker):
    generator.searchers["DOU"] = mocker.Mock()
    generator.searchers["DOU"].exec_search.return_value = SEARCH_RESULT
    generator.searchers["QD"] = mocker.Mock()
    generator.searchers["QD"].exec_search.side_effect = RuntimeError("HTTP 503")

    output = generator.perform_searches(
        **_search_kwargs(sources=["DOU", "QD"]), **_context()
    )

    assert output["result"] == SEARCH_RESULT
    assert output["sources_status"]["DOU"] == {"status": "ok", "error": None}
    assert output["sources_status"]["QD"]["status"] == "failed"
    assert "HTTP 503" in output["sources_status"]["QD"]["error"]


def test_all_sources_failing_raises(generator, mocker):
    for name in ("DOU", "QD"):
        searcher = mocker.Mock()
        searcher.exec_search.side_effect = ValueError("indisponível")
        generator.searchers[name] = searcher

    with pytest.raises(RuntimeError) as excinfo:
        generator.perform_searches(
            **_search_kwargs(sources=["DOU", "QD"]), **_context()
        )

    message = str(excinfo.value)
    assert "Nenhuma fonte concluiu a busca" in message
    assert "DOU" in message and "QD" in message


def test_empty_source_is_reported(generator, mocker):
    generator.searchers["DOU"] = mocker.Mock()
    generator.searchers["DOU"].exec_search.return_value = {}

    output = generator.perform_searches(**_search_kwargs(sources=["DOU"]), **_context())

    assert output["result"] == {}
    assert output["sources_status"]["DOU"] == {"status": "empty", "error": None}


def test_doe_sp_alias_uses_sp_searcher(generator, mocker):
    sp_searcher = mocker.Mock()
    sp_searcher.exec_search.return_value = SEARCH_RESULT
    generator.searchers["DOESP"] = sp_searcher

    output = generator.perform_searches(
        **_search_kwargs(sources=["DOE:SP"]), **_context()
    )

    sp_searcher.exec_search.assert_called_once()
    assert output["sources_status"] == {"DOE:SP": {"status": "ok", "error": None}}
    assert output["result"] == SEARCH_RESULT


def test_legacy_doesp_is_canonicalized(generator, mocker):
    sp_searcher = mocker.Mock()
    sp_searcher.exec_search.return_value = SEARCH_RESULT
    generator.searchers["DOESP"] = sp_searcher

    output = generator.perform_searches(
        **_search_kwargs(sources=["DOESP"]), **_context()
    )

    assert "DOE:SP" in output["sources_status"]
    assert "DOESP" not in output["sources_status"]


def test_unimplemented_source_fails_with_clear_message(generator, mocker):
    generator.searchers["DOU"] = mocker.Mock()
    generator.searchers["DOU"].exec_search.return_value = SEARCH_RESULT

    with pytest.raises(ValueError) as excinfo:
        generator.perform_searches(
            **_search_kwargs(sources=["DOU", "DOE:MG"]), **_context()
        )

    message = str(excinfo.value)
    assert "DOE:MG" in message
    assert "fase F2" in message
