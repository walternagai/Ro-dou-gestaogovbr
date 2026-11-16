"""Testes do contrato e do registry de adaptadores estaduais (fase F3)."""

from datetime import date
from unittest import mock

import pytest
from dags.ro_dou_src.state_adapters import (
    STATE_ADAPTERS,
    STATE_SOURCE_SEARCHERS,
    DOEMGAdapter,
    DOESPAdapter,
    StateGazetteAdapter,
    get_state_adapter,
)

REFERENCE_DATE = date(2026, 11, 14)


def test_registry_resolves_state_sources():
    assert get_state_adapter("DOE:SP").source_id == "DOE:SP"
    assert get_state_adapter("DOE:MG").source_id == "DOE:MG"

    with pytest.raises(ValueError) as excinfo:
        get_state_adapter("DOE:XX")

    assert "DOE:XX" in str(excinfo.value)
    assert "DOE:SP" in str(excinfo.value)


def test_adapters_follow_protocol():
    assert isinstance(DOESPAdapter(hook=mock.Mock()), StateGazetteAdapter)
    assert isinstance(DOEMGAdapter(hook=mock.Mock()), StateGazetteAdapter)


def test_registry_maps_sources_to_searchers():
    assert STATE_SOURCE_SEARCHERS == {"DOE:SP": "DOESP", "DOE:MG": "MG"}
    assert set(STATE_ADAPTERS) == set(STATE_SOURCE_SEARCHERS)


def test_dosp_adapter_search_returns_single_key():
    hook = mock.Mock()
    hook.search_text.return_value = [{"title": "x"}]
    adapter = DOESPAdapter(hook=hook)

    result = adapter.search(
        ["a", "b"], REFERENCE_DATE, search_date="DIA", cadernos=["Executivo"]
    )

    assert result == {"a, b": [{"title": "x"}]}
    hook.search_text.assert_called_once_with(
        search_term=["a", "b"],
        journals=["Executivo"],
        reference_date=REFERENCE_DATE,
        search_date="DIA",
    )


def test_dosp_adapter_normalize_is_identity():
    raw = {"title": "x"}
    assert DOESPAdapter(hook=mock.Mock()).normalize(raw, "x") is raw


def test_dosp_fetch_by_date_is_not_supported():
    with pytest.raises(NotImplementedError):
        DOESPAdapter(hook=mock.Mock()).fetch_by_date(REFERENCE_DATE)


def test_mg_adapter_delegates_search_and_normalize():
    hook = mock.Mock()
    hook.search.return_value = {"x": [{"title": "t"}]}
    hook.normalize.return_value = {"title": "normalizado"}
    adapter = DOEMGAdapter(hook=hook)

    assert adapter.search(["x"], REFERENCE_DATE, "DIA", ["Executivo"]) == {
        "x": [{"title": "t"}]
    }
    hook.search.assert_called_once_with(
        ["x"], REFERENCE_DATE, search_date="DIA", cadernos=["Executivo"]
    )
    assert adapter.normalize({"a": 1}, "x") == {"title": "normalizado"}


def test_mg_fetch_by_date_builds_caderno_items():
    hook = mock.Mock()
    hook.PORTAL_URL = "https://portal.example/edicao"
    hook.fetch_edition.return_value = {
        "dataPublicacao": "2026-11-14T00:00:00",
        "cadernos": [{"id": 1, "descricao": "Diário do Executivo"}],
    }

    items = DOEMGAdapter(hook=hook).fetch_by_date(REFERENCE_DATE)

    assert len(items) == 1
    assert items[0]["section"] == "DOE-MG - Diário do Executivo"
    assert items[0]["date"] == "14/11/2026"
    assert items[0]["href"] == "https://portal.example/edicao"
    assert items[0]["id"].endswith(":1")
