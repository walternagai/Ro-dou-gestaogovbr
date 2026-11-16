"""Testes da task de persistência do clipping (sem banco)."""

from types import SimpleNamespace

import pendulum
import pytest
from dags.ro_dou_src.dou_dag_generator import DouDigestDagGenerator


def _context(run_id="run-1"):
    return {
        "run_id": run_id,
        "dag_run": SimpleNamespace(run_type="scheduled"),
        "data_interval_end": pendulum.datetime(2026, 11, 16, 3, 0, tz="UTC"),
    }


def _specs(sources=("DOU",)):
    return SimpleNamespace(
        id="dag_test",
        search=[SimpleNamespace(sources=list(sources))],
    )


def test_persist_results_skips_when_connection_is_not_configured(monkeypatch):
    generator = DouDigestDagGenerator()
    monkeypatch.setattr(
        DouDigestDagGenerator,
        "_clipping_connection",
        staticmethod(lambda: None),
    )
    monkeypatch.setattr(
        generator,
        "get_xcom_pull_tasks",
        lambda num_searches, **context: [
            {
                "result": {"grupo": {"termo": {"departamento": [{"title": "x"}]}}},
                "sources_status": {
                    "DOU": {"status": "ok", "error": None, "items_count": 1}
                },
            }
        ],
    )

    assert generator.persist_results(num_searches=1, specs=_specs(), **_context()) is None


def test_persist_results_raises_when_search_is_missing_and_no_connection(monkeypatch):
    generator = DouDigestDagGenerator()
    monkeypatch.setattr(
        DouDigestDagGenerator,
        "_clipping_connection",
        staticmethod(lambda: None),
    )
    monkeypatch.setattr(
        generator,
        "get_xcom_pull_tasks",
        lambda num_searches, **context: [None],
    )

    with pytest.raises(RuntimeError):
        generator.persist_results(num_searches=1, specs=_specs(), **_context())
