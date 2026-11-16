"""Testes de integração da persistência do clipping.

Requerem um PostgreSQL com permissão para criar o schema `clipping`. Rodam
somente quando `CLIPPING_TEST_DSN` está definido; sem a variável são ignorados
para não depender de banco nos testes padrão.

Exemplo:
    CLIPPING_TEST_DSN=postgresql://postgres:postgres@localhost:5432/clipping \\
        pytest -q tests/clipping_persistence_pg_test.py
"""

import os
from datetime import date
from types import SimpleNamespace

import pendulum
import pytest

psycopg2 = pytest.importorskip("psycopg2")

from dags.ro_dou_src.clipping_persistence import (
    RUN_FAILED,
    RUN_PARTIAL,
    RUN_SUCCESS,
    mark_run_failed,
    persist_run,
)
from dags.ro_dou_src.dou_dag_generator import DouDigestDagGenerator

REFERENCE_DATE = date(2026, 11, 16)
SCHEMA_FILE = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__), "..", "clipping", "sql", "01-schema.sql"
    )
)


def _item(href="https://www.in.gov.br/web/dou/-/extrato-1", **overrides):
    item = {
        "source": "DOU",
        "section": "Seção 3",
        "title": "Extrato de contrato",
        "href": href,
        "abstract": "Contrato de licitação",
        "date": "15/11/2026",
    }
    item.update(overrides)
    return item


def _search_result(items, statuses):
    return {
        "result": {
            "single_group": {
                "licitação": {
                    "single_department": items,
                }
            }
        },
        "sources_status": statuses,
    }


@pytest.fixture()
def conn():
    dsn = os.getenv("CLIPPING_TEST_DSN")
    if not dsn:
        pytest.skip("CLIPPING_TEST_DSN não definido")
    if not os.path.exists(SCHEMA_FILE):
        pytest.skip("clipping/sql/01-schema.sql não encontrado")

    connection = psycopg2.connect(dsn)
    connection.autocommit = True
    with connection.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS clipping CASCADE")
        with open(SCHEMA_FILE, encoding="utf-8") as schema:
            cur.execute(schema.read())
    connection.autocommit = False

    yield connection

    connection.close()


def test_persist_run_is_idempotent(conn):
    results = [
        _search_result(
            [_item(href="https://example.test/1"), _item(href="https://example.test/2")],
            {"DOU": {"status": "ok", "error": None, "items_count": 2}},
        )
    ]

    first = persist_run(
        conn,
        dag_id="dag_test",
        run_id="run-1",
        reference_date=REFERENCE_DATE,
        search_results=results,
        configured_sources=["DOU"],
    )
    second = persist_run(
        conn,
        dag_id="dag_test",
        run_id="run-1",
        reference_date=REFERENCE_DATE,
        search_results=results,
        configured_sources=["DOU"],
    )

    assert first.status == RUN_SUCCESS
    assert second.items_count == 2

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM clipping.item")
        assert cur.fetchone()[0] == 2
        cur.execute(
            "SELECT status, items_count FROM clipping.run WHERE id = 'run-1'"
        )
        assert cur.fetchone() == (RUN_SUCCESS, 2)
        cur.execute(
            "SELECT status FROM clipping.source_run WHERE run_id = 'run-1'"
        )
        assert [row[0] for row in cur.fetchall()] == ["ok"]


def test_reprocessing_preserves_first_run_id(conn):
    persist_run(
        conn,
        dag_id="dag_test",
        run_id="run-1",
        reference_date=REFERENCE_DATE,
        search_results=[
            _search_result(
                [_item(abstract="texto original")],
                {"DOU": {"status": "ok", "error": None, "items_count": 1}},
            )
        ],
        configured_sources=["DOU"],
    )
    persist_run(
        conn,
        dag_id="dag_test",
        run_id="run-2",
        reference_date=REFERENCE_DATE,
        search_results=[
            _search_result(
                [_item(abstract="texto atualizado")],
                {"DOU": {"status": "ok", "error": None, "items_count": 1}},
            )
        ],
        configured_sources=["DOU"],
    )

    with conn.cursor() as cur:
        cur.execute("SELECT run_id, abstract FROM clipping.item")
        assert cur.fetchone() == ("run-1", "texto atualizado")


def test_partial_run_when_one_source_fails(conn):
    results = [
        _search_result(
            [_item()],
            {
                "DOU": {"status": "ok", "error": None, "items_count": 1},
                "QD": {"status": "failed", "error": "HTTP 503", "items_count": 0},
            },
        )
    ]

    summary = persist_run(
        conn,
        dag_id="dag_test",
        run_id="run-partial",
        reference_date=REFERENCE_DATE,
        search_results=results,
        configured_sources=["DOU", "QD"],
    )

    assert summary.status == RUN_PARTIAL
    assert summary.sources_ok == ["DOU"]
    assert [item["source"] for item in summary.sources_failed] == ["QD"]

    with conn.cursor() as cur:
        cur.execute(
            "SELECT source, status FROM clipping.source_run "
            "WHERE run_id = 'run-partial' ORDER BY source"
        )
        assert cur.fetchall() == [("DOU", "ok"), ("QD", "failed")]


def test_failed_run_when_no_source_finishes(conn):
    summary = persist_run(
        conn,
        dag_id="dag_test",
        run_id="run-failed",
        reference_date=REFERENCE_DATE,
        search_results=[None],
        configured_sources=["DOU"],
    )

    assert summary.status == RUN_FAILED
    assert summary.items_count == 0
    assert summary.sources_ok == []

    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, items_count FROM clipping.run WHERE id = 'run-failed'"
        )
        assert cur.fetchone() == (RUN_FAILED, 0)


def test_mark_run_failed_is_idempotent(conn):
    mark_run_failed(
        conn,
        run_id="run-crash",
        dag_id="dag_test",
        reference_date=REFERENCE_DATE,
        error="falha inesperada",
    )
    mark_run_failed(
        conn,
        run_id="run-crash",
        dag_id="dag_test",
        reference_date=REFERENCE_DATE,
        error="falha inesperada",
    )

    with conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM clipping.run WHERE id = 'run-crash'"
        )
        assert cur.fetchone() == ("failed",)


class _NonClosingConnection:
    """Evita que a task feche a conexão compartilhada pelo teste."""

    def __init__(self, conn):
        self._conn = conn

    def cursor(self, *args, **kwargs):
        return self._conn.cursor(*args, **kwargs)

    def commit(self):
        return self._conn.commit()

    def rollback(self):
        return self._conn.rollback()

    def close(self):
        return None


def _task_context(run_id):
    return {
        "run_id": run_id,
        "dag_run": SimpleNamespace(run_type="scheduled"),
        "data_interval_end": pendulum.datetime(2026, 11, 16, 3, 0, tz="UTC"),
    }


def _task_specs(sources=("DOU",)):
    return SimpleNamespace(
        id="dag_test",
        search=[SimpleNamespace(sources=list(sources))],
    )


def test_task_persists_results(conn, monkeypatch):
    generator = DouDigestDagGenerator()
    monkeypatch.setattr(
        DouDigestDagGenerator,
        "_clipping_connection",
        staticmethod(lambda: _NonClosingConnection(conn)),
    )
    monkeypatch.setattr(
        generator,
        "get_xcom_pull_tasks",
        lambda num_searches, **context: [
            _search_result(
                [_item()],
                {"DOU": {"status": "ok", "error": None, "items_count": 1}},
            )
        ],
    )

    generator.persist_results(
        num_searches=1, specs=_task_specs(), **_task_context("run-task")
    )

    with conn.cursor() as cur:
        cur.execute("SELECT status, items_count FROM clipping.run WHERE id = 'dag_test:run-task'")
        assert cur.fetchone() == (RUN_SUCCESS, 1)


def test_task_raises_and_records_failed_when_search_is_missing(conn, monkeypatch):
    generator = DouDigestDagGenerator()
    monkeypatch.setattr(
        DouDigestDagGenerator,
        "_clipping_connection",
        staticmethod(lambda: _NonClosingConnection(conn)),
    )
    monkeypatch.setattr(
        generator,
        "get_xcom_pull_tasks",
        lambda num_searches, **context: [None],
    )

    with pytest.raises(RuntimeError):
        generator.persist_results(
            num_searches=1, specs=_task_specs(), **_task_context("run-task-failed")
        )

    with conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM clipping.run WHERE id = 'dag_test:run-task-failed'"
        )
        assert cur.fetchone() == ("failed",)
