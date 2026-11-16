"""Testes do expurgo por retenção.

A parte de integração roda apenas com ``CLIPPING_TEST_DSN`` e o schema do
clipping acessível.
"""

import os
from pathlib import Path

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from scripts.purge import purge

SCHEMA_CANDIDATES = (
    Path(__file__).resolve().parents[3] / "clipping" / "sql" / "01-schema.sql",
    Path("/opt/airflow/clipping/sql/01-schema.sql"),
)
SCHEMA_FILE = next((path for path in SCHEMA_CANDIDATES if path.exists()), None)


@pytest.fixture()
def dsn():
    url = os.getenv("CLIPPING_TEST_DSN")
    if not url:
        pytest.skip("CLIPPING_TEST_DSN não definido")
    if SCHEMA_FILE is None:
        pytest.skip("clipping/sql/01-schema.sql não encontrado")

    connection = psycopg2.connect(url)
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute("DROP SCHEMA IF EXISTS clipping CASCADE")
        cursor.execute(SCHEMA_FILE.read_text(encoding="utf-8"))

        cursor.execute(
            """
            INSERT INTO clipping.run
                (id, dag_id, reference_date, status, sources_requested,
                 sources_ok, items_count, started_at, finished_at)
            VALUES
                ('run-old', 'dag', CURRENT_DATE - interval '26 months',
                 'success', '{DOU}', '{DOU}', 1,
                 now() - interval '26 months', now() - interval '26 months'),
                ('run-new', 'dag', CURRENT_DATE - interval '1 month',
                 'success', '{DOU}', '{DOU}', 1,
                 now() - interval '1 month', now() - interval '1 month')
            """
        )
        cursor.execute(
            """
            INSERT INTO clipping.item
                (id, run_id, source, published_date, title, abstract, url)
            VALUES
                ('old', 'run-old', 'DOU',
                 CURRENT_DATE - interval '25 months', 'antigo', 'x',
                 'https://example.test/old'),
                ('new', 'run-new', 'DOU',
                 CURRENT_DATE - interval '1 month', 'recente', 'y',
                 'https://example.test/new')
            """
        )
        cursor.execute(
            """
            INSERT INTO clipping.api_request
                (key_id, route, status, created_at)
            VALUES
                ('k', '/old', 200, now() - interval '100 days'),
                ('k', '/new', 200, now() - interval '1 day')
            """
        )

    connection.close()
    return url


def test_purge_dry_run_does_not_delete(dsn):
    summary = purge(dsn, months=24, api_request_days=90, dry_run=True)

    assert summary["items"] == 1
    assert summary["runs"] == 1
    assert summary["api_requests"] == 1

    connection = psycopg2.connect(dsn)
    with connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM clipping.item")
        assert cursor.fetchone()[0] == 2
    connection.close()


def test_purge_removes_only_expired_rows(dsn):
    summary = purge(
        dsn, months=24, api_request_days=90, batch_size=1, dry_run=False
    )

    assert summary["items"] == 1
    assert summary["runs"] == 1
    assert summary["api_requests"] == 1

    connection = psycopg2.connect(dsn)
    with connection.cursor() as cursor:
        cursor.execute("SELECT id FROM clipping.item ORDER BY id")
        assert [row[0] for row in cursor.fetchall()] == ["new"]

        cursor.execute("SELECT id FROM clipping.run ORDER BY id")
        assert [row[0] for row in cursor.fetchall()] == ["run-new"]

        cursor.execute("SELECT route FROM clipping.api_request ORDER BY route")
        assert [row[0] for row in cursor.fetchall()] == ["/new"]
    connection.close()
