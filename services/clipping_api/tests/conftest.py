"""Fixtures dos testes da API do clipping."""

import os
from datetime import UTC, date, datetime

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("CLIPPING_DB_URL", "postgresql://unused")
os.environ.setdefault("AIRFLOW_API_TOKEN", "test-token")

from app import main as api_main
from app.auth import hash_key, rate_limiter
from app.metrics import metrics

from .fake_db import FakeDB, make_item, make_run


@pytest.fixture()
def api():
    now_utc = datetime.now(UTC)
    database = FakeDB()
    database.keys[hash_key("read-key")] = {
        "id": "key-read",
        "label": "leitura",
        "scope": ["clipping:read"],
        "active": True,
    }
    database.keys[hash_key("trigger-key")] = {
        "id": "key-trigger",
        "label": "disparo",
        "scope": ["clipping:read", "clipping:trigger"],
        "active": True,
    }

    database.items = [
        make_item(item_id="item-1", source="DOU"),
        make_item(
            item_id="item-2",
            source="DOE:MG",
            uf="MG",
            title="Portaria MG",
            abstract="licitação mineira",
        ),
        make_item(
            item_id="item-3",
            source="DOE:MG",
            uf="MG",
            published_date=date(2026, 11, 13),
            title="Outro dia",
            abstract="contrato",
        ),
    ]
    database.runs = [
        make_run(run_id="dag_test:run-1"),
        make_run(
            run_id="dag_test:run-2",
            status="partial",
            sources_requested=("DOU", "QD"),
            sources_ok=("DOU",),
            sources_failed=(
                {"source": "QD", "error": "HTTP 503", "at": "2026-11-14T05:10:00-03:00"},
            ),
        ),
    ]
    database.source_runs = [
        {
            "source": "DOU",
            "status": "ok",
            "items_count": 1,
            "started_at": now_utc,
        },
        {
            "source": "QD",
            "status": "failed",
            "items_count": 0,
            "started_at": now_utc,
        },
    ]

    api_main.app.state.db = database
    api_main._IDEMPOTENCY.clear()
    rate_limiter.reset()
    metrics.reset()

    with TestClient(api_main.app) as client:
        yield client, database

    rate_limiter.reset()
