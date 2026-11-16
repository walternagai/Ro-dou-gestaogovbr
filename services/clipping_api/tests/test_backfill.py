"""Testes do script de backfill (sem rede)."""

from datetime import date
from unittest import mock

import pytest
from scripts.backfill import date_range, has_run, logical_date, trigger_day


def _response(status, payload=None):
    response = mock.Mock()
    response.status_code = status
    response.json.return_value = payload or {}
    return response


def test_date_range_accepts_31_days():
    days = date_range(date(2026, 10, 1), date(2026, 10, 31), include_weekends=True)
    assert len(days) == 31


def test_date_range_rejects_more_than_31_days():
    with pytest.raises(ValueError, match="31"):
        date_range(date(2026, 10, 1), date(2026, 11, 1))


def test_date_range_rejects_inverted_window():
    with pytest.raises(ValueError):
        date_range(date(2026, 11, 2), date(2026, 11, 1))


def test_date_range_skips_weekends_by_default():
    days = date_range(date(2026, 11, 6), date(2026, 11, 9))
    assert [day.weekday() for day in days] == [4, 0]  # sexta e segunda


def test_logical_date_uses_brasilia_noon():
    assert logical_date(date(2026, 11, 6)) == "2026-11-06T12:00:00-03:00"


def test_has_run_reads_dag_runs():
    client = mock.Mock()
    client.get.return_value = _response(200, {"dag_runs": [{"dag_run_id": "x"}]})
    assert has_run(client, "http://airflow", "token", "dag", date(2026, 11, 6))

    client.get.return_value = _response(200, {"dag_runs": []})
    assert not has_run(client, "http://airflow", "token", "dag", date(2026, 11, 6))

    client.get.return_value = _response(500, {})
    assert not has_run(client, "http://airflow", "token", "dag", date(2026, 11, 6))


def test_trigger_day_sends_backfill_conf():
    client = mock.Mock()
    client.post.return_value = _response(200, {"dag_run_id": "run-1"})

    result = trigger_day(
        client, "http://airflow", "token", "dag", date(2026, 11, 6), ["DOU"]
    )

    assert result == "queued"
    _, kwargs = client.post.call_args
    assert kwargs["json"]["logical_date"] == "2026-11-06T12:00:00-03:00"
    assert kwargs["json"]["conf"] == {
        "reference_date": "2026-11-06",
        "sources": ["DOU"],
        "backfill": True,
    }
    assert kwargs["headers"]["Authorization"] == "Bearer token"


def test_trigger_day_handles_conflict_and_error():
    client = mock.Mock()
    client.post.return_value = _response(409)
    assert (
        trigger_day(client, "http://airflow", "t", "dag", date(2026, 11, 6), None)
        == "exists"
    )

    client.post.return_value = _response(500)
    assert trigger_day(
        client, "http://airflow", "t", "dag", date(2026, 11, 6), None
    ).startswith("failed")
