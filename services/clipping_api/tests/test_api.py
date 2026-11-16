"""Testes de comportamento da API do Rodou Clipping."""

from datetime import date

from app.auth import rate_limiter


def _headers(key: str = "read-key") -> dict:
    return {"X-API-Key": key}


def test_healthz(api):
    client, _ = api
    response = client.get("/clipping/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_items_requires_api_key(api):
    client, _ = api
    response = client.get("/clipping/v1/items", params={"date": "2026-11-14"})
    assert response.status_code == 401
    assert response.headers["content-type"].startswith("application/problem+json")
    assert response.json()["title"] == "Chave de API ausente"


def test_invalid_api_key_is_forbidden(api):
    client, _ = api
    response = client.get(
        "/clipping/v1/items",
        params={"date": "2026-11-14"},
        headers=_headers("nao-existe"),
    )
    assert response.status_code == 403


def test_items_filters_and_sources_failed(api):
    client, _ = api
    response = client.get(
        "/clipping/v1/items",
        params={"date": "2026-11-14", "source": "DOE:MG", "uf": "MG"},
        headers=_headers(),
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["id"] == "item-2"
    assert payload["reference"] == {"from": "2026-11-14", "to": "2026-11-14"}
    assert payload["sources_requested"] == ["DOU", "QD"]
    assert [failure["source"] for failure in payload["sources_failed"]] == ["QD"]


def test_items_pagination(api):
    client, _ = api
    response = client.get(
        "/clipping/v1/items",
        params={"date": "2026-11-14", "size": 1, "page": 1},
        headers=_headers(),
    )

    payload = response.json()
    assert payload["total"] == 2
    assert len(payload["items"]) == 1
    assert payload["next_page"] == 2


def test_items_etag_returns_304(api):
    client, _ = api
    params = {"date": "2026-11-14"}

    first = client.get("/clipping/v1/items", params=params, headers=_headers())
    assert first.status_code == 200
    etag = first.headers["etag"]
    assert first.headers["cache-control"].startswith("private")

    second = client.get(
        "/clipping/v1/items",
        params=params,
        headers={**_headers(), "If-None-Match": etag},
    )
    assert second.status_code == 304
    assert second.content == b""


def test_invalid_source_returns_allowed(api):
    client, _ = api
    response = client.get(
        "/clipping/v1/items",
        params={"date": "2026-11-14", "source": "XPTO"},
        headers=_headers(),
    )

    assert response.status_code == 400
    body = response.json()
    assert "XPTO" in body["detail"]
    assert "DOU" in body["allowed"]


def test_invalid_window_is_rejected(api):
    client, _ = api
    response = client.get(
        "/clipping/v1/items",
        params={"from": "2026-01-01", "to": "2026-11-14"},
        headers=_headers(),
    )
    assert response.status_code == 400


def test_get_item(api):
    client, _ = api
    assert client.get("/clipping/v1/items/item-1", headers=_headers()).status_code == 200
    assert client.get("/clipping/v1/items/nope", headers=_headers()).status_code == 404


def test_runs_endpoints(api):
    client, _ = api

    listed = client.get(
        "/clipping/v1/runs", params={"date": "2026-11-14"}, headers=_headers()
    )
    assert listed.status_code == 200
    assert len(listed.json()["runs"]) == 2

    single = client.get("/clipping/v1/runs/dag_test:run-1", headers=_headers())
    assert single.status_code == 200
    assert single.json()["id"] == "dag_test:run-1"


def test_sources_endpoint(api):
    client, _ = api
    response = client.get("/clipping/v1/sources", headers=_headers())

    assert response.status_code == 200
    sources = {source["source"]: source for source in response.json()["sources"]}
    assert set(sources) == {"DOU", "QD", "INLABS", "DOE:SP", "DOE:MG"}
    assert sources["QD"]["last_status"] == "failed"
    assert sources["DOE:MG"]["uf"] == "MG"


def test_trigger_requires_trigger_scope(api):
    client, _ = api
    response = client.post(
        "/clipping/v1/runs",
        json={"reference_date": "2026-11-15"},
        headers=_headers("read-key"),
    )
    assert response.status_code == 403


def test_trigger_is_idempotent(api, mocker):
    client, _ = api
    from app import main as api_main

    trigger = mocker.patch.object(api_main, "trigger_dag", return_value="run-1")
    headers = {"X-API-Key": "trigger-key", "Idempotency-Key": "clipping:2026-11-15"}
    body = {"reference_date": "2026-11-15", "sources": ["DOU"]}

    first = client.post("/clipping/v1/runs", json=body, headers=headers)
    second = client.post("/clipping/v1/runs", json=body, headers=headers)

    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["run_id"] == "rodou_clipping:run-1"
    assert second.json()["run_id"] == first.json()["run_id"]
    trigger.assert_called_once()


def test_trigger_conflicts_with_running_run(api, mocker):
    client, database = api
    from app import main as api_main

    database.running_dates.add(date(2026, 11, 15))
    trigger = mocker.patch.object(api_main, "trigger_dag", return_value="run-1")

    response = client.post(
        "/clipping/v1/runs",
        json={"reference_date": "2026-11-15"},
        headers={"X-API-Key": "trigger-key"},
    )

    assert response.status_code == 409
    trigger.assert_not_called()


def test_rate_limit_returns_429(api, monkeypatch):
    client, _ = api
    monkeypatch.setenv("CLIPPING_READ_LIMIT_PER_MINUTE", "1")
    rate_limiter.reset()

    first = client.get(
        "/clipping/v1/items", params={"date": "2026-11-14"}, headers=_headers()
    )
    second = client.get(
        "/clipping/v1/items", params={"date": "2026-11-14"}, headers=_headers()
    )

    assert first.status_code == 200
    assert second.status_code == 429
    assert second.headers["Retry-After"]
    assert second.headers["X-RateLimit-Limit"] == "1"


def test_audit_is_recorded(api):
    client, database = api
    client.get(
        "/clipping/v1/items", params={"date": "2026-11-14"}, headers=_headers()
    )

    assert database.audits
    assert database.audits[0]["key_id"] == "key-read"
    assert database.audits[0]["route"] == "/clipping/v1/items"
