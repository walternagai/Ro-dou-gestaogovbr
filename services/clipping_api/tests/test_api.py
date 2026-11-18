"""Testes de comportamento da API do Rodou Clipping."""

import logging
from datetime import UTC, date, datetime, timedelta

from app.auth import rate_limiter

from .fake_db import make_item


def _headers(key: str = "read-key") -> dict:
    return {"X-API-Key": key}


def test_healthz_and_livez(api):
    client, database = api

    health = client.get("/clipping/healthz")
    assert health.status_code == 200
    assert health.json() == {"status": "ok", "checks": {"database": "ok"}}

    live = client.get("/clipping/livez")
    assert live.status_code == 200
    assert live.json()["status"] == "ok"

    database.healthy = False
    assert client.get("/clipping/healthz").status_code == 503
    assert client.get("/clipping/livez").status_code == 200


def test_readyz_checks_db_and_airflow(api, mocker):
    client, database = api
    from app import main as api_main

    mocker.patch.object(api_main, "airflow_health", return_value=True)
    ok = client.get("/clipping/readyz")
    assert ok.status_code == 200
    assert ok.json() == {"status": "ok", "checks": {"database": "ok", "airflow": "ok"}}

    mocker.patch.object(api_main, "airflow_health", return_value=False)
    down = client.get("/clipping/readyz")
    assert down.status_code == 503
    assert down.json()["code"] == "SERVICE_UNAVAILABLE"

    database.healthy = False
    mocker.patch.object(api_main, "airflow_health", return_value=True)
    assert client.get("/clipping/readyz").status_code == 503


def test_items_requires_api_key_and_supports_bearer_fallback(api):
    client, _ = api
    response = client.get(
        "/clipping/v1/items",
        params={"date": "2026-11-14"},
        headers={"X-Request-Id": "req-401"},
    )
    assert response.status_code == 401
    assert response.headers["content-type"].startswith("application/problem+json")
    assert response.headers["X-Request-Id"] == "req-401"
    body = response.json()
    assert body["title"] == "Chave de API ausente"
    assert body["code"] == "UNAUTHORIZED"
    assert body["message"]
    assert body["request_id"] == "req-401"

    bearer = client.get(
        "/clipping/v1/items",
        params={"date": "2026-11-14"},
        headers={"Authorization": "Bearer read-key"},
    )
    assert bearer.status_code == 200


def test_invalid_api_key_is_forbidden(api):
    client, _ = api
    response = client.get(
        "/clipping/v1/items",
        params={"date": "2026-11-14"},
        headers=_headers("nao-existe"),
    )
    assert response.status_code == 403
    assert response.json()["code"] == "FORBIDDEN"


def test_items_filters_partial_reference_and_content_hash(api):
    client, database = api
    # RC-01: item estadual com uf=None no banco ainda casa com uf=MG e sai normalizado
    database.items.append(
        make_item(
            item_id="item-mg-null-uf",
            source="DOE:MG",
            uf=None,
            title="Portaria MG legada",
        )
    )

    # RC-02: filtrar source=DOE:MG não herda falha de QD (partial deve ser False)
    filtered = client.get(
        "/clipping/v1/items",
        params={"date": "2026-11-14", "source": "DOE:MG", "uf": "mg"},
        headers=_headers(),
    )
    assert filtered.status_code == 200
    f_payload = filtered.json()
    assert f_payload["total"] == 2
    assert all(it["uf"] == "MG" for it in f_payload["items"])
    assert f_payload["partial"] is False
    assert f_payload["sources_failed"] == []

    # Sem filtro de fonte, a falha de QD aparece com a data afetada
    unfiltered = client.get(
        "/clipping/v1/items",
        params={"date": "2026-11-14"},
        headers=_headers(),
    )
    assert unfiltered.status_code == 200
    payload = unfiltered.json()
    assert payload["reference"] == {
        "from": "2026-11-14",
        "to": "2026-11-14",
        "effective_date": "2026-11-14",
        "timezone": "America/Sao_Paulo",
    }
    assert payload["partial"] is True
    assert payload["sources_requested"] == ["DOU", "QD"]
    assert payload["sources_failed"][0]["source"] == "QD"
    assert payload["sources_failed"][0]["code"] == "UPSTREAM_ERROR"
    assert payload["sources_failed"][0]["message"] == "HTTP 503"
    assert payload["sources_failed"][0]["date"] == "2026-11-14"


def test_missing_weekday_and_disjoint_filters_and_31_days_window(api):
    client, database = api
    from .fake_db import make_run

    # RC-05: 31 dias corridos (pontas incluídas) passa; 32 dias falha com 422
    ok_31 = client.get(
        "/clipping/v1/items",
        params={"from": "2026-10-01", "to": "2026-10-31"},
        headers=_headers(),
    )
    assert ok_31.status_code == 200

    fail_32 = client.get(
        "/clipping/v1/items",
        params={"from": "2026-10-01", "to": "2026-11-01"},
        headers=_headers(),
    )
    assert fail_32.status_code == 422

    # RC-02: combinação disjunta (DOU + MG) não gera falha falsa
    disjoint = client.get(
        "/clipping/v1/items",
        params={"date": "2026-11-14", "source": "DOU", "uf": "MG"},
        headers=_headers(),
    )
    assert disjoint.status_code == 200
    assert disjoint.json()["total"] == 0
    assert disjoint.json()["partial"] is False
    assert disjoint.json()["sources_failed"] == []

    # RC-03: dia útil (2026-11-16, segunda) sem run entre runs existentes gera MISSING_RUN
    database.runs.append(
        make_run(
            run_id="dag_test:run-17",
            reference_date=date(2026, 11, 17),
            started_at=datetime(2026, 11, 17, 5, 0),
        )
    )
    gap = client.get(
        "/clipping/v1/items",
        params={"from": "2026-11-16", "to": "2026-11-17", "source": "DOU"},
        headers=_headers(),
    )
    assert gap.status_code == 200
    g_body = gap.json()
    assert g_body["partial"] is True
    assert g_body["sources_failed"][0]["source"] == "DOU"
    assert g_body["sources_failed"][0]["code"] == "MISSING_RUN"
    assert g_body["sources_failed"][0]["date"] == "2026-11-16"


def test_enabled_sources_filter_hides_inactive_sources(api, monkeypatch):
    client, _ = api
    monkeypatch.setenv("CLIPPING_ENABLED_SOURCES", "DOU,DOE:MG")

    sources = client.get("/clipping/v1/sources", headers=_headers())
    assert sources.status_code == 200
    assert [s["source"] for s in sources.json()["sources"]] == ["DOU", "DOE:MG"]

    disabled_src = client.get(
        "/clipping/v1/items",
        params={"date": "2026-11-14", "source": "QD"},
        headers=_headers(),
    )
    assert disabled_src.status_code == 422
    assert disabled_src.json()["allowed"] == ["DOU", "DOE:MG"]

    disabled_uf = client.get(
        "/clipping/v1/items",
        params={"date": "2026-11-14", "uf": "SP"},
        headers=_headers(),
    )
    assert disabled_uf.status_code == 422
    assert disabled_uf.json()["allowed"] == ["MG"]


def test_items_page_and_cursor_pagination(api):
    client, database = api
    database.items.append(
        make_item(item_id="item-4", source="DOU", published_date=date(2026, 11, 14))
    )

    first = client.get(
        "/clipping/v1/items",
        params={"date": "2026-11-14", "size": 2, "page": 1},
        headers=_headers(),
    )
    p1 = first.json()
    assert p1["total"] == 3
    assert [it["id"] for it in p1["items"]] == ["item-1", "item-2"]
    assert p1["next_page"] == 2
    assert p1["next_cursor"]

    second = client.get(
        "/clipping/v1/items",
        params={"date": "2026-11-14", "size": 2, "cursor": p1["next_cursor"]},
        headers=_headers(),
    )
    p2 = second.json()
    assert [it["id"] for it in p2["items"]] == ["item-4"]
    assert p2["next_page"] is None
    assert p2["next_cursor"] is None


def test_items_etag_and_runs_etag_return_304(api):
    client, _ = api

    for path, params in (
        ("/clipping/v1/items", {"date": "2026-11-14"}),
        ("/clipping/v1/runs", {"date": "2026-11-14"}),
        ("/clipping/v1/sources", None),
    ):
        first = client.get(path, params=params, headers=_headers())
        assert first.status_code == 200
        etag = first.headers["etag"]
        assert first.headers["cache-control"].startswith("private")

        second = client.get(
            path,
            params=params,
            headers={**_headers(), "If-None-Match": etag},
        )
        assert second.status_code == 304
        assert second.content == b""


def test_validation_errors_return_422_with_code_and_allowed(api):
    client, _ = api

    bad_source = client.get(
        "/clipping/v1/items",
        params={"date": "2026-11-14", "source": "XPTO"},
        headers=_headers(),
    )
    assert bad_source.status_code == 422
    body = bad_source.json()
    assert body["code"] == "VALIDATION_ERROR"
    assert "XPTO" in body["detail"]
    assert "DOU" in body["allowed"]

    bad_uf = client.get(
        "/clipping/v1/items",
        params={"date": "2026-11-14", "uf": "XX"},
        headers=_headers(),
    )
    assert bad_uf.status_code == 422
    assert bad_uf.json()["code"] == "VALIDATION_ERROR"
    assert bad_uf.json()["allowed"] == ["MG", "SP"]

    bad_window = client.get(
        "/clipping/v1/items",
        params={"from": "2026-01-01", "to": "2026-11-14"},
        headers=_headers(),
    )
    assert bad_window.status_code == 422

    both_date_and_from = client.get(
        "/clipping/v1/items",
        params={"date": "2026-11-14", "from": "2026-11-13", "to": "2026-11-14"},
        headers=_headers(),
    )
    assert both_date_and_from.status_code == 422

    short_q = client.get(
        "/clipping/v1/items",
        params={"date": "2026-11-14", "q": "a"},
        headers=_headers(),
    )
    assert short_q.status_code == 422

    bad_since = client.get(
        "/clipping/v1/items",
        params={"date": "2026-11-14", "since": "nao-e-data"},
        headers=_headers(),
    )
    assert bad_since.status_code == 422

    oversize = client.get(
        "/clipping/v1/items",
        params={"date": "2026-11-14", "size": 201},
        headers=_headers(),
    )
    assert oversize.status_code == 422

    bad_cursor = client.get(
        "/clipping/v1/items",
        params={"date": "2026-11-14", "cursor": "!!!"},
        headers=_headers(),
    )
    assert bad_cursor.status_code == 422

    cursor_with_page = client.get(
        "/clipping/v1/items",
        params={"date": "2026-11-14", "page": 2, "cursor": "abc"},
        headers=_headers(),
    )
    assert cursor_with_page.status_code == 422


def test_malformed_json_returns_400(api):
    client, _ = api
    response = client.post(
        "/clipping/v1/runs",
        content="{invalid-json",
        headers={**_headers("trigger-key"), "Content-Type": "application/json"},
    )
    assert response.status_code == 400
    assert response.json()["code"] == "BAD_REQUEST"


def test_items_sanitize_html_and_control_chars(api):
    client, database = api
    long_html = (
        "<p>Aviso\x00 de <span class='highlight'>Licitação</span></p>"
        + (" x" * 700)
    )
    database.items.append(
        make_item(
            item_id="item-dirty",
            source="DOU",
            title="<b>Portaria</b>\x07 123" + (" A" * 300),
            abstract=long_html,
        )
    )

    listed = client.get(
        "/clipping/v1/items",
        params={"date": "2026-11-14", "q": "Portaria"},
        headers=_headers(),
    )
    assert listed.status_code == 200
    item = listed.json()["items"][0]
    assert "<b>" not in item["title"]
    assert "\x07" not in item["title"]
    assert len(item["title"]) <= 500
    assert "<p>" not in item["abstract"]
    assert "<span" not in item["abstract"]
    assert "\x00" not in item["abstract"]
    assert len(item["abstract"]) <= 1000

    single = client.get("/clipping/v1/items/item-dirty", headers=_headers()).json()
    assert "<p>" not in single["abstract"]
    assert len(single["abstract"]) <= 1000


def test_get_item(api):
    client, _ = api
    assert client.get("/clipping/v1/items/item-1", headers=_headers()).status_code == 200
    not_found = client.get("/clipping/v1/items/nope", headers=_headers())
    assert not_found.status_code == 404
    assert not_found.json()["code"] == "NOT_FOUND"


def test_runs_endpoints(api):
    client, _ = api

    listed = client.get(
        "/clipping/v1/runs", params={"date": "2026-11-14"}, headers=_headers()
    )
    assert listed.status_code == 200
    runs = listed.json()["runs"]
    assert len(runs) == 2
    partial_run = next(r for r in runs if r["id"] == "dag_test:run-2")
    assert partial_run["sources_failed"][0]["code"] == "UPSTREAM_ERROR"
    assert partial_run["sources_failed"][0]["message"] == "HTTP 503"

    single = client.get("/clipping/v1/runs/dag_test:run-1", headers=_headers())
    assert single.status_code == 200
    assert single.json()["id"] == "dag_test:run-1"


def test_sources_endpoint_includes_freshness_and_stale(api, monkeypatch):
    client, database = api
    monkeypatch.setenv("CLIPPING_STALE_AFTER_HOURS", "24")
    database.source_runs.append(
        {
            "source": "DOE:MG",
            "status": "ok",
            "items_count": 5,
            "started_at": datetime.now(UTC) - timedelta(hours=30),
        }
    )

    response = client.get("/clipping/v1/sources", headers=_headers())
    assert response.status_code == 200
    sources = {source["source"]: source for source in response.json()["sources"]}
    assert set(sources) == {"DOU", "QD", "INLABS", "DOE:SP", "DOE:MG"}
    assert sources["DOU"]["stale"] is False
    assert sources["DOU"]["last_success_at"] is not None
    assert sources["QD"]["last_status"] == "failed"
    assert sources["QD"]["stale"] is True
    assert sources["QD"]["last_success_at"] is None
    assert sources["DOE:MG"]["uf"] == "MG"
    assert sources["DOE:MG"]["stale"] is True


def test_trigger_requires_trigger_scope(api):
    client, _ = api
    response = client.post(
        "/clipping/v1/runs",
        json={"reference_date": "2026-11-15"},
        headers=_headers("read-key"),
    )
    assert response.status_code == 403
    assert response.json()["code"] == "FORBIDDEN"


def test_trigger_is_idempotent(api, mocker):
    client, database = api
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
    assert database.queued_runs[0]["id"] == "rodou_clipping:run-1"


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
    assert response.json()["code"] == "CONFLICT"
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
    assert second.json()["code"] == "RATE_LIMITED"


def test_audit_request_id_and_query_privacy(api, caplog):
    client, database = api
    sensitive_term = "sigilo_demanda_9911"

    with caplog.at_level(logging.INFO, logger="clipping.api"):
        response = client.get(
            "/clipping/v1/items",
            params={"date": "2026-11-14", "q": sensitive_term},
            headers={
                **_headers(),
                "X-Request-Id": "corr-123",
                "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            },
        )

    assert response.status_code == 200
    assert response.headers["X-Request-Id"] == "corr-123"
    assert database.audits[0]["key_id"] == "key-read"
    assert database.audits[0]["route"] == "/clipping/v1/items"
    assert database.audits[0]["request_id"] == "corr-123"

    log_blob = "\n".join(record.getMessage() + str(record.__dict__) for record in caplog.records)
    assert sensitive_term not in log_blob
    assert "read-key" not in log_blob


def test_internal_metrics_endpoint_and_public_route_removed(api):
    client, _ = api
    params = {"date": "2026-11-14"}

    first = client.get("/clipping/v1/items", params=params, headers=_headers())
    client.get(
        "/clipping/v1/items",
        params=params,
        headers={**_headers(), "If-None-Match": first.headers["etag"]},
    )

    assert client.get("/clipping/metrics").status_code == 404

    response = client.get("/internal/metrics")
    assert response.status_code == 200
    body = response.text
    assert (
        'clipping_api_requests_total{route="/clipping/v1/items",status="200"} 1'
        in body
    )
    assert (
        'clipping_api_requests_total{route="/clipping/v1/items",status="304"} 1'
        in body
    )
    assert 'clipping_api_source_status{source="QD",status="failed"} 1' in body


def test_openapi_contract_and_expose_docs_flag(api, monkeypatch):
    client, _ = api
    spec = client.get("/clipping/openapi.json").json()

    schemes = spec["components"]["securitySchemes"]
    assert list(schemes.keys()) == ["ApiKeyAuth"]
    assert schemes["ApiKeyAuth"]["type"] == "apiKey"
    assert schemes["ApiKeyAuth"]["in"] == "header"
    assert schemes["ApiKeyAuth"]["name"] == "X-API-Key"

    items_get = spec["paths"]["/clipping/v1/items"]["get"]
    assert {"ApiKeyAuth": []} in items_get["security"]
    assert set(items_get["responses"]).issuperset(
        {"200", "304", "401", "403", "422", "429", "503"}
    )

    params_by_name = {p["name"]: p for p in items_get["parameters"]}
    assert params_by_name["size"]["schema"]["minimum"] == 1
    assert params_by_name["size"]["schema"]["maximum"] == 200
    assert params_by_name["since"]["schema"]["anyOf"][0]["format"] == "date-time"

    trigger_post = spec["paths"]["/clipping/v1/runs"]["post"]
    assert any(p["name"] == "Idempotency-Key" and p["in"] == "header" for p in trigger_post["parameters"])

    run_schema = spec["components"]["schemas"]["RunModel"]
    assert set(run_schema["properties"]["status"]["enum"]) == {
        "queued",
        "running",
        "success",
        "partial",
        "failed",
    }

    monkeypatch.setenv("CLIPPING_EXPOSE_DOCS", "false")
    assert client.get("/clipping/openapi.json").status_code == 404
    assert client.get("/clipping/docs").status_code == 404
