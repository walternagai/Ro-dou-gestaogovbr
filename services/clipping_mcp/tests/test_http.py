"""Testes do transporte HTTP do servidor MCP."""

import pytest
from app import main as mcp_main
from fastapi.testclient import TestClient

from tests.fakes import FakeApiClient


@pytest.fixture()
def api():
    mcp_main.mcp_server.client = FakeApiClient()
    mcp_main._SSE_SESSIONS.clear()
    with TestClient(mcp_main.app) as client:
        yield client, mcp_main.mcp_server.client
    mcp_main._SSE_SESSIONS.clear()


def test_healthz(api):
    client, _ = api
    response = client.get("/clipping/mcp/healthz")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_initialize_via_http(api):
    client, _ = api
    response = client.post(
        "/clipping/mcp",
        json={"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}},
    )

    assert response.status_code == 200
    assert response.json()["result"]["serverInfo"]["name"] == "rodou-clipping-mcp"


def test_tools_call_forwards_api_key(api):
    client, fake = api
    response = client.post(
        "/clipping/mcp",
        headers={"X-API-Key": "agent-key"},
        json={
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "list_items",
                "arguments": {"date": "2026-11-14"},
            },
        },
    )

    assert response.status_code == 200
    assert fake.calls[-1][2] == "agent-key"
    assert response.json()["result"]["isError"] is False


def test_tools_call_forwards_bearer_token(api):
    client, fake = api
    client.post(
        "/clipping/mcp",
        headers={"Authorization": "Bearer token-123"},
        json={
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "list_sources",
                "arguments": {},
            },
        },
    )

    assert fake.calls[-1][2] == "Bearer token-123"


def test_notification_returns_204(api):
    client, _ = api
    response = client.post(
        "/clipping/mcp",
        json={"jsonrpc": "2.0", "method": "notifications/initialized"},
    )
    assert response.status_code == 204


def test_invalid_json_returns_400(api):
    client, _ = api
    response = client.post(
        "/clipping/mcp", content="{invalid", headers={"Content-Type": "application/json"}
    )
    assert response.status_code == 400
    assert response.json()["error"]["code"] == -32700


def test_sse_endpoint_and_messages(api):
    client, _ = api

    stream = client.get(
        "/clipping/mcp/sse", headers={"X-API-Key": "sse-key"}
    )
    assert stream.status_code == 200
    assert stream.headers["content-type"].startswith("text/event-stream")
    assert "event: endpoint" in stream.text

    endpoint = next(
        line.split("data: ", 1)[1]
        for line in stream.text.splitlines()
        if line.startswith("data: ")
    )

    response = client.post(
        endpoint,
        json={"jsonrpc": "2.0", "id": 4, "method": "initialize", "params": {}},
    )
    assert response.status_code == 200
    assert response.json()["result"]["protocolVersion"] == "2024-11-05"


def test_sse_messages_use_session_auth(api):
    client, fake = api
    stream = client.get(
        "/clipping/mcp", headers={"X-API-Key": "session-key"}
    )
    endpoint = next(
        line.split("data: ", 1)[1]
        for line in stream.text.splitlines()
        if line.startswith("data: ")
    )

    client.post(
        endpoint,
        json={
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {"name": "list_sources", "arguments": {}},
        },
    )

    assert fake.calls[-1][2] == "session-key"
