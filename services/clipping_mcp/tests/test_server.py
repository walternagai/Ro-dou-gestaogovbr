"""Testes do protocolo MCP e das ferramentas."""

import asyncio
import json

import pytest
from app.client import ApiClientError
from app.server import McpServer
from app.tools import DATA_SECURITY_BANNER

from tests.fakes import FakeApiClient


def _run(coro):
    return asyncio.run(coro)


def _content(response: dict) -> str:
    return response["result"]["content"][0]["text"]


@pytest.fixture()
def server():
    client = FakeApiClient()
    return McpServer(client), client


def test_initialize_returns_server_info(server):
    mcp, _ = server
    response = _run(
        mcp.handle_message(
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
        )
    )

    assert response["result"]["protocolVersion"] == "2024-11-05"
    assert response["result"]["serverInfo"]["name"] == "rodou-clipping-mcp"
    assert "tools" in response["result"]["capabilities"]


def test_notification_returns_none(server):
    mcp, _ = server
    response = _run(
        mcp.handle_message(
            {"jsonrpc": "2.0", "method": "notifications/initialized"}
        )
    )
    assert response is None


def test_tools_list_has_six_tools(server):
    mcp, _ = server
    response = _run(
        mcp.handle_message(
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list"}
        )
    )

    names = {tool["name"] for tool in response["result"]["tools"]}
    assert names == {
        "list_items",
        "get_item",
        "list_runs",
        "get_run",
        "list_sources",
        "trigger_run",
    }


def test_list_items_caps_size_and_truncates_abstract(server):
    mcp, client = server
    response = _run(
        mcp.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "list_items",
                    "arguments": {"date": "2026-11-14", "size": 500},
                },
            },
            auth_header="key-123",
        )
    )

    text = _content(response)
    assert text.startswith(DATA_SECURITY_BANNER)

    payload = json.loads(text[len(DATA_SECURITY_BANNER) :])
    assert payload["items"][0]["abstract"].endswith("[truncado]")
    assert len(payload["items"][0]["abstract"]) <= 1020
    assert payload["items"][1]["abstract"] == "curto"

    name, params, auth = client.calls[-1]
    assert name == "list_items"
    assert params["size"] == 50
    assert params["date"] == "2026-11-14"
    assert auth == "key-123"
    assert response["result"]["isError"] is False


def test_get_item_has_banner_and_truncation(server):
    mcp, _ = server
    response = _run(
        mcp.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {"name": "get_item", "arguments": {"item_id": "item-1"}},
            }
        )
    )

    text = _content(response)
    assert text.startswith(DATA_SECURITY_BANNER)
    item = json.loads(text[len(DATA_SECURITY_BANNER) :])
    assert item["abstract"].endswith("[truncado]")


def test_list_runs_and_get_run(server):
    mcp, _ = server

    listed = _run(
        mcp.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {"name": "list_runs", "arguments": {"date": "2026-11-14"}},
            }
        )
    )
    assert json.loads(_content(listed))["runs"][0]["status"] == "success"

    single = _run(
        mcp.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 6,
                "method": "tools/call",
                "params": {"name": "get_run", "arguments": {"run_id": "dag:run-1"}},
            }
        )
    )
    assert json.loads(_content(single))["id"] == "dag:run-1"


def test_list_sources(server):
    mcp, _ = server
    response = _run(
        mcp.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 7,
                "method": "tools/call",
                "params": {"name": "list_sources", "arguments": {}},
            }
        )
    )
    assert json.loads(_content(response))["sources"][0]["source"] == "DOU"


def test_trigger_run_uses_idempotency_key(server):
    mcp, client = server

    _run(
        mcp.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 8,
                "method": "tools/call",
                "params": {
                    "name": "trigger_run",
                    "arguments": {
                        "reference_date": "2026-11-15",
                        "sources": ["DOU"],
                        "idempotency_key": "custom-key",
                    },
                },
            }
        )
    )

    name, payload, _, idempotency_key = client.calls[-1]
    assert name == "trigger_run"
    assert payload == {"reference_date": "2026-11-15", "sources": ["DOU"]}
    assert idempotency_key == "custom-key"


def test_trigger_run_defaults_idempotency_key(server):
    mcp, client = server
    _run(
        mcp.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 9,
                "method": "tools/call",
                "params": {
                    "name": "trigger_run",
                    "arguments": {"reference_date": "2026-11-15"},
                },
            }
        )
    )

    assert client.calls[-1][3] == "clipping:mcp:2026-11-15"


def test_api_error_becomes_tool_error(server):
    mcp, client = server
    client.trigger_error = ApiClientError(403, "Escopo insuficiente", "trigger")
    response = _run(
        mcp.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 10,
                "method": "tools/call",
                "params": {
                    "name": "trigger_run",
                    "arguments": {"reference_date": "2026-11-15"},
                },
            }
        )
    )

    assert response["result"]["isError"] is True
    assert "403" in _content(response)


def test_unknown_tool_returns_error(server):
    mcp, _ = server
    response = _run(
        mcp.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 11,
                "method": "tools/call",
                "params": {"name": "does_not_exist", "arguments": {}},
            }
        )
    )
    assert response["result"]["isError"] is True


def test_unknown_method_returns_jsonrpc_error(server):
    mcp, _ = server
    response = _run(
        mcp.handle_message({"jsonrpc": "2.0", "id": 12, "method": "nope"})
    )
    assert response["error"]["code"] == -32601


def test_invalid_json_returns_parse_error(server):
    mcp, _ = server
    response = _run(mcp.handle_message("{invalid"))
    assert response["error"]["code"] == -32700
