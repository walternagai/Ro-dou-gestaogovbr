"""Serviço Web FastAPI para o transporte HTTP do Model Context Protocol (MCP)."""

import hashlib
import json
import logging
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Header, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

from .client import ClippingApiClient
from .server import McpServer
from .settings import get_settings

logger = logging.getLogger("clipping.mcp")

@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield
    await _http_client.aclose()


app = FastAPI(
    title="Rodou Clipping MCP Server",
    version="1.0.0",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
    lifespan=lifespan,
)

settings = get_settings()
_http_client = httpx.AsyncClient(timeout=30.0)
api_client = ClippingApiClient(settings, client=_http_client)
mcp_server = McpServer(api_client)

# Sessões ativas para clientes SSE
_SSE_SESSIONS: dict[str, dict] = {}


@app.get("/clipping/mcp/healthz")
async def healthz():
    """Healthcheck simples do serviço MCP."""
    return {"status": "ok", "service": "rodou-clipping-mcp"}


def _extract_auth(
    x_api_key: str | None, authorization: str | None
) -> tuple[str | None, str | None]:
    token = x_api_key or authorization
    key_id = None
    if token:
        # Extrai hash de 8 caracteres para fins de log de auditoria
        key_id = hashlib.sha256(token.encode()).hexdigest()[:8]
    return token, key_id


@app.post("/clipping/mcp")
async def handle_post_mcp(
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    authorization: str | None = Header(default=None),
):
    """Transporte HTTP direto para mensagens JSON-RPC 2.0 do MCP."""
    auth_header, key_id = _extract_auth(x_api_key, authorization)

    try:
        raw_body = await request.body()
        message = json.loads(raw_body.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001 - corpo inválido vira erro JSON-RPC
        return JSONResponse(
            {
                "jsonrpc": "2.0",
                "id": None,
                "error": {"code": -32700, "message": f"Parse error: {exc}"},
            },
            status_code=400,
        )

    response = await mcp_server.handle_message(
        message, auth_header=auth_header, key_id=key_id
    )

    if response is None:
        # Notificações em JSON-RPC retornam 204 No Content
        return JSONResponse(content={}, status_code=204)

    return JSONResponse(content=response, status_code=200)


@app.get("/clipping/mcp")
@app.get("/clipping/mcp/sse")
async def handle_sse(
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    authorization: str | None = Header(default=None),
):
    """Transporte SSE (Server-Sent Events) padrão da especificação MCP."""
    auth_header, key_id = _extract_auth(x_api_key, authorization)
    session_id = str(uuid.uuid4())
    _SSE_SESSIONS[session_id] = {"auth": auth_header, "key_id": key_id}

    async def event_generator():
        # Informa ao cliente o endpoint POST para envio de mensagens
        endpoint_url = f"/clipping/mcp/messages?sessionId={session_id}"
        yield f"event: endpoint\ndata: {endpoint_url}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/clipping/mcp/messages")
async def handle_sse_messages(
    request: Request,
    session_id: str = Query(..., alias="sessionId"),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    authorization: str | None = Header(default=None),
):
    """Endpoint de mensagens para sessões SSE."""
    session = _SSE_SESSIONS.get(session_id)
    auth_header, key_id = _extract_auth(x_api_key, authorization)
    if not auth_header and session:
        auth_header = session.get("auth")
        key_id = session.get("key_id")

    try:
        raw_body = await request.body()
        message = json.loads(raw_body.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001 - corpo inválido vira erro JSON-RPC
        return JSONResponse(
            {
                "jsonrpc": "2.0",
                "id": None,
                "error": {"code": -32700, "message": f"Parse error: {exc}"},
            },
            status_code=400,
        )

    response = await mcp_server.handle_message(
        message, auth_header=auth_header, key_id=key_id
    )

    if response is None:
        return JSONResponse(content={}, status_code=204)

    return JSONResponse(content=response, status_code=200)
