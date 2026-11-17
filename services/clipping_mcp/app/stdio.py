"""Transporte stdio para execução local do servidor MCP."""

import asyncio
import hashlib
import json
import logging
import sys

import httpx

from .client import ClippingApiClient
from .server import McpServer
from .settings import get_settings

# No transporte stdio, stdout é reservado para JSON-RPC; logs devem ir para stderr.
logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logger = logging.getLogger("clipping.mcp.stdio")


async def run_stdio_server(api_key: str | None = None) -> None:
    settings = get_settings()
    key = api_key or settings.api_key
    auth_header = f"Bearer {key}" if key else None
    key_id = hashlib.sha256(key.encode()).hexdigest()[:8] if key else "stdio_local"

    async with httpx.AsyncClient(timeout=30.0) as http_client:
        api_client = ClippingApiClient(settings, client=http_client)
        server = McpServer(api_client)

        logger.info("Servidor MCP iniciado no modo stdio. Aguardando mensagens...")

        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        await asyncio.get_running_loop().connect_read_pipe(lambda: protocol, sys.stdin)

        while True:
            line = await reader.readline()
            if not line:
                break

            line_str = line.decode("utf-8").strip()
            if not line_str:
                continue

            response = await server.handle_message(
                line_str, auth_header=auth_header, key_id=key_id
            )
            if response is not None:
                sys.stdout.write(json.dumps(response, ensure_ascii=False) + "\n")
                sys.stdout.flush()
