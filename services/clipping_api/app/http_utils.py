"""Utilitários de resposta JSON com ETag e cache curto."""

import hashlib
import json
from datetime import date, datetime

from fastapi import Request
from fastapi.responses import Response


def _default(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"Tipo não serializável: {type(value)!r}")


def json_response(request: Request, payload: dict, max_age: int) -> Response:
    """Serializa uma resposta estável e aplica ETag/If-None-Match."""
    body = json.dumps(
        payload,
        default=_default,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    etag = '"' + hashlib.sha256(body.encode("utf-8")).hexdigest() + '"'
    cache_control = f"private, max-age={max_age}"

    if request.headers.get("if-none-match") == etag:
        return Response(
            status_code=304,
            headers={"ETag": etag, "Cache-Control": cache_control},
        )

    return Response(
        content=body,
        media_type="application/json",
        headers={"ETag": etag, "Cache-Control": cache_control},
    )
