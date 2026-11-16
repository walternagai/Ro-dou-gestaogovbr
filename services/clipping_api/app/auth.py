"""Autenticação por chave de API, escopos e limite de requisições."""

import hashlib
import time
from collections import defaultdict, deque

from fastapi import Header, Request

from .db import DatabaseUnavailable
from .problems import ApiProblem
from .settings import get_settings

READ_SCOPE = "clipping:read"
TRIGGER_SCOPE = "clipping:trigger"


def hash_key(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def _bearer(authorization: str | None) -> str | None:
    if not authorization:
        return None
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        return None
    return token.strip()


class RateLimiter:
    """Janela deslizante em memória, por chave e escopo."""

    def __init__(self):
        self._events: dict[tuple[str, str], deque] = defaultdict(deque)

    def allow(self, key_id: str, scope: str, limit: int, window: int):
        now = time.monotonic()
        events = self._events[(key_id, scope)]

        while events and now - events[0] >= window:
            events.popleft()

        if len(events) >= limit:
            retry_after = max(1, int(window - (now - events[0])) + 1)
            return False, 0, retry_after

        events.append(now)
        return True, limit - len(events), None

    def reset(self) -> None:
        self._events.clear()


rate_limiter = RateLimiter()


def require_scope(scope: str):
    """Dependency que valida a chave, o escopo e o limite de requisições."""

    def dependency(
        request: Request,
        x_api_key: str | None = Header(default=None, alias="X-API-Key"),
        authorization: str | None = Header(default=None),
    ) -> dict:
        raw_key = x_api_key or _bearer(authorization)
        if not raw_key:
            raise ApiProblem(
                401,
                "Chave de API ausente",
                detail="Envie o header X-API-Key (ou Authorization: Bearer).",
            )

        try:
            key = request.app.state.db.get_api_key(hash_key(raw_key))
        except DatabaseUnavailable as exc:
            raise ApiProblem(503, "Banco indisponível", detail=str(exc)) from exc

        if not key or not key.get("active"):
            raise ApiProblem(403, "Chave inválida ou inativa")

        if scope not in (key.get("scope") or []):
            raise ApiProblem(
                403,
                "Escopo insuficiente",
                detail=f"A chave precisa do escopo '{scope}'.",
            )

        settings = get_settings()
        if scope == TRIGGER_SCOPE:
            limit, window = settings.trigger_limit_per_hour, 3600
        else:
            limit, window = settings.read_limit_per_minute, 60

        allowed, remaining, retry_after = rate_limiter.allow(
            key["id"], scope, limit, window
        )

        request.state.key_id = key["id"]
        request.state.rate_limit = {
            "limit": limit,
            "remaining": remaining,
            "reset": int(time.time()) + window,
        }

        if not allowed:
            raise ApiProblem(
                429,
                "Limite de requisições excedido",
                detail=f"Limite de {limit} requisições por {window} segundos.",
                headers={"Retry-After": str(retry_after)},
            )

        return key

    return dependency
