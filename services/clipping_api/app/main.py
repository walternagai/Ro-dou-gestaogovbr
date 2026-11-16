"""API HTTP do Rodou Clipping (fase F4).

Endpoints públicos em ``/clipping/v1`` e healthcheck em ``/clipping/healthz``.
A autenticação é por ``X-API-Key`` (ou ``Authorization: Bearer``), com escopos
``clipping:read`` e ``clipping:trigger``.
"""

import logging
import threading
import time
from datetime import UTC, date, datetime
from typing import Annotated

from fastapi import Depends, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool
from starlette.exceptions import HTTPException as StarletteHTTPException

from .airflow import trigger_dag
from .auth import READ_SCOPE, TRIGGER_SCOPE, require_scope
from .catalog import SOURCE_NAMES, SOURCES, validate_sources
from .db import Database
from .http_utils import json_response
from .problems import ApiProblem
from .settings import get_settings

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Rodou Clipping API",
    version="1.0.0",
    openapi_url="/clipping/openapi.json",
    docs_url="/clipping/docs",
    redoc_url=None,
)
app.state.db = Database(get_settings().db_url)

# Cache de idempotência do disparo (por instância).
_IDEMPOTENCY: dict[str, tuple[str, str, float]] = {}
_IDEMPOTENCY_TTL_SECONDS = 24 * 3600
_IDEMPOTENCY_LOCK = threading.Lock()


# ----------------------------------------------------------------------
# Modelos
# ----------------------------------------------------------------------
class ItemModel(BaseModel):
    id: str
    source: str
    uf: str | None = None
    gazette: str | None = None
    published_date: date
    section: str | None = None
    org: str | None = None
    title: str | None = None
    abstract: str | None = None
    url: str
    matched_terms: list[str] = []
    updated_at: datetime | None = None


class ItemsResponse(BaseModel):
    reference: dict
    sources_requested: list[str]
    sources_failed: list[dict]
    total: int
    page: int
    size: int
    next_page: int | None = None
    items: list[ItemModel]


class RunModel(BaseModel):
    id: str
    dag_id: str
    reference_date: date
    status: str
    sources_requested: list[str] = []
    sources_ok: list[str] = []
    sources_failed: list[dict] = []
    items_count: int = 0
    started_at: datetime | None = None
    finished_at: datetime | None = None


class RunsResponse(BaseModel):
    runs: list[RunModel]


class SourceModel(BaseModel):
    source: str
    uf: str | None = None
    cadernos: list[str] = []
    rate_limit_rps: float = 1.0
    last_status: str | None = None
    last_items_count: int | None = None
    last_run_at: datetime | None = None


class SourcesResponse(BaseModel):
    sources: list[SourceModel]


class TriggerRequest(BaseModel):
    reference_date: date
    sources: list[str] | None = None


class TriggerResponse(BaseModel):
    run_id: str
    dag_run_id: str
    status: str = "queued"


# ----------------------------------------------------------------------
# Erros
# ----------------------------------------------------------------------
def _problem_response(
    status: int,
    title: str,
    detail: str | None = None,
    allowed: list[str] | None = None,
    headers: dict | None = None,
) -> JSONResponse:
    body = {"type": "about:blank", "title": title, "status": status}
    if detail:
        body["detail"] = detail
    if allowed:
        body["allowed"] = allowed
    return JSONResponse(
        body,
        status_code=status,
        media_type="application/problem+json",
        headers=headers or {},
    )


@app.exception_handler(ApiProblem)
async def api_problem_handler(request: Request, exc: ApiProblem):
    return _problem_response(
        exc.status, exc.title, exc.detail, exc.allowed, exc.headers
    )


@app.exception_handler(RequestValidationError)
async def validation_handler(request: Request, exc: RequestValidationError):
    first = exc.errors()[0] if exc.errors() else {}
    field = ".".join(str(part) for part in first.get("loc", [])) or "parâmetro"
    detail = f"{field}: {first.get('msg', 'valor inválido')}"
    return _problem_response(400, "Parâmetro inválido", detail=detail)


@app.exception_handler(StarletteHTTPException)
async def http_handler(request: Request, exc: StarletteHTTPException):
    return _problem_response(exc.status_code, str(exc.detail))


@app.exception_handler(Exception)
async def unexpected_handler(request: Request, exc: Exception):
    logger.exception("Erro inesperado na API do clipping")
    return _problem_response(500, "Erro interno")


# ----------------------------------------------------------------------
# Auditoria e headers de limite
# ----------------------------------------------------------------------
@app.middleware("http")
async def audit_requests(request: Request, call_next):
    started = time.perf_counter()
    response = await call_next(request)

    rate = getattr(request.state, "rate_limit", None)
    if rate:
        response.headers["X-RateLimit-Limit"] = str(rate["limit"])
        response.headers["X-RateLimit-Remaining"] = str(rate["remaining"])
        response.headers["X-RateLimit-Reset"] = str(rate["reset"])

    key_id = getattr(request.state, "key_id", None)
    if key_id and request.url.path.startswith("/clipping/v1"):
        try:
            await run_in_threadpool(
                request.app.state.db.insert_audit,
                key_id=key_id,
                route=request.url.path,
                status=response.status_code,
                latency_ms=int((time.perf_counter() - started) * 1000),
                request_id=request.headers.get("X-Request-ID"),
            )
        except Exception:
            logger.exception("Falha ao registrar auditoria da API")

    return response


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def _parse_sources(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    sources = [item.strip() for item in raw.split(",") if item.strip()]
    if not sources:
        return None
    try:
        return validate_sources(sources)
    except ValueError as exc:
        raise ApiProblem(
            400,
            "Fonte inválida",
            detail=str(exc),
            allowed=list(SOURCE_NAMES),
        ) from exc


def _parse_since(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        value = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ApiProblem(
            400,
            "Parâmetro inválido",
            detail="since deve estar em ISO-8601 (ex.: 2026-11-16T05:00:00-03:00).",
        ) from exc
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value


def _resolve_reference(
    reference: date | None, date_from: date | None, date_to: date | None
) -> tuple[date, date]:
    if reference is not None:
        if date_from or date_to:
            raise ApiProblem(
                400,
                "Parâmetro inválido",
                detail="Informe 'date' ou 'from'/'to', não ambos.",
            )
        return reference, reference

    if not (date_from and date_to):
        raise ApiProblem(
            400,
            "Parâmetro inválido",
            detail="Informe 'date' ou o par 'from'/'to'.",
        )
    if date_from > date_to:
        raise ApiProblem(
            400, "Parâmetro inválido", detail="'from' deve ser anterior a 'to'."
        )
    if (date_to - date_from).days > 31:
        raise ApiProblem(
            400,
            "Parâmetro inválido",
            detail="A janela máxima entre 'from' e 'to' é de 31 dias.",
        )
    return date_from, date_to


def _paginate(page: int, size: int) -> tuple[int, int]:
    if page < 1:
        raise ApiProblem(400, "Parâmetro inválido", detail="page deve ser >= 1.")
    if size < 1 or size > 200:
        raise ApiProblem(
            400, "Parâmetro inválido", detail="size deve estar entre 1 e 200."
        )
    return (page - 1) * size, size


# ----------------------------------------------------------------------
# Endpoints
# ----------------------------------------------------------------------
@app.get("/clipping/healthz", operation_id="health")
def health(request: Request):
    if request.app.state.db.health():
        return {"status": "ok"}
    raise ApiProblem(503, "Banco indisponível")


@app.get(
    "/clipping/v1/items",
    response_model=ItemsResponse,
    operation_id="listItems",
    dependencies=[Depends(require_scope(READ_SCOPE))],
)
def list_items(
    request: Request,
    reference: Annotated[date | None, Query(alias="date")] = None,
    date_from: Annotated[date | None, Query(alias="from")] = None,
    date_to: Annotated[date | None, Query(alias="to")] = None,
    source: str | None = None,
    uf: str | None = None,
    q: str | None = None,
    since: str | None = None,
    page: int = 1,
    size: int = 50,
):
    start, end = _resolve_reference(reference, date_from, date_to)
    sources = _parse_sources(source)
    since_dt = _parse_since(since)
    offset, limit = _paginate(page, size)

    if uf is not None:
        uf = uf.strip().upper()
        if len(uf) != 2 or not uf.isalpha():
            raise ApiProblem(
                400, "Parâmetro inválido", detail="uf deve ter duas letras."
            )

    rows, total = request.app.state.db.list_items(
        date=reference,
        date_from=None if reference else start,
        date_to=None if reference else end,
        sources=sources,
        uf=uf,
        q=q,
        since=since_dt,
        limit=limit,
        offset=offset,
    )
    summary = request.app.state.db.runs_summary(
        date_from=start, date_to=end
    )

    payload = {
        "reference": {
            "from": start.isoformat(),
            "to": end.isoformat(),
        },
        "sources_requested": summary["sources_requested"],
        "sources_failed": summary["sources_failed"],
        "total": total,
        "page": page,
        "size": size,
        "next_page": page + 1 if offset + len(rows) < total else None,
        "items": rows,
    }
    return json_response(
        request, payload, get_settings().cache_max_age_seconds
    )


@app.get(
    "/clipping/v1/items/{item_id}",
    response_model=ItemModel,
    operation_id="getItem",
    dependencies=[Depends(require_scope(READ_SCOPE))],
)
def get_item(request: Request, item_id: str):
    item = request.app.state.db.get_item(item_id)
    if item is None:
        raise ApiProblem(404, "Item não encontrado")
    return item


@app.get(
    "/clipping/v1/runs",
    response_model=RunsResponse,
    operation_id="listRuns",
    dependencies=[Depends(require_scope(READ_SCOPE))],
)
def list_runs(
    request: Request,
    reference: Annotated[date | None, Query(alias="date")] = None,
):
    return {"runs": request.app.state.db.list_runs(date=reference)}


@app.get(
    "/clipping/v1/runs/{run_id}",
    response_model=RunModel,
    operation_id="getRun",
    dependencies=[Depends(require_scope(READ_SCOPE))],
)
def get_run(request: Request, run_id: str):
    run = request.app.state.db.get_run(run_id)
    if run is None:
        raise ApiProblem(404, "Execução não encontrada")
    return run


@app.get(
    "/clipping/v1/sources",
    response_model=SourcesResponse,
    operation_id="listSources",
    dependencies=[Depends(require_scope(READ_SCOPE))],
)
def list_sources(request: Request):
    statuses = request.app.state.db.source_statuses()
    sources = []

    for source in SOURCES.values():
        status = statuses.get(source["source"], {})
        sources.append(
            {
                **source,
                "last_status": status.get("status"),
                "last_items_count": status.get("items_count"),
                "last_run_at": status.get("started_at"),
            }
        )

    return json_response(
        request,
        {"sources": sources},
        get_settings().cache_max_age_seconds,
    )


@app.post(
    "/clipping/v1/runs",
    response_model=TriggerResponse,
    status_code=202,
    operation_id="triggerRun",
    dependencies=[Depends(require_scope(TRIGGER_SCOPE))],
)
def trigger_run(request: Request, body: TriggerRequest):
    settings = get_settings()

    if body.sources:
        try:
            validate_sources(body.sources)
        except ValueError as exc:
            raise ApiProblem(
                400,
                "Fonte inválida",
                detail=str(exc),
                allowed=list(SOURCE_NAMES),
            ) from exc

    idempotency_key = request.headers.get("Idempotency-Key")
    now = time.time()
    if idempotency_key:
        with _IDEMPOTENCY_LOCK:
            cached = _IDEMPOTENCY.get(idempotency_key)
            if cached and cached[2] > now:
                return JSONResponse(
                    status_code=202,
                    content={
                        "run_id": cached[0],
                        "dag_run_id": cached[1],
                        "status": "queued",
                    },
                )

    if request.app.state.db.has_running_run(body.reference_date):
        raise ApiProblem(
            409,
            "Execução concorrente",
            detail="Já existe uma execução em andamento para esta data.",
        )

    dag_run_id = trigger_dag(
        settings,
        reference_date=body.reference_date,
        sources=body.sources,
    )
    run_id = f"{settings.dag_id}:{dag_run_id}"

    if idempotency_key:
        with _IDEMPOTENCY_LOCK:
            _IDEMPOTENCY[idempotency_key] = (
                run_id,
                dag_run_id,
                now + _IDEMPOTENCY_TTL_SECONDS,
            )

    return JSONResponse(
        status_code=202,
        content={"run_id": run_id, "dag_run_id": dag_run_id, "status": "queued"},
    )
