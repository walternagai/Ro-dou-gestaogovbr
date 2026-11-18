"""API HTTP do Rodou Clipping.

Endpoints versionados em ``/clipping/v1`` e sondas de saúde em
``/clipping/healthz``, ``/clipping/livez`` e ``/clipping/readyz``.
Métricas Prometheus ficam restritas à rota interna ``/internal/metrics``.
"""

import base64
import json
import threading
import time
import uuid
from datetime import UTC, date, datetime, timedelta
from datetime import date as DateType
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, Header, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field
from starlette.concurrency import run_in_threadpool
from starlette.exceptions import HTTPException as StarletteHTTPException

from .airflow import airflow_health, trigger_dag
from .auth import READ_SCOPE, TRIGGER_SCOPE, require_scope
from .catalog import (
    SOURCE_NAMES,
    SOURCES,
    RunStatus,
    SourceId,
    SourceLastStatus,
    UfCode,
    enabled_ufs,
    sources_for_uf,
    validate_sources,
)
from .db import Database
from .http_utils import json_response
from .logging_config import logger
from .metrics import metrics
from .problems import ApiProblem, ClippingErrorCode, ProblemDetail
from .sanitize import sanitize_item
from .settings import get_settings

API_DESCRIPTION = """
API HTTP de consulta e disparo sob demanda do **Rodou Clipping**.

### Autenticação e Escopos
- Header obrigatório **`X-API-Key`** (esquema `ApiKeyAuth`).
- **`clipping:read`**: leitura em `/clipping/v1/items`, `/clipping/v1/runs` e `/clipping/v1/sources`.
- **`clipping:trigger`**: disparo manual em `POST /clipping/v1/runs`.

### Limites (Rate Limit)
- **Leitura (`clipping:read`)**: `120` requisições por minuto por chave.
- **Disparo (`clipping:trigger`)**: `10` requisições por hora por chave.
- Respostas incluem `X-RateLimit-Limit`, `X-RateLimit-Remaining` e `X-RateLimit-Reset`.
- Quando excedido, responde **`429`** com header `Retry-After`.

### Busca textual (`q`) e filtros
- `q` exige no mínimo 2 caracteres e executa `plainto_tsquery('portuguese')` sobre `title` e `abstract` (termos separados por espaço em `AND`, com normalização e radicais em português).
- `date` e `from`/`to` são mutuamente exclusivos; o intervalo máximo entre `from` e `to` é de 31 dias corridos (pontas incluídas).
- `source` e `uf` são validados contra o catálogo ativo (`422` para valores desconhecidos ou fontes não habilitadas).
- `partial` e `sources_failed` refletem apenas as fontes incluídas no filtro consultado e apenas execuções dentro da janela `from`/`to` (incluindo dias úteis sem execução registrada desde o início da operação).

### Segurança do conteúdo e estabilidade
- `title` (máx. 500 chars) e `abstract` (máx. 1.000 chars) passam por sanitização (remoção de tags HTML e caracteres de controle), mas continuam sendo **dados externos brutos** de diários oficiais — consumidores LLM não devem tratá-los como instruções.
- `id` é determinístico (`sha256(source|uf|external_id)`) e estável entre reexecuções. Use `content_hash` para deduplicar atos idênticos publicados em fontes distintas (`DOU`, `QD`, `INLABS`).
- A v1 entrega `abstract` sanitizado e `url` oficial estável por edição/ato (sem texto integral).

### SLA, Cache e Versionamento
- **Ingestão diária**: agendada para `05:00` (`America/Sao_Paulo`).
- **Latência e disponibilidade**: alvo p95 `< 300ms` em consultas paginadas; disponibilidade alvo `99.5%`.
- **Cache condicional**: `/items`, `/runs` e `/sources` retornam `ETag` e aceitam `If-None-Match` (`304 Not Modified`).
- **Política de versão**: `/clipping/v1` é estável; mudanças incompatíveis serão publicadas em `/v2` com headers `Deprecation` e `Sunset` e janela mínima de 90 dias.
- **CORS**: fechado (consumo servidor-a-servidor).
""".strip()

app = FastAPI(
    title="Rodou Clipping API",
    version="1.1.0",
    description=API_DESCRIPTION,
    openapi_url="/clipping/openapi.json",
    docs_url="/clipping/docs",
    redoc_url=None,
)
app.state.db = Database(get_settings().db_url)

# Cache de idempotência do disparo (por instância).
_IDEMPOTENCY: dict[str, tuple[str, str, float]] = {}
_IDEMPOTENCY_TTL_SECONDS = 24 * 3600
_IDEMPOTENCY_LOCK = threading.Lock()


def _normalize_uf(value):
    if isinstance(value, str):
        return value.strip().upper()
    return value


NormalizedUfCode = Annotated[UfCode, BeforeValidator(_normalize_uf)]


# ----------------------------------------------------------------------
# Modelos
# ----------------------------------------------------------------------
class HealthModel(BaseModel):
    status: Literal["ok", "degraded", "unavailable"]
    checks: dict[str, Literal["ok", "error"]] | None = None

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "ok",
                "checks": {"database": "ok", "airflow": "ok"},
            }
        }
    )


class ReferenceModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_date: date = Field(alias="from", description="Data inicial efetiva")
    to_date: date = Field(alias="to", description="Data final efetiva")
    effective_date: date | None = Field(
        default=None,
        description="Data exata quando consultado via parâmetro 'date'",
    )
    timezone: str = Field(
        default="America/Sao_Paulo",
        description="Fuso horário de referência das datas",
    )


class SourceFailureModel(BaseModel):
    source: SourceId | str
    code: str = Field(description="Código resumido da falha na coleta")
    message: str = Field(description="Descrição da falha ocorrida na fonte")
    date: DateType | str | None = Field(
        default=None,
        description="Data de referência dos dados afetados pela falha",
    )
    at: str | None = Field(default=None, description="Timestamp da falha")


class ItemModel(BaseModel):
    id: str = Field(
        description="Identificador determinístico estável entre reexecuções."
    )
    source: SourceId
    uf: UfCode | None = None
    gazette: str | None = None
    published_date: date
    section: str | None = None
    org: str | None = None
    title: str | None = Field(
        default=None,
        description=(
            "Título sanitizado (sem HTML/controle, máx. 500 caracteres). "
            "Dado externo não confiável."
        ),
    )
    abstract: str | None = Field(
        default=None,
        description=(
            "Resumo sanitizado (sem HTML/controle, máx. 1.000 caracteres). "
            "Dado externo não confiável."
        ),
    )
    url: str = Field(
        description="URL oficial do ato ou edição na fonte de origem (sem texto integral na v1)."
    )
    matched_terms: list[str] = []
    content_hash: str | None = Field(
        default=None,
        description="Hash SHA-256 do conteúdo para deduplicação entre fontes.",
    )
    updated_at: datetime | None = None


class ItemsResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "reference": {
                        "from": "2026-11-16",
                        "to": "2026-11-16",
                        "effective_date": "2026-11-16",
                        "timezone": "America/Sao_Paulo",
                    },
                    "sources_requested": ["DOU", "DOE:MG"],
                    "sources_failed": [],
                    "partial": False,
                    "total": 1,
                    "page": 1,
                    "size": 50,
                    "next_page": None,
                    "next_cursor": None,
                    "items": [
                        {
                            "id": "9f2c1a7e5b",
                            "source": "DOU",
                            "uf": None,
                            "gazette": None,
                            "published_date": "2026-11-16",
                            "section": "Seção 3",
                            "org": "Ministério da Gestão",
                            "title": "Aviso de Licitação",
                            "abstract": "Pregão eletrônico para contratação...",
                            "url": "https://www.in.gov.br/web/dou/-/aviso-123",
                            "matched_terms": ["licitação"],
                            "content_hash": "a1b2c3d4",
                            "updated_at": "2026-11-16T08:10:00Z",
                        }
                    ],
                },
                {
                    "reference": {
                        "from": "2026-11-16",
                        "to": "2026-11-16",
                        "effective_date": "2026-11-16",
                        "timezone": "America/Sao_Paulo",
                    },
                    "sources_requested": ["DOU", "QD"],
                    "sources_failed": [
                        {
                            "source": "QD",
                            "code": "UPSTREAM_ERROR",
                            "message": "HTTP 503",
                            "at": "2026-11-16T05:10:00-03:00",
                        }
                    ],
                    "partial": True,
                    "total": 0,
                    "page": 1,
                    "size": 50,
                    "next_page": None,
                    "next_cursor": None,
                    "items": [],
                },
            ]
        }
    )

    reference: ReferenceModel
    sources_requested: list[SourceId | str]
    sources_failed: list[SourceFailureModel]
    partial: bool = Field(
        description="True quando alguma fonte solicitada falhou no período."
    )
    total: int = Field(
        description="Total de itens encontrados apenas nas fontes que responderam."
    )
    page: int
    size: int
    next_page: int | None = None
    next_cursor: str | None = Field(
        default=None,
        description="Cursor opaco para paginação estável por keyset (published_date DESC, id ASC).",
    )
    items: list[ItemModel]


class RunModel(BaseModel):
    id: str
    dag_id: str
    reference_date: date
    status: RunStatus
    sources_requested: list[SourceId | str] = []
    sources_ok: list[SourceId | str] = []
    sources_failed: list[SourceFailureModel] = []
    items_count: int = 0
    started_at: datetime | None = None
    finished_at: datetime | None = None


class RunsResponse(BaseModel):
    runs: list[RunModel]


class SourceModel(BaseModel):
    source: SourceId
    uf: UfCode | None = None
    cadernos: list[str] = []
    rate_limit_rps: float = 1.0
    last_status: SourceLastStatus | None = None
    last_items_count: int | None = None
    last_run_at: datetime | None = None
    last_success_at: datetime | None = None
    stale: bool = Field(
        default=True,
        description="True quando não há ingestão bem-sucedida recente dentro da janela configurada.",
    )


class SourcesResponse(BaseModel):
    sources: list[SourceModel]


class TriggerRequest(BaseModel):
    reference_date: date
    sources: list[SourceId] | None = None


class TriggerResponse(BaseModel):
    run_id: str
    dag_run_id: str
    status: RunStatus = "queued"


# ----------------------------------------------------------------------
# Documentação de respostas de erro no OpenAPI
# ----------------------------------------------------------------------
_PROBLEM_RESPONSE = {
    "model": ProblemDetail,
    "content": {"application/problem+json": {}},
}
_READ_RESPONSES = {
    304: {"description": "Não modificado (ETag correspondente)"},
    401: {**_PROBLEM_RESPONSE, "description": "Credencial ausente"},
    403: {
        **_PROBLEM_RESPONSE,
        "description": "Chave inválida, revogada ou sem escopo suficiente",
    },
    422: {**_PROBLEM_RESPONSE, "description": "Parâmetros inválidos"},
    429: {**_PROBLEM_RESPONSE, "description": "Limite de requisições excedido"},
    503: {**_PROBLEM_RESPONSE, "description": "Banco indisponível"},
}
_ITEM_RESPONSES = {
    key: value for key, value in _READ_RESPONSES.items() if key != 304
} | {
    404: {**_PROBLEM_RESPONSE, "description": "Recurso não encontrado"},
}
_TRIGGER_RESPONSES = {
    400: {**_PROBLEM_RESPONSE, "description": "Corpo JSON inválido"},
    401: _READ_RESPONSES[401],
    403: _READ_RESPONSES[403],
    409: {**_PROBLEM_RESPONSE, "description": "Execução concorrente em andamento"},
    422: _READ_RESPONSES[422],
    429: _READ_RESPONSES[429],
    503: {
        **_PROBLEM_RESPONSE,
        "description": "Disparo indisponível ou Airflow inacessível",
    },
}


# ----------------------------------------------------------------------
# Erros
# ----------------------------------------------------------------------
def _problem_response(
    status: int,
    title: str,
    detail: str | None = None,
    allowed: list[str] | None = None,
    headers: dict | None = None,
    code: ClippingErrorCode | None = None,
    request_id: str | None = None,
) -> JSONResponse:
    resolved_code = code or {
        400: ClippingErrorCode.BAD_REQUEST,
        401: ClippingErrorCode.UNAUTHORIZED,
        403: ClippingErrorCode.FORBIDDEN,
        404: ClippingErrorCode.NOT_FOUND,
        409: ClippingErrorCode.CONFLICT,
        422: ClippingErrorCode.VALIDATION_ERROR,
        429: ClippingErrorCode.RATE_LIMITED,
        503: ClippingErrorCode.SERVICE_UNAVAILABLE,
    }.get(status, ClippingErrorCode.INTERNAL_ERROR)

    body = {
        "type": "about:blank",
        "title": title,
        "status": status,
        "code": resolved_code.value,
        "message": detail or title,
    }
    if detail:
        body["detail"] = detail
    if allowed:
        body["allowed"] = allowed
    if request_id:
        body["request_id"] = request_id

    merged_headers = dict(headers or {})
    if request_id and "X-Request-Id" not in merged_headers:
        merged_headers["X-Request-Id"] = request_id

    return JSONResponse(
        body,
        status_code=status,
        media_type="application/problem+json",
        headers=merged_headers,
    )


@app.exception_handler(ApiProblem)
async def api_problem_handler(request: Request, exc: ApiProblem):
    return _problem_response(
        exc.status,
        exc.title,
        exc.detail,
        exc.allowed,
        exc.headers,
        code=exc.code,
        request_id=getattr(request.state, "request_id", None),
    )


@app.exception_handler(RequestValidationError)
async def validation_handler(request: Request, exc: RequestValidationError):
    errors = exc.errors()
    first = errors[0] if errors else {}
    is_json_decode = any(err.get("type") == "json_invalid" for err in errors)
    status = 400 if is_json_decode else 422
    title = "Corpo JSON inválido" if is_json_decode else "Parâmetro inválido"

    loc = [str(part) for part in first.get("loc", [])]
    field = ".".join(loc) or "parâmetro"
    detail = f"{field}: {first.get('msg', 'valor inválido')}"

    allowed = None
    settings = get_settings()
    if "sources" in loc:
        allowed = list(settings.enabled_sources)
    elif "uf" in loc:
        allowed = list(enabled_ufs(settings.enabled_sources))

    return _problem_response(
        status,
        title,
        detail=detail,
        allowed=allowed,
        code=(
            ClippingErrorCode.BAD_REQUEST
            if is_json_decode
            else ClippingErrorCode.VALIDATION_ERROR
        ),
        request_id=getattr(request.state, "request_id", None),
    )


@app.exception_handler(StarletteHTTPException)
async def http_handler(request: Request, exc: StarletteHTTPException):
    return _problem_response(
        exc.status_code,
        str(exc.detail),
        request_id=getattr(request.state, "request_id", None),
    )


@app.exception_handler(Exception)
async def unexpected_handler(request: Request, exc: Exception):
    logger.exception(
        "Erro inesperado na API do clipping",
        extra={"request_id": getattr(request.state, "request_id", None)},
    )
    return _problem_response(
        500,
        "Erro interno",
        code=ClippingErrorCode.INTERNAL_ERROR,
        request_id=getattr(request.state, "request_id", None),
    )


# ----------------------------------------------------------------------
# Observabilidade, auditoria e headers de limite
# ----------------------------------------------------------------------
@app.middleware("http")
async def audit_requests(request: Request, call_next):
    started = time.perf_counter()
    request_id = (request.headers.get("X-Request-Id") or "").strip() or str(
        uuid.uuid4()
    )
    traceparent = (request.headers.get("traceparent") or "").strip() or None
    request.state.request_id = request_id
    request.state.traceparent = traceparent

    if request.url.path in ("/clipping/openapi.json", "/clipping/docs"):
        if not get_settings().expose_docs:
            response = _problem_response(
                404,
                "Documentação desabilitada",
                code=ClippingErrorCode.NOT_FOUND,
                request_id=request_id,
            )
        else:
            response = await call_next(request)
    else:
        response = await call_next(request)

    response.headers["X-Request-Id"] = request_id

    rate = getattr(request.state, "rate_limit", None)
    if rate:
        response.headers["X-RateLimit-Limit"] = str(rate["limit"])
        response.headers["X-RateLimit-Remaining"] = str(rate["remaining"])
        response.headers["X-RateLimit-Reset"] = str(rate["reset"])

    key_id = getattr(request.state, "key_id", None)
    latency_ms = int((time.perf_counter() - started) * 1000)
    route = _normalize_route(request.url.path)

    if route.startswith("/clipping"):
        metrics.record(route, response.status_code, latency_ms)
        logger.info(
            "request",
            extra={
                "event": "request",
                "route": route,
                "status": response.status_code,
                "latency_ms": latency_ms,
                "key_id": key_id,
                "request_id": request_id,
                "traceparent": traceparent,
            },
        )

    if key_id and request.url.path.startswith("/clipping/v1"):
        try:
            await run_in_threadpool(
                request.app.state.db.insert_audit,
                key_id=key_id,
                route=request.url.path,
                status=response.status_code,
                latency_ms=latency_ms,
                request_id=request_id,
            )
        except Exception:  # noqa: BLE001 - auditoria não pode derrubar a resposta
            logger.exception(
                "Falha ao registrar auditoria da API",
                extra={"request_id": request_id},
            )

    return response


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def _parse_sources(
    raw: str | None,
    *,
    allowed_sources: tuple[str, ...] = SOURCE_NAMES,
) -> list[str] | None:
    if not raw:
        return None
    sources = [item.strip() for item in raw.split(",") if item.strip()]
    if not sources:
        return None
    try:
        return validate_sources(sources, allowed_sources=allowed_sources)
    except ValueError as exc:
        raise ApiProblem(
            422,
            "Fonte inválida",
            detail=str(exc),
            allowed=list(allowed_sources),
            code=ClippingErrorCode.VALIDATION_ERROR,
        ) from exc


def _resolve_reference(
    reference: date | None, date_from: date | None, date_to: date | None
) -> tuple[date, date]:
    if reference is not None:
        if date_from or date_to:
            raise ApiProblem(
                422,
                "Parâmetro inválido",
                detail="Informe 'date' ou 'from'/'to', não ambos.",
                code=ClippingErrorCode.VALIDATION_ERROR,
            )
        return reference, reference

    if not (date_from and date_to):
        raise ApiProblem(
            422,
            "Parâmetro inválido",
            detail="Informe 'date' ou o par 'from'/'to'.",
            code=ClippingErrorCode.VALIDATION_ERROR,
        )
    if date_from > date_to:
        raise ApiProblem(
            422,
            "Parâmetro inválido",
            detail="'from' deve ser anterior a 'to'.",
            code=ClippingErrorCode.VALIDATION_ERROR,
        )
    if (date_to - date_from).days + 1 > 31:
        raise ApiProblem(
            422,
            "Parâmetro inválido",
            detail="A janela máxima entre 'from' e 'to' é de 31 dias.",
            code=ClippingErrorCode.VALIDATION_ERROR,
        )
    return date_from, date_to


def _normalize_route(path: str) -> str:
    for prefix, normalized in (
        ("/clipping/v1/items/", "/clipping/v1/items/{id}"),
        ("/clipping/v1/runs/", "/clipping/v1/runs/{id}"),
    ):
        if path.startswith(prefix) and path != prefix:
            return normalized
    return path


def _encode_cursor(published_date: date | str, item_id: str) -> str:
    date_str = (
        published_date.isoformat()
        if isinstance(published_date, date)
        else str(published_date)
    )
    raw = json.dumps({"d": date_str, "id": item_id}, separators=(",", ":")).encode(
        "utf-8"
    )
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode_cursor(token: str) -> tuple[date, str]:
    try:
        padding = "=" * (-len(token) % 4)
        raw = base64.urlsafe_b64decode(token + padding).decode("utf-8")
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise TypeError("formato inválido")
        d_raw = data.get("d")
        item_id = data.get("id")
        if not isinstance(d_raw, str) or not isinstance(item_id, str) or not item_id:
            raise ValueError("campos ausentes")
        return date.fromisoformat(d_raw), item_id
    except Exception as exc:
        raise ApiProblem(
            422,
            "Cursor inválido",
            detail="O parâmetro 'cursor' está malformado ou inválido.",
            code=ClippingErrorCode.VALIDATION_ERROR,
        ) from exc


def _is_stale(last_success_at: datetime | None, stale_after_hours: int) -> bool:
    if last_success_at is None:
        return True
    ts = (
        last_success_at.replace(tzinfo=UTC)
        if last_success_at.tzinfo is None
        else last_success_at.astimezone(UTC)
    )
    return (datetime.now(UTC) - ts) > timedelta(hours=stale_after_hours)


# ----------------------------------------------------------------------
# Endpoints públicos (sondas) e internos (métricas)
# ----------------------------------------------------------------------
@app.get(
    "/clipping/livez",
    response_model=HealthModel,
    operation_id="livez",
    tags=["Observabilidade"],
)
def livez():
    return {"status": "ok"}


@app.get(
    "/clipping/healthz",
    response_model=HealthModel,
    operation_id="health",
    tags=["Observabilidade"],
    responses={503: _READ_RESPONSES[503]},
)
def health(request: Request):
    if request.app.state.db.health():
        return {"status": "ok", "checks": {"database": "ok"}}
    raise ApiProblem(
        503,
        "Banco indisponível",
        code=ClippingErrorCode.SERVICE_UNAVAILABLE,
    )


@app.get(
    "/clipping/readyz",
    response_model=HealthModel,
    operation_id="readyz",
    tags=["Observabilidade"],
    responses={503: _READ_RESPONSES[503]},
)
def readyz(request: Request):
    settings = get_settings()
    db_ok = request.app.state.db.health()
    af_ok = airflow_health(settings)
    checks = {
        "database": "ok" if db_ok else "error",
        "airflow": "ok" if af_ok else "error",
    }
    if db_ok and af_ok:
        return {"status": "ok", "checks": checks}

    failing = [name for name, state in checks.items() if state != "ok"]
    raise ApiProblem(
        503,
        "Dependência indisponível",
        detail=f"Dependências indisponíveis: {', '.join(failing)}.",
        code=ClippingErrorCode.SERVICE_UNAVAILABLE,
    )


@app.get("/internal/metrics", include_in_schema=False)
def metrics_endpoint(request: Request):
    try:
        statuses = request.app.state.db.source_statuses()
    except Exception:  # noqa: BLE001 - métricas não devem falhar por banco
        statuses = {}
    return PlainTextResponse(
        metrics.render(statuses),
        media_type="text/plain; version=0.0.4",
    )


# ----------------------------------------------------------------------
# Endpoints v1
# ----------------------------------------------------------------------
@app.get(
    "/clipping/v1/items",
    response_model=ItemsResponse,
    operation_id="listItems",
    dependencies=[Depends(require_scope(READ_SCOPE))],
    responses=_READ_RESPONSES,
    tags=["Itens"],
)
def list_items(
    request: Request,
    reference: Annotated[
        date | None,
        Query(alias="date", description="Data de publicação exata (AAAA-MM-DD)."),
    ] = None,
    date_from: Annotated[
        date | None,
        Query(alias="from", description="Data inicial do período (AAAA-MM-DD)."),
    ] = None,
    date_to: Annotated[
        date | None,
        Query(
            alias="to",
            description="Data final do período (AAAA-MM-DD, máx. 31 dias corridos contando as pontas).",
        ),
    ] = None,
    source: Annotated[
        str | None,
        Query(
            description=(
                "Fontes separadas por vírgula (DOU, QD, INLABS, DOE:SP, DOE:MG)."
            )
        ),
    ] = None,
    uf: Annotated[
        NormalizedUfCode | None,
        Query(description="Sigla da UF suportada (MG, SP)."),
    ] = None,
    q: Annotated[
        str | None,
        Query(
            min_length=2,
            max_length=200,
            description=(
                "Busca textual em título e resumo (plainto_tsquery em português; "
                "mínimo 2 caracteres; termos em AND)."
            ),
        ),
    ] = None,
    since: Annotated[
        datetime | None,
        Query(description="Timestamp ISO-8601 para delta por updated_at."),
    ] = None,
    page: Annotated[
        int,
        Query(ge=1, description="Número da página (1..N, ignorado com cursor)."),
    ] = 1,
    size: Annotated[
        int,
        Query(ge=1, le=200, description="Quantidade de itens por página (1 a 200)."),
    ] = 50,
    cursor: Annotated[
        str | None,
        Query(
            description=(
                "Cursor opaco retornado em next_cursor para paginação estável por keyset."
            )
        ),
    ] = None,
):
    settings = get_settings()
    start, end = _resolve_reference(reference, date_from, date_to)
    sources = _parse_sources(source, allowed_sources=settings.enabled_sources)

    allowed_ufs = enabled_ufs(settings.enabled_sources)
    if uf is not None and uf not in allowed_ufs:
        raise ApiProblem(
            422,
            "Parâmetro inválido",
            detail=f"UF não habilitada: {uf}. Valores aceitos: {', '.join(allowed_ufs)}.",
            allowed=list(allowed_ufs),
            code=ClippingErrorCode.VALIDATION_ERROR,
        )

    if sources is not None and uf is not None:
        summary_sources: list[str] | None = [
            s for s in sources if SOURCES[s].get("uf") == uf
        ]
    elif sources is not None:
        summary_sources = sources
    elif uf is not None:
        summary_sources = sources_for_uf(uf, settings.enabled_sources)
    else:
        summary_sources = None

    if q is not None:
        q = q.strip()
        if len(q) < 2:
            raise ApiProblem(
                422,
                "Parâmetro inválido",
                detail="q deve ter ao menos 2 caracteres.",
                code=ClippingErrorCode.VALIDATION_ERROR,
            )

    if since is not None and since.tzinfo is None:
        since = since.replace(tzinfo=UTC)

    decoded_cursor = None
    if cursor is not None:
        if page != 1:
            raise ApiProblem(
                422,
                "Parâmetro inválido",
                detail="Não combine 'cursor' com 'page' maior que 1.",
                code=ClippingErrorCode.VALIDATION_ERROR,
            )
        decoded_cursor = _decode_cursor(cursor)

    offset = (page - 1) * size
    rows, total, has_more = request.app.state.db.list_items(
        date=reference,
        date_from=None if reference else start,
        date_to=None if reference else end,
        sources=sources,
        uf=uf,
        q=q,
        since=since,
        limit=size,
        offset=offset,
        cursor=decoded_cursor,
    )
    sanitized_rows = [sanitize_item(row) for row in rows]
    summary = request.app.state.db.runs_summary(
        date_from=start,
        date_to=end,
        sources=summary_sources,
        enabled_sources=settings.enabled_sources,
    )

    next_cursor = (
        _encode_cursor(
            sanitized_rows[-1]["published_date"], sanitized_rows[-1]["id"]
        )
        if has_more and sanitized_rows
        else None
    )
    next_page = page + 1 if decoded_cursor is None and has_more else None

    payload = {
        "reference": {
            "from": start.isoformat(),
            "to": end.isoformat(),
            "effective_date": reference.isoformat() if reference else None,
            "timezone": "America/Sao_Paulo",
        },
        "sources_requested": summary["sources_requested"],
        "sources_failed": summary["sources_failed"],
        "partial": bool(summary["sources_failed"]),
        "total": total,
        "page": page,
        "size": size,
        "next_page": next_page,
        "next_cursor": next_cursor,
        "items": sanitized_rows,
    }
    return json_response(request, payload, settings.cache_max_age_seconds)


@app.get(
    "/clipping/v1/items/{item_id}",
    response_model=ItemModel,
    operation_id="getItem",
    dependencies=[Depends(require_scope(READ_SCOPE))],
    responses=_ITEM_RESPONSES,
    tags=["Itens"],
)
def get_item(request: Request, item_id: str):
    item = request.app.state.db.get_item(item_id)
    if item is None:
        raise ApiProblem(
            404,
            "Item não encontrado",
            code=ClippingErrorCode.NOT_FOUND,
        )
    return sanitize_item(item)


@app.get(
    "/clipping/v1/runs",
    response_model=RunsResponse,
    operation_id="listRuns",
    dependencies=[Depends(require_scope(READ_SCOPE))],
    responses=_READ_RESPONSES,
    tags=["Execuções"],
)
def list_runs(
    request: Request,
    reference: Annotated[
        date | None,
        Query(alias="date", description="Filtrar execuções pela data de referência."),
    ] = None,
):
    runs = request.app.state.db.list_runs(date=reference)
    return json_response(
        request,
        {"runs": runs},
        get_settings().cache_max_age_seconds,
    )


@app.get(
    "/clipping/v1/runs/{run_id}",
    response_model=RunModel,
    operation_id="getRun",
    dependencies=[Depends(require_scope(READ_SCOPE))],
    responses=_ITEM_RESPONSES,
    tags=["Execuções"],
)
def get_run(request: Request, run_id: str):
    run = request.app.state.db.get_run(run_id)
    if run is None:
        raise ApiProblem(
            404,
            "Execução não encontrada",
            code=ClippingErrorCode.NOT_FOUND,
        )
    return run


@app.get(
    "/clipping/v1/sources",
    response_model=SourcesResponse,
    operation_id="listSources",
    dependencies=[Depends(require_scope(READ_SCOPE))],
    responses=_READ_RESPONSES,
    tags=["Fontes"],
)
def list_sources(request: Request):
    settings = get_settings()
    statuses = request.app.state.db.source_statuses()
    sources = []

    for source in SOURCES.values():
        if source["source"] not in settings.enabled_sources:
            continue
        status = statuses.get(source["source"], {})
        last_success_at = status.get("last_success_at")
        sources.append(
            {
                **source,
                "last_status": status.get("status"),
                "last_items_count": status.get("items_count"),
                "last_run_at": status.get("started_at"),
                "last_success_at": last_success_at,
                "stale": _is_stale(last_success_at, settings.stale_after_hours),
            }
        )

    return json_response(
        request,
        {"sources": sources},
        settings.cache_max_age_seconds,
    )


@app.post(
    "/clipping/v1/runs",
    response_model=TriggerResponse,
    status_code=202,
    operation_id="triggerRun",
    dependencies=[Depends(require_scope(TRIGGER_SCOPE))],
    responses=_TRIGGER_RESPONSES,
    tags=["Execuções"],
)
def trigger_run(
    request: Request,
    body: TriggerRequest,
    idempotency_key: Annotated[
        str | None,
        Header(
            alias="Idempotency-Key",
            description=(
                "Chave opcional para tornar o disparo idempotente por até 24h."
            ),
        ),
    ] = None,
):
    settings = get_settings()

    if body.sources:
        try:
            validate_sources(
                list(body.sources), allowed_sources=settings.enabled_sources
            )
        except ValueError as exc:
            raise ApiProblem(
                422,
                "Fonte inválida",
                detail=str(exc),
                allowed=list(settings.enabled_sources),
                code=ClippingErrorCode.VALIDATION_ERROR,
            ) from exc

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
            code=ClippingErrorCode.CONFLICT,
        )

    dag_run_id = trigger_dag(
        settings,
        reference_date=body.reference_date,
        sources=list(body.sources) if body.sources else None,
    )
    run_id = f"{settings.dag_id}:{dag_run_id}"

    try:
        request.app.state.db.create_queued_run(
            run_id=run_id,
            dag_id=settings.dag_id,
            reference_date=body.reference_date,
            sources=body.sources,
        )
    except Exception:  # noqa: BLE001 - a run já foi disparada; não falhar o 202
        logger.exception(
            "Falha ao registrar a run queued do clipping",
            extra={"request_id": getattr(request.state, "request_id", None)},
        )

    if idempotency_key:
        with _IDEMPOTENCY_LOCK:
            _IDEMPOTENCY[idempotency_key] = (
                run_id,
                dag_run_id,
                now + _IDEMPOTENCY_TTL_SECONDS,
            )

    metrics.record_trigger()

    return JSONResponse(
        status_code=202,
        content={"run_id": run_id, "dag_run_id": dag_run_id, "status": "queued"},
    )
