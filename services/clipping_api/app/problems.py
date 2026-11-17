"""Respostas de erro padronizadas no formato ``application/problem+json``."""

from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class ClippingErrorCode(str, Enum):
    """Códigos de erro determinísticos para mapeamento por clientes."""

    BAD_REQUEST = "BAD_REQUEST"
    UNAUTHORIZED = "UNAUTHORIZED"
    FORBIDDEN = "FORBIDDEN"
    NOT_FOUND = "NOT_FOUND"
    CONFLICT = "CONFLICT"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    RATE_LIMITED = "RATE_LIMITED"
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"
    INTERNAL_ERROR = "INTERNAL_ERROR"


class ProblemDetail(BaseModel):
    """Schema padronizado de erro (RFC 7807 + código tipado)."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "type": "about:blank",
                "title": "Parâmetro inválido",
                "status": 422,
                "code": "VALIDATION_ERROR",
                "message": "Fonte(s) inválida(s): XPTO. Valores aceitos: DOU, QD, INLABS, DOE:SP, DOE:MG.",
                "detail": "Fonte(s) inválida(s): XPTO. Valores aceitos: DOU, QD, INLABS, DOE:SP, DOE:MG.",
                "allowed": ["DOU", "QD", "INLABS", "DOE:SP", "DOE:MG"],
                "request_id": "2c8d3f2a-4c3a-4b88-9c6b-1a2b3c4d5e6f",
            }
        }
    )

    type: str = Field(default="about:blank", description="URI do tipo do problema")
    title: str = Field(description="Resumo curto do erro")
    status: int = Field(description="Código HTTP")
    code: ClippingErrorCode = Field(description="Código estável para o consumidor")
    message: str = Field(description="Mensagem descritiva para diagnóstico")
    detail: str | None = Field(default=None, description="Detalhe complementar")
    allowed: list[str] | None = Field(
        default=None, description="Valores aceitos quando aplicável"
    )
    request_id: str | None = Field(
        default=None, description="Correlação da requisição (X-Request-Id)"
    )


class ApiProblem(Exception):
    """Erro de API convertido em ``application/problem+json``."""

    def __init__(
        self,
        status: int,
        title: str,
        detail: str | None = None,
        allowed: list[str] | None = None,
        headers: dict | None = None,
        code: ClippingErrorCode | None = None,
    ):
        super().__init__(title)
        self.status = status
        self.title = title
        self.detail = detail
        self.allowed = allowed
        self.headers = headers or {}
        self.code = code or _default_code(status)


def _default_code(status: int) -> ClippingErrorCode:
    return {
        400: ClippingErrorCode.BAD_REQUEST,
        401: ClippingErrorCode.UNAUTHORIZED,
        403: ClippingErrorCode.FORBIDDEN,
        404: ClippingErrorCode.NOT_FOUND,
        409: ClippingErrorCode.CONFLICT,
        422: ClippingErrorCode.VALIDATION_ERROR,
        429: ClippingErrorCode.RATE_LIMITED,
        503: ClippingErrorCode.SERVICE_UNAVAILABLE,
    }.get(status, ClippingErrorCode.INTERNAL_ERROR)
