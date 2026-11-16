"""Respostas de erro no formato ``application/problem+json``."""


class ApiProblem(Exception):
    """Erro de API convertido em ``application/problem+json``."""

    def __init__(
        self,
        status: int,
        title: str,
        detail: str | None = None,
        allowed: list[str] | None = None,
        headers: dict | None = None,
    ):
        super().__init__(title)
        self.status = status
        self.title = title
        self.detail = detail
        self.allowed = allowed
        self.headers = headers or {}
