"""Cliente HTTP assíncrono para a API REST do Rodou Clipping."""

import httpx

from .settings import Settings


class ApiClientError(Exception):
    def __init__(self, status_code: int, message: str, detail: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.message = message
        self.detail = detail


class ClippingApiClient:
    def __init__(self, settings: Settings, client: httpx.AsyncClient | None = None):
        self.settings = settings
        self._external_client = client

    def _headers(self, auth_header: str | None = None) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        token = auth_header or (
            f"Bearer {self.settings.api_key}" if self.settings.api_key else ""
        )
        if token:
            if token.lower().startswith("bearer "):
                headers["Authorization"] = token
            else:
                headers["X-API-Key"] = token
        return headers

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        json_body: dict | None = None,
        auth_header: str | None = None,
        extra_headers: dict | None = None,
    ) -> dict:
        url = f"{self.settings.api_base_url}{path}"
        headers = self._headers(auth_header)
        if extra_headers:
            headers.update(extra_headers)

        client = self._external_client or httpx.AsyncClient(timeout=15.0)
        try:
            response = await client.request(
                method, url, params=params, json=json_body, headers=headers
            )
        except httpx.RequestError as exc:
            raise ApiClientError(
                503,
                "API do Clipping indisponível",
                f"Erro de conexão com {url}: {exc}",
            ) from exc
        finally:
            if not self._external_client:
                await client.aclose()

        if response.status_code >= 400:
            error_detail = None
            try:
                data = response.json()
                error_detail = data.get("detail") or data.get("title")
            except Exception:  # noqa: BLE001 - corpo de erro pode não ser JSON
                error_detail = response.text

            raise ApiClientError(
                response.status_code,
                f"Erro HTTP {response.status_code} da API",
                error_detail,
            )

        if response.status_code == 204:
            return {}
        return response.json()

    async def list_items(
        self, params: dict, auth_header: str | None = None
    ) -> dict:
        return await self._request(
            "GET", "/clipping/v1/items", params=params, auth_header=auth_header
        )

    async def get_item(
        self, item_id: str, auth_header: str | None = None
    ) -> dict:
        return await self._request(
            "GET", f"/clipping/v1/items/{item_id}", auth_header=auth_header
        )

    async def list_runs(
        self, params: dict, auth_header: str | None = None
    ) -> dict:
        return await self._request(
            "GET", "/clipping/v1/runs", params=params, auth_header=auth_header
        )

    async def get_run(self, run_id: str, auth_header: str | None = None) -> dict:
        return await self._request(
            "GET", f"/clipping/v1/runs/{run_id}", auth_header=auth_header
        )

    async def list_sources(self, auth_header: str | None = None) -> dict:
        return await self._request(
            "GET", "/clipping/v1/sources", auth_header=auth_header
        )

    async def trigger_run(
        self,
        payload: dict,
        auth_header: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict:
        extra_headers = (
            {"Idempotency-Key": idempotency_key} if idempotency_key else None
        )
        return await self._request(
            "POST",
            "/clipping/v1/runs",
            json_body=payload,
            auth_header=auth_header,
            extra_headers=extra_headers,
        )
