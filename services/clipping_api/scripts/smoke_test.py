"""Smoke test da API do Rodou Clipping (somente leitura).

Uso:
    CLIPPING_API_URL=https://rodou.ninja-apps.work \
    CLIPPING_API_KEY=<chave> \
        python services/clipping_api/scripts/smoke_test.py

Sem `CLIPPING_API_KEY`, valida apenas sondas de saúde, OpenAPI e o 401.
Defina `CLIPPING_INTERNAL_URL` (ex.: `http://clipping-api:8000`) para validar
`/internal/metrics` na rede interna.
"""

import os
import sys
from datetime import date, timedelta

import httpx

BASE_URL = os.getenv("CLIPPING_API_URL", "http://localhost:8000").rstrip("/")
INTERNAL_URL = os.getenv("CLIPPING_INTERNAL_URL", "").rstrip("/")
API_KEY = os.getenv("CLIPPING_API_KEY")
REFERENCE_DATE = os.getenv(
    "CLIPPING_API_DATE", (date.today() - timedelta(days=1)).isoformat()
)

failures = []


def check(name: str, condition: bool, detail: str = "") -> None:
    status = "PASS" if condition else "FAIL"
    suffix = f" ({detail})" if detail else ""
    print(f"[{status}] {name}{suffix}")
    if not condition:
        failures.append(name)


def main() -> int:
    print(f"API: {BASE_URL} | data de referência: {REFERENCE_DATE}")

    health = httpx.get(f"{BASE_URL}/clipping/healthz", timeout=10)
    check("healthz 200", health.status_code == 200, f"HTTP {health.status_code}")

    livez = httpx.get(f"{BASE_URL}/clipping/livez", timeout=10)
    check("livez 200", livez.status_code == 200, f"HTTP {livez.status_code}")

    readyz = httpx.get(f"{BASE_URL}/clipping/readyz", timeout=10)
    check("readyz 200", readyz.status_code == 200, f"HTTP {readyz.status_code}")

    openapi = httpx.get(f"{BASE_URL}/clipping/openapi.json", timeout=10)
    spec = openapi.json() if openapi.status_code == 200 else {}
    paths = (spec or {}).get("paths", {})
    schemes = ((spec or {}).get("components") or {}).get("securitySchemes", {})
    if openapi.status_code == 200:
        check("openapi com /clipping/v1/items", "/clipping/v1/items" in paths)
        check("openapi com ApiKeyAuth", "ApiKeyAuth" in schemes)
    else:
        print(f"[SKIP] openapi desabilitado (HTTP {openapi.status_code})")

    unauthorized = httpx.get(
        f"{BASE_URL}/clipping/v1/items",
        params={"date": REFERENCE_DATE},
        timeout=10,
    )
    check(
        "401 sem chave",
        unauthorized.status_code == 401
        and unauthorized.json().get("code") == "UNAUTHORIZED",
        f"HTTP {unauthorized.status_code}",
    )

    if INTERNAL_URL:
        metrics = httpx.get(f"{INTERNAL_URL}/internal/metrics", timeout=10)
        check(
            "metrics interno exposto",
            metrics.status_code == 200
            and "clipping_api_requests_total" in metrics.text,
            f"HTTP {metrics.status_code}",
        )
    else:
        print("[SKIP] metrics interno (defina CLIPPING_INTERNAL_URL)")

    if API_KEY:
        headers = {"X-API-Key": API_KEY}
        items = httpx.get(
            f"{BASE_URL}/clipping/v1/items",
            params={"date": REFERENCE_DATE, "size": 1},
            headers=headers,
            timeout=15,
        )
        check("items com chave", items.status_code == 200, f"HTTP {items.status_code}")

        sources = httpx.get(
            f"{BASE_URL}/clipping/v1/sources", headers=headers, timeout=10
        )
        check(
            "sources com chave",
            sources.status_code == 200,
            f"HTTP {sources.status_code}",
        )
    else:
        print("[SKIP] consultas autenticadas (defina CLIPPING_API_KEY)")

    print(f"\n{len(failures)} falha(s).")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
