"""Integração com a API REST do Airflow (disparo e healthcheck)."""

import httpx

from .problems import ApiProblem, ClippingErrorCode
from .settings import Settings


def airflow_health(settings: Settings) -> bool:
    """Verifica se a API REST do Airflow responde ao healthcheck."""
    url = f"{settings.airflow_api_url}/api/v2/monitor/health"
    try:
        response = httpx.get(url, timeout=3)
    except httpx.HTTPError:
        return False
    return response.status_code == 200


def trigger_dag(
    settings: Settings, *, reference_date, sources: list[str] | None
) -> str:
    """Cria uma execução manual e devolve o ``dag_run_id`` do Airflow."""
    if not settings.trigger_enabled:
        raise ApiProblem(
            503,
            "Disparo de execução não configurado",
            detail="Defina AIRFLOW_API_TOKEN para habilitar POST /runs.",
            code=ClippingErrorCode.SERVICE_UNAVAILABLE,
        )

    url = f"{settings.airflow_api_url}/api/v2/dags/{settings.dag_id}/dagRuns"
    payload = {
        "logical_date": f"{reference_date.isoformat()}T12:00:00-03:00",
        "conf": {
            "reference_date": reference_date.isoformat(),
            "sources": sources or [],
            "origin": "clipping-api",
        },
    }

    try:
        response = httpx.post(
            url,
            json=payload,
            headers={"Authorization": f"Bearer {settings.airflow_token}"},
            timeout=10,
        )
    except httpx.HTTPError as exc:
        raise ApiProblem(
            503,
            "Airflow indisponível",
            detail=f"Não foi possível disparar a DAG: {exc.__class__.__name__}.",
            code=ClippingErrorCode.SERVICE_UNAVAILABLE,
        ) from exc

    if response.status_code == 409:
        raise ApiProblem(
            409,
            "Execução concorrente",
            detail="Já existe uma execução em andamento para esta data.",
            code=ClippingErrorCode.CONFLICT,
        )
    if response.status_code >= 400:
        raise ApiProblem(
            503,
            "Falha ao disparar a DAG",
            detail=f"Airflow respondeu HTTP {response.status_code}.",
            code=ClippingErrorCode.SERVICE_UNAVAILABLE,
        )

    dag_run_id = response.json().get("dag_run_id")
    if not dag_run_id:
        raise ApiProblem(
            503,
            "Resposta inesperada do Airflow",
            detail="A resposta não contém dag_run_id.",
            code=ClippingErrorCode.SERVICE_UNAVAILABLE,
        )

    return dag_run_id
