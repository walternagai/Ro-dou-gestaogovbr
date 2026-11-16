"""Disparo de execuções da DAG do clipping via API REST do Airflow."""

import httpx

from .problems import ApiProblem
from .settings import Settings


def trigger_dag(
    settings: Settings, *, reference_date, sources: list[str] | None
) -> str:
    """Cria uma execução manual e devolve o ``dag_run_id`` do Airflow."""
    if not settings.trigger_enabled:
        raise ApiProblem(
            503,
            "Disparo de execução não configurado",
            detail="Defina AIRFLOW_API_TOKEN para habilitar POST /runs.",
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
        ) from exc

    if response.status_code == 409:
        raise ApiProblem(
            409,
            "Execução concorrente",
            detail="Já existe uma execução em andamento para esta data.",
        )
    if response.status_code >= 400:
        raise ApiProblem(
            503,
            "Falha ao disparar a DAG",
            detail=f"Airflow respondeu HTTP {response.status_code}.",
        )

    dag_run_id = response.json().get("dag_run_id")
    if not dag_run_id:
        raise ApiProblem(
            503,
            "Resposta inesperada do Airflow",
            detail="A resposta não contém dag_run_id.",
        )

    return dag_run_id
