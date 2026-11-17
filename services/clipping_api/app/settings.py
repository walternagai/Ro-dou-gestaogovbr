"""Configuração do serviço Rodou Clipping (lida do ambiente a cada requisição)."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    db_url: str
    airflow_api_url: str
    airflow_token: str
    dag_id: str
    read_limit_per_minute: int
    trigger_limit_per_hour: int
    cache_max_age_seconds: int
    expose_docs: bool
    stale_after_hours: int

    @property
    def trigger_enabled(self) -> bool:
        return bool(self.airflow_token)


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def get_settings() -> Settings:
    return Settings(
        db_url=os.getenv("CLIPPING_DB_URL", ""),
        airflow_api_url=os.getenv(
            "AIRFLOW_API_URL", "http://airflow-api-server:8080"
        ).rstrip("/"),
        airflow_token=os.getenv("AIRFLOW_API_TOKEN", ""),
        dag_id=os.getenv("CLIPPING_DAG_ID", "rodou_clipping"),
        read_limit_per_minute=_int_env("CLIPPING_READ_LIMIT_PER_MINUTE", 120),
        trigger_limit_per_hour=_int_env("CLIPPING_TRIGGER_LIMIT_PER_HOUR", 10),
        cache_max_age_seconds=_int_env("CLIPPING_CACHE_MAX_AGE", 60),
        expose_docs=_bool_env("CLIPPING_EXPOSE_DOCS", True),
        stale_after_hours=_int_env("CLIPPING_STALE_AFTER_HOURS", 26),
    )
