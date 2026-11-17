"""Configurações do serviço Rodou Clipping MCP."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    api_base_url: str
    api_key: str  # Chave padrão opcional para modo stdio local
    default_size_limit: int
    max_abstract_length: int


def get_settings() -> Settings:
    return Settings(
        api_base_url=os.getenv(
            "CLIPPING_API_BASE_URL", "http://clipping-api:8000"
        ).rstrip("/"),
        api_key=os.getenv("CLIPPING_API_KEY", ""),
        default_size_limit=int(os.getenv("MCP_MAX_ITEMS_PER_CALL", "50")),
        max_abstract_length=int(os.getenv("MCP_MAX_ABSTRACT_LENGTH", "1000")),
    )
