"""Fakes usados nos testes do servidor MCP."""

from dataclasses import dataclass

from app.client import ApiClientError


@dataclass
class FakeSettings:
    api_base_url: str = "http://api.test"
    api_key: str = ""
    default_size_limit: int = 50
    max_abstract_length: int = 1000


class FakeApiClient:
    def __init__(self):
        self.settings = FakeSettings()
        self.calls = []
        self.items = {
            "reference": {"from": "2026-11-14", "to": "2026-11-14"},
            "sources_requested": ["DOU"],
            "sources_failed": [],
            "total": 2,
            "page": 1,
            "size": 50,
            "next_page": None,
            "items": [
                {
                    "id": "item-1",
                    "source": "DOU",
                    "title": "Portaria",
                    "abstract": "x" * 1500,
                    "published_date": "2026-11-14",
                },
                {
                    "id": "item-2",
                    "source": "DOE:MG",
                    "title": "Edital",
                    "abstract": "curto",
                    "published_date": "2026-11-14",
                },
            ],
        }
        self.item = {
            "id": "item-1",
            "source": "DOU",
            "title": "Portaria",
            "abstract": "y" * 1200,
            "url": "https://example.test/1",
        }
        self.runs = {"runs": [{"id": "dag:run-1", "status": "success"}]}
        self.run = {"id": "dag:run-1", "status": "success"}
        self.sources = {"sources": [{"source": "DOU"}]}
        self.trigger_response = {
            "run_id": "rodou_clipping:run-1",
            "dag_run_id": "run-1",
            "status": "queued",
        }
        self.trigger_error = None
        self.not_found = False

    async def list_items(self, params, auth_header=None):
        self.calls.append(("list_items", params, auth_header))
        return self.items

    async def get_item(self, item_id, auth_header=None):
        self.calls.append(("get_item", item_id, auth_header))
        if self.not_found or item_id != "item-1":
            raise ApiClientError(404, "Item não encontrado", None)
        return self.item

    async def list_runs(self, params, auth_header=None):
        self.calls.append(("list_runs", params, auth_header))
        return self.runs

    async def get_run(self, run_id, auth_header=None):
        self.calls.append(("get_run", run_id, auth_header))
        if self.not_found:
            raise ApiClientError(404, "Execução não encontrada", None)
        return self.run

    async def list_sources(self, auth_header=None):
        self.calls.append(("list_sources", None, auth_header))
        return self.sources

    async def trigger_run(self, payload, auth_header=None, idempotency_key=None):
        self.calls.append(
            ("trigger_run", payload, auth_header, idempotency_key)
        )
        if self.trigger_error:
            raise self.trigger_error
        return self.trigger_response
