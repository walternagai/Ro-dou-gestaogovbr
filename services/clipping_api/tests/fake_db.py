"""Banco falso em memória para os testes da API (sem PostgreSQL)."""

from datetime import date, datetime


class FakeDB:
    def __init__(self):
        self.keys: dict[str, dict] = {}
        self.items: list[dict] = []
        self.runs: list[dict] = []
        self.source_runs: list[dict] = []
        self.audits: list[dict] = []
        self.running_dates: set[date] = set()
        self.queued_runs: list[dict] = []

    def health(self) -> bool:
        return True

    def get_api_key(self, key_hash: str):
        return self.keys.get(key_hash)

    def insert_audit(self, **record) -> None:
        self.audits.append(record)

    # ------------------------------------------------------------------
    def list_items(
        self,
        *,
        date=None,
        date_from=None,
        date_to=None,
        sources=None,
        uf=None,
        q=None,
        since=None,
        limit=50,
        offset=0,
    ):
        rows = []
        for item in self.items:
            if date is not None and item["published_date"] != date:
                continue
            if date_from is not None and item["published_date"] < date_from:
                continue
            if date_to is not None and item["published_date"] > date_to:
                continue
            if sources and item["source"] not in sources:
                continue
            if uf and item.get("uf") != uf:
                continue
            if q:
                haystack = f"{item.get('title') or ''} {item.get('abstract') or ''}".casefold()
                if q.casefold() not in haystack:
                    continue
            if since is not None and not (item["updated_at"] > since):
                continue
            rows.append(item)

        rows.sort(key=lambda row: row["id"])
        rows.sort(key=lambda row: row["published_date"], reverse=True)
        return rows[offset : offset + limit], len(rows)

    def get_item(self, item_id: str):
        return next((item for item in self.items if item["id"] == item_id), None)

    # ------------------------------------------------------------------
    def list_runs(self, *, date=None, limit=50):
        runs = [run for run in self.runs if date is None or run["reference_date"] == date]
        runs.sort(key=lambda run: run["started_at"], reverse=True)
        return runs[:limit]

    def get_run(self, run_id: str):
        return next((run for run in self.runs if run["id"] == run_id), None)

    def runs_summary(self, *, date_from, date_to):
        runs = [
            run
            for run in self.runs
            if date_from <= run["reference_date"] <= date_to
        ]
        requested, ok, failed = [], [], {}

        for run in runs:
            for source in run.get("sources_requested") or []:
                if source not in requested:
                    requested.append(source)
            for source in run.get("sources_ok") or []:
                if source not in ok:
                    ok.append(source)
            for failure in run.get("sources_failed") or []:
                failed.setdefault(failure["source"], failure)

        return {
            "sources_requested": requested,
            "sources_ok": ok,
            "sources_failed": list(failed.values()),
        }

    def source_statuses(self):
        latest: dict[str, dict] = {}
        for row in sorted(
            self.source_runs, key=lambda entry: entry["started_at"], reverse=True
        ):
            latest.setdefault(row["source"], row)
        return latest

    def create_queued_run(self, *, run_id, dag_id, reference_date, sources):
        self.queued_runs.append(
            {
                "id": run_id,
                "dag_id": dag_id,
                "reference_date": reference_date,
                "sources": list(sources or []),
            }
        )

    def has_running_run(self, reference_date) -> bool:
        return reference_date in self.running_dates


def make_item(
    *,
    item_id: str,
    source: str = "DOU",
    uf: str | None = None,
    published_date: date = date(2026, 11, 14),
    title: str = "Extrato de contrato",
    abstract: str = "licitação",
    updated_at: datetime | None = None,
    url: str | None = None,
):
    return {
        "id": item_id,
        "source": source,
        "uf": uf,
        "gazette": None,
        "published_date": published_date,
        "section": "Seção 3",
        "org": None,
        "title": title,
        "abstract": abstract,
        "url": url or f"https://example.test/{item_id}",
        "matched_terms": ["licitação"],
        "updated_at": updated_at or datetime(2026, 11, 14, 8, 0),
    }


def make_run(
    *,
    run_id: str,
    reference_date: date = date(2026, 11, 14),
    status: str = "success",
    sources_requested=("DOU",),
    sources_ok=("DOU",),
    sources_failed=(),
    items_count: int = 1,
    started_at: datetime | None = None,
):
    return {
        "id": run_id,
        "dag_id": "dag_test",
        "reference_date": reference_date,
        "status": status,
        "sources_requested": list(sources_requested),
        "sources_ok": list(sources_ok),
        "sources_failed": list(sources_failed),
        "items_count": items_count,
        "started_at": started_at or datetime(2026, 11, 14, 5, 0),
        "finished_at": datetime(2026, 11, 14, 5, 10),
    }
