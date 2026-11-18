"""Banco falso em memória para os testes da API (sem PostgreSQL)."""

from datetime import date, datetime

from app.catalog import SOURCE_NAMES, sources_for_uf
from app.db import _normalize_item, normalize_failure, summarize_runs


class FakeDB:
    def __init__(self):
        self.keys: dict[str, dict] = {}
        self.items: list[dict] = []
        self.runs: list[dict] = []
        self.source_runs: list[dict] = []
        self.audits: list[dict] = []
        self.running_dates: set[date] = set()
        self.queued_runs: list[dict] = []
        self.healthy: bool = True

    def health(self) -> bool:
        return self.healthy

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
        cursor=None,
    ):
        uf_sources = set(sources_for_uf(uf)) if uf else set()
        rows = []
        for item in self.items:
            if date is not None and item["published_date"] != date:
                continue
            if date_from is not None and item["published_date"] < date_from:
                continue
            if date_to is not None and item["published_date"] > date_to:
                continue
            if sources is not None and item["source"] not in sources:
                continue
            if uf:
                item_uf = item.get("uf")
                if item_uf != uf and not (
                    item_uf is None and item.get("source") in uf_sources
                ):
                    continue
            if q:
                haystack = (
                    f"{item.get('title') or ''} {item.get('abstract') or ''}".casefold()
                )
                if q.casefold() not in haystack:
                    continue
            if since is not None and not (item["updated_at"] > since):
                continue
            rows.append(_normalize_item(item))

        rows.sort(key=lambda row: row["id"])
        rows.sort(key=lambda row: row["published_date"], reverse=True)
        total = len(rows)

        if cursor is not None:
            cursor_date, cursor_id = cursor
            rows = [
                row
                for row in rows
                if (row["published_date"] < cursor_date)
                or (row["published_date"] == cursor_date and row["id"] > cursor_id)
            ]
            sliced = rows[: limit + 1]
        else:
            sliced = rows[offset : offset + limit + 1]

        has_more = len(sliced) > limit
        return sliced[:limit], total, has_more

    def get_item(self, item_id: str):
        item = next((item for item in self.items if item["id"] == item_id), None)
        return _normalize_item(item) if item else None

    # ------------------------------------------------------------------
    def list_runs(self, *, date=None, limit=50):
        runs = [
            {
                **run,
                "sources_failed": [
                    normalize_failure(f, reference_date=run.get("reference_date"))
                    for f in (run.get("sources_failed") or [])
                ],
            }
            for run in self.runs
            if date is None or run["reference_date"] == date
        ]
        runs.sort(key=lambda run: run["started_at"], reverse=True)
        return runs[:limit]

    def get_run(self, run_id: str):
        run = next((run for run in self.runs if run["id"] == run_id), None)
        if run is None:
            return None
        return {
            **run,
            "sources_failed": [
                normalize_failure(f, reference_date=run.get("reference_date"))
                for f in (run.get("sources_failed") or [])
            ],
        }

    def runs_summary(
        self,
        *,
        date_from,
        date_to,
        sources=None,
        enabled_sources=SOURCE_NAMES,
    ):
        first_run_date = min(
            (run["reference_date"] for run in self.runs),
            default=None,
        )
        latest_by_date: dict[date, dict] = {}
        for run in sorted(
            self.runs, key=lambda entry: entry["started_at"], reverse=True
        ):
            ref_date = run["reference_date"]
            if date_from <= ref_date <= date_to:
                latest_by_date.setdefault(ref_date, run)

        return summarize_runs(
            list(latest_by_date.values()),
            date_from=date_from,
            date_to=date_to,
            first_run_date=first_run_date,
            sources=sources,
            enabled_sources=enabled_sources,
        )

    def source_statuses(self):
        latest: dict[str, dict] = {}
        last_success: dict[str, datetime] = {}
        for row in sorted(
            self.source_runs, key=lambda entry: entry["started_at"], reverse=True
        ):
            latest.setdefault(row["source"], dict(row))
            if row.get("status") in ("ok", "empty"):
                last_success.setdefault(row["source"], row["started_at"])

        for source, entry in latest.items():
            entry["last_success_at"] = last_success.get(source)
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
    content_hash: str | None = None,
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
        "content_hash": content_hash or f"hash-{item_id}",
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
