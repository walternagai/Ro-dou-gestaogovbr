"""Acesso ao banco ``clipping`` (somente leitura + auditoria)."""

import contextlib
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import RealDictCursor

from .catalog import SOURCE_NAMES, SOURCES, sources_for_uf

_BRT = ZoneInfo("America/Sao_Paulo")


class DatabaseUnavailable(RuntimeError):
    """O banco não está configurado ou não respondeu."""


def normalize_failure(
    failure: dict, *, reference_date: date | None = None
) -> dict:
    """Normaliza um registro de falha de fonte para o contrato tipado."""
    resolved_date = failure.get("date")
    if resolved_date is None and reference_date is not None:
        resolved_date = reference_date.isoformat()
    elif isinstance(resolved_date, date):
        resolved_date = resolved_date.isoformat()

    return {
        "source": failure.get("source") or "?",
        "code": failure.get("code") or "UPSTREAM_ERROR",
        "message": failure.get("message")
        or failure.get("error")
        or "Falha na coleta",
        "date": resolved_date,
        "at": failure.get("at"),
    }


def _normalize_run(row: dict) -> dict:
    row = dict(row)
    ref_date = row.get("reference_date")
    row["sources_failed"] = [
        normalize_failure(failure, reference_date=ref_date)
        for failure in (row.get("sources_failed") or [])
    ]
    return row


def _normalize_item(row: dict) -> dict:
    row = dict(row)
    uf = row.get("uf")
    if isinstance(uf, str):
        uf = uf.strip() or None
    if uf is None:
        uf = SOURCES.get(row.get("source") or "", {}).get("uf")
    row["uf"] = uf
    return row


def summarize_runs(
    runs: list[dict],
    *,
    date_from: date,
    date_to: date,
    first_run_date: date | None,
    sources: list[str] | None = None,
    enabled_sources: tuple[str, ...] = SOURCE_NAMES,
    now_brt: datetime | None = None,
) -> dict:
    """Consolida ``sources_requested``, ``sources_ok`` e ``sources_failed``.

    Considera apenas a execução mais recente por ``reference_date``, restringe ao
    subconjunto de fontes consultado (quando informado) e aponta dias úteis sem
    execução desde ``first_run_date`` como ``MISSING_RUN``.
    """
    target_set = set(sources) if sources is not None else set(enabled_sources)
    requested: list[str] = []
    ok: list[str] = []
    failed: dict[tuple[str, str | None], dict] = {}
    seen_dates: set[date] = set()

    for run in runs:
        ref_date = run.get("reference_date")
        if ref_date is not None:
            seen_dates.add(ref_date)

        run_requested = list(run.get("sources_requested") or [])
        run_ok = list(run.get("sources_ok") or [])
        run_failed = list(run.get("sources_failed") or [])

        for source in run_requested:
            if source in target_set and source not in requested:
                requested.append(source)

        for source in run_ok:
            if source in target_set and source not in ok:
                ok.append(source)

        recorded_failed_sources: set[str] = set()
        for failure in run_failed:
            normalized = normalize_failure(failure, reference_date=ref_date)
            f_source = normalized["source"]
            if f_source == "*":
                expand = (
                    list(sources)
                    if sources is not None
                    else [s for s in (run_requested or enabled_sources) if s in target_set]
                )
                for src in expand:
                    failed.setdefault(
                        (src, normalized["date"]),
                        {**normalized, "source": src},
                    )
                    recorded_failed_sources.add(src)
            elif f_source in target_set:
                failed.setdefault((f_source, normalized["date"]), normalized)
                recorded_failed_sources.add(f_source)

        if run.get("status") == "failed" and ref_date is not None:
            fallback_pool = (
                list(sources)
                if sources is not None
                else (run_requested or list(enabled_sources))
            )
            day_iso = ref_date.isoformat()
            for src in fallback_pool:
                if run_requested and src not in run_requested:
                    continue
                if src not in run_ok and src not in recorded_failed_sources:
                    failed.setdefault(
                        (src, day_iso),
                        {
                            "source": src,
                            "code": "UPSTREAM_ERROR",
                            "message": "Execução falhou sem concluir esta fonte.",
                            "date": day_iso,
                            "at": None,
                        },
                    )

    if first_run_date is not None:
        current_brt = now_brt or datetime.now(_BRT)
        max_check_date = (
            current_brt.date() - timedelta(days=1)
            if current_brt.hour < 6
            else current_brt.date()
        )
        check_start = max(date_from, first_run_date)
        check_end = min(date_to, max_check_date)
        missing_sources = (
            list(sources) if sources is not None else list(enabled_sources)
        )
        cur_day = check_start
        while cur_day <= check_end:
            if cur_day.weekday() < 5 and cur_day not in seen_dates:
                day_iso = cur_day.isoformat()
                for src in missing_sources:
                    failed.setdefault(
                        (src, day_iso),
                        {
                            "source": src,
                            "code": "MISSING_RUN",
                            "message": (
                                f"Sem execução registrada para o dia útil {day_iso}."
                            ),
                            "date": day_iso,
                            "at": None,
                        },
                    )
            cur_day += timedelta(days=1)

    return {
        "sources_requested": requested,
        "sources_ok": ok,
        "sources_failed": list(failed.values()),
    }


class Database:
    def __init__(self, url: str):
        self.url = url

    @contextlib.contextmanager
    def _cursor(self, commit: bool = False):
        if not self.url:
            raise DatabaseUnavailable("CLIPPING_DB_URL não configurada")
        connection = psycopg2.connect(self.url, connect_timeout=5)
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                yield cursor
            if commit:
                connection.commit()
        finally:
            connection.close()

    # ------------------------------------------------------------------
    # Saúde e credenciais
    # ------------------------------------------------------------------
    def health(self) -> bool:
        try:
            with self._cursor() as cursor:
                cursor.execute("SELECT 1")
                return cursor.fetchone() is not None
        except Exception:  # noqa: BLE001 - health não deve levantar
            return False

    def get_api_key(self, key_hash: str) -> dict | None:
        with self._cursor() as cursor:
            cursor.execute(
                "SELECT id, label, scope, active FROM clipping.api_key "
                "WHERE key_hash = %s",
                (key_hash,),
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def insert_audit(
        self,
        *,
        key_id: str,
        route: str,
        status: int,
        latency_ms: int,
        request_id: str | None,
    ) -> None:
        with self._cursor(commit=True) as cursor:
            cursor.execute(
                "INSERT INTO clipping.api_request "
                "(key_id, route, status, latency_ms, request_id) "
                "VALUES (%s, %s, %s, %s, %s)",
                (key_id, route, status, latency_ms, request_id),
            )

    # ------------------------------------------------------------------
    # Itens
    # ------------------------------------------------------------------
    @staticmethod
    def _item_filters(
        *,
        date=None,
        date_from=None,
        date_to=None,
        sources=None,
        uf=None,
        q=None,
        since=None,
    ) -> tuple[str, dict]:
        where = []
        params: dict = {}

        if date is not None:
            where.append("published_date = %(date)s")
            params["date"] = date
        if date_from is not None:
            where.append("published_date >= %(date_from)s")
            params["date_from"] = date_from
        if date_to is not None:
            where.append("published_date <= %(date_to)s")
            params["date_to"] = date_to
        if sources is not None:
            where.append("source = ANY(%(sources)s)")
            params["sources"] = list(sources)
        if uf:
            uf_sources = sources_for_uf(uf)
            if uf_sources:
                where.append(
                    "(uf = %(uf)s OR (uf IS NULL AND source = ANY(%(uf_sources)s)))"
                )
                params["uf_sources"] = uf_sources
            else:
                where.append("uf = %(uf)s")
            params["uf"] = uf
        if q:
            where.append(
                "to_tsvector('portuguese', coalesce(title, '') || ' ' || "
                "coalesce(abstract, '')) @@ plainto_tsquery('portuguese', %(q)s)"
            )
            params["q"] = q
        if since is not None:
            where.append("updated_at > %(since)s")
            params["since"] = since

        return (" AND ".join(where) if where else "TRUE"), params

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
        limit: int = 50,
        offset: int = 0,
        cursor: tuple[date, str] | None = None,
    ) -> tuple[list[dict], int, bool]:
        where_sql, params = self._item_filters(
            date=date,
            date_from=date_from,
            date_to=date_to,
            sources=sources,
            uf=uf,
            q=q,
            since=since,
        )

        page_where = where_sql
        page_params = dict(params)
        page_params["fetch_limit"] = limit + 1

        if cursor is not None:
            cursor_date, cursor_id = cursor
            page_where = (
                f"({where_sql}) AND "
                "((published_date < %(cursor_date)s) "
                "OR (published_date = %(cursor_date)s AND id > %(cursor_id)s))"
            )
            page_params["cursor_date"] = cursor_date
            page_params["cursor_id"] = cursor_id
            pagination_sql = "LIMIT %(fetch_limit)s"
        else:
            page_params["offset"] = offset
            pagination_sql = "LIMIT %(fetch_limit)s OFFSET %(offset)s"

        with self._cursor() as db_cursor:
            db_cursor.execute(
                f"SELECT count(*) AS total FROM clipping.item WHERE {where_sql}",
                params,
            )
            total = db_cursor.fetchone()["total"]

            db_cursor.execute(
                f"""
                SELECT id, source, uf, gazette, published_date, section, org,
                       title, abstract, url, matched_terms, content_hash,
                       updated_at
                  FROM clipping.item
                 WHERE {page_where}
                 ORDER BY published_date DESC, id ASC
                 {pagination_sql}
                """,
                page_params,
            )
            rows = [_normalize_item(row) for row in db_cursor.fetchall()]

        has_more = len(rows) > limit
        return rows[:limit], int(total), has_more

    def get_item(self, item_id: str) -> dict | None:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT id, source, uf, gazette, published_date, section, org,
                       title, abstract, url, matched_terms, content_hash,
                       updated_at
                  FROM clipping.item
                 WHERE id = %s
                """,
                (item_id,),
            )
            row = cursor.fetchone()
            return _normalize_item(row) if row else None

    # ------------------------------------------------------------------
    # Execuções
    # ------------------------------------------------------------------
    def list_runs(self, *, date=None, limit: int = 50) -> list[dict]:
        params: dict = {"limit": limit}
        where = ""
        if date is not None:
            where = "WHERE reference_date = %(date)s"
            params["date"] = date

        with self._cursor() as cursor:
            cursor.execute(
                f"""
                SELECT id, dag_id, reference_date, status, sources_requested,
                       sources_ok, sources_failed, items_count, started_at,
                       finished_at
                  FROM clipping.run
                  {where}
                 ORDER BY started_at DESC
                 LIMIT %(limit)s
                """,
                params,
            )
            return [_normalize_run(row) for row in cursor.fetchall()]

    def get_run(self, run_id: str) -> dict | None:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT id, dag_id, reference_date, status, sources_requested,
                       sources_ok, sources_failed, items_count, started_at,
                       finished_at
                  FROM clipping.run
                 WHERE id = %s
                """,
                (run_id,),
            )
            row = cursor.fetchone()
            return _normalize_run(row) if row else None

    def runs_summary(
        self,
        *,
        date_from,
        date_to,
        sources: list[str] | None = None,
        enabled_sources: tuple[str, ...] = SOURCE_NAMES,
    ) -> dict:
        with self._cursor() as cursor:
            cursor.execute(
                "SELECT MIN(reference_date) AS first_date FROM clipping.run"
            )
            first_row = cursor.fetchone()
            first_run_date = first_row["first_date"] if first_row else None

            cursor.execute(
                """
                SELECT DISTINCT ON (reference_date)
                       reference_date, status, sources_requested, sources_ok,
                       sources_failed
                  FROM clipping.run
                 WHERE reference_date BETWEEN %s AND %s
                 ORDER BY reference_date, started_at DESC
                """,
                (date_from, date_to),
            )
            runs = [dict(row) for row in cursor.fetchall()]

        return summarize_runs(
            runs,
            date_from=date_from,
            date_to=date_to,
            first_run_date=first_run_date,
            sources=sources,
            enabled_sources=enabled_sources,
        )

    def source_statuses(self) -> dict[str, dict]:
        with self._cursor() as cursor:
            cursor.execute(
                """
                WITH latest AS (
                    SELECT DISTINCT ON (sr.source)
                           sr.source, sr.status, sr.items_count, sr.error,
                           sr.duration_ms, r.started_at
                      FROM clipping.source_run sr
                      JOIN clipping.run r ON r.id = sr.run_id
                     ORDER BY sr.source, r.started_at DESC
                ),
                last_ok AS (
                    SELECT sr.source, MAX(r.started_at) AS last_success_at
                      FROM clipping.source_run sr
                      JOIN clipping.run r ON r.id = sr.run_id
                     WHERE sr.status IN ('ok', 'empty')
                     GROUP BY sr.source
                )
                SELECT latest.source, latest.status, latest.items_count,
                       latest.error, latest.duration_ms, latest.started_at,
                       last_ok.last_success_at
                  FROM latest
                  LEFT JOIN last_ok USING (source)
                """
            )
            return {row["source"]: dict(row) for row in cursor.fetchall()}

    def create_queued_run(
        self, *, run_id: str, dag_id: str, reference_date, sources
    ) -> None:
        """Registra a run assim que o Airflow aceita o disparo."""
        with self._cursor(commit=True) as cursor:
            cursor.execute(
                """
                INSERT INTO clipping.run
                    (id, dag_id, dag_run_id, reference_date, status,
                     sources_requested, started_at)
                VALUES (%s, %s, %s, %s, 'queued', %s, now())
                ON CONFLICT (id) DO NOTHING
                """,
                (
                    run_id,
                    dag_id,
                    run_id.split(":", 1)[-1],
                    reference_date,
                    list(sources or []),
                ),
            )

    def has_running_run(self, reference_date) -> bool:
        with self._cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM clipping.run "
                "WHERE reference_date = %s AND status IN ('queued', 'running') "
                "LIMIT 1",
                (reference_date,),
            )
            return cursor.fetchone() is not None
