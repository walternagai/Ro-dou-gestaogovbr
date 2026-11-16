"""Acesso ao banco ``clipping`` (somente leitura + auditoria)."""

import contextlib

import psycopg2
from psycopg2.extras import RealDictCursor


class DatabaseUnavailable(RuntimeError):
    """O banco não está configurado ou não respondeu."""


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
        if sources:
            where.append("source = ANY(%(sources)s)")
            params["sources"] = list(sources)
        if uf:
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
    ) -> tuple[list[dict], int]:
        where_sql, params = self._item_filters(
            date=date,
            date_from=date_from,
            date_to=date_to,
            sources=sources,
            uf=uf,
            q=q,
            since=since,
        )

        with self._cursor() as cursor:
            cursor.execute(
                f"SELECT count(*) AS total FROM clipping.item WHERE {where_sql}",
                params,
            )
            total = cursor.fetchone()["total"]

            cursor.execute(
                f"""
                SELECT id, source, uf, gazette, published_date, section, org,
                       title, abstract, url, matched_terms, updated_at
                  FROM clipping.item
                 WHERE {where_sql}
                 ORDER BY published_date DESC, id ASC
                 LIMIT %(limit)s OFFSET %(offset)s
                """,
                {**params, "limit": limit, "offset": offset},
            )
            rows = [dict(row) for row in cursor.fetchall()]

        return rows, int(total)

    def get_item(self, item_id: str) -> dict | None:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT id, source, uf, gazette, published_date, section, org,
                       title, abstract, url, matched_terms, updated_at
                  FROM clipping.item
                 WHERE id = %s
                """,
                (item_id,),
            )
            row = cursor.fetchone()
            return dict(row) if row else None

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
            return [dict(row) for row in cursor.fetchall()]

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
            return dict(row) if row else None

    def runs_summary(self, *, date_from, date_to) -> dict:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT sources_requested, sources_ok, sources_failed
                  FROM clipping.run
                 WHERE reference_date BETWEEN %s AND %s
                 ORDER BY started_at DESC
                 LIMIT 50
                """,
                (date_from, date_to),
            )
            runs = [dict(row) for row in cursor.fetchall()]

        requested: list[str] = []
        ok: list[str] = []
        failed: dict[str, dict] = {}

        for run in runs:
            for source in run.get("sources_requested") or []:
                if source not in requested:
                    requested.append(source)
            for source in run.get("sources_ok") or []:
                if source not in ok:
                    ok.append(source)
            for failure in run.get("sources_failed") or []:
                failed.setdefault(
                    failure.get("source", "?"),
                    {
                        "source": failure.get("source"),
                        "error": failure.get("error"),
                        "at": failure.get("at"),
                    },
                )

        return {
            "sources_requested": requested,
            "sources_ok": ok,
            "sources_failed": list(failed.values()),
        }

    def source_statuses(self) -> dict[str, dict]:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT ON (sr.source)
                       sr.source, sr.status, sr.items_count, sr.error,
                       sr.duration_ms, r.started_at
                  FROM clipping.source_run sr
                  JOIN clipping.run r ON r.id = sr.run_id
                 ORDER BY sr.source, r.started_at DESC
                """
            )
            return {row["source"]: dict(row) for row in cursor.fetchall()}

    def has_running_run(self, reference_date) -> bool:
        with self._cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM clipping.run "
                "WHERE reference_date = %s AND status IN ('queued', 'running') "
                "LIMIT 1",
                (reference_date,),
            )
            return cursor.fetchone() is not None
