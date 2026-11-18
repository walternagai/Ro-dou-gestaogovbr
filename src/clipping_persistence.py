"""Persistência do clipping no banco `clipping` (fase F1).

O módulo concentra a lógica de normalização dos resultados e a escrita no
banco, sem depender do Airflow, para permitir testes diretos contra um
PostgreSQL. A task do Airflow apenas resolve a conexão e chama `persist_run`.
"""

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime

logger = logging.getLogger(__name__)

#: Connection ID do Airflow que aponta para o banco `clipping`.
CLIPPING_CONN_ID = "clipping_db"

STATUS_OK = "ok"
STATUS_EMPTY = "empty"
STATUS_FAILED = "failed"

RUN_SUCCESS = "success"
RUN_PARTIAL = "partial"
RUN_FAILED = "failed"

_SOURCE_UFS = {"DOE:MG": "MG", "DOE:SP": "SP"}

# Colunas de `clipping.item` são passadas como parâmetros nomeados nos
# comandos SQL abaixo.

RUN_UPSERT = """
    INSERT INTO clipping.run (
        id, dag_id, dag_run_id, reference_date, status,
        sources_requested, started_at
    )
    VALUES (
        %(id)s, %(dag_id)s, %(dag_run_id)s, %(reference_date)s, 'running',
        %(sources_requested)s, %(started_at)s
    )
    ON CONFLICT (id) DO UPDATE SET
        dag_id = EXCLUDED.dag_id,
        reference_date = EXCLUDED.reference_date,
        status = 'running',
        sources_requested = EXCLUDED.sources_requested,
        started_at = LEAST(clipping.run.started_at, EXCLUDED.started_at)
"""

RUN_FINISH_UPDATE = """
    UPDATE clipping.run
       SET status = %(status)s,
           sources_ok = %(sources_ok)s,
           sources_failed = %(sources_failed)s::jsonb,
           items_count = %(items_count)s,
           finished_at = %(finished_at)s
     WHERE id = %(id)s
"""

RUN_MARK_FAILED = """
    INSERT INTO clipping.run (
        id, dag_id, dag_run_id, reference_date, status,
        sources_requested, sources_failed, items_count, started_at, finished_at
    )
    VALUES (
        %(id)s, %(dag_id)s, %(dag_run_id)s, %(reference_date)s, 'failed',
        %(sources_requested)s, %(sources_failed)s::jsonb, 0, %(now)s, %(now)s
    )
    ON CONFLICT (id) DO UPDATE SET
        status = 'failed',
        sources_failed = EXCLUDED.sources_failed,
        finished_at = EXCLUDED.finished_at
"""

ITEM_UPSERT = """
    INSERT INTO clipping.item (
        id, run_id, source, uf, gazette, external_id, published_date,
        section, org, title, abstract, url, matched_terms, content_hash,
        created_at, updated_at
    )
    VALUES (
        %(id)s, %(run_id)s, %(source)s, %(uf)s, %(gazette)s, %(external_id)s,
        %(published_date)s, %(section)s, %(org)s, %(title)s, %(abstract)s,
        %(url)s, %(matched_terms)s, %(content_hash)s, %(created_at)s,
        %(updated_at)s
    )
    ON CONFLICT (id) DO UPDATE SET
        uf = EXCLUDED.uf,
        abstract = EXCLUDED.abstract,
        title = EXCLUDED.title,
        url = EXCLUDED.url,
        content_hash = EXCLUDED.content_hash,
        matched_terms = EXCLUDED.matched_terms,
        updated_at = EXCLUDED.updated_at
"""

SOURCE_RUN_UPSERT = """
    INSERT INTO clipping.source_run (
        run_id, source, status, items_count, error, duration_ms
    )
    VALUES (
        %(run_id)s, %(source)s, %(status)s, %(items_count)s, %(error)s,
        %(duration_ms)s
    )
    ON CONFLICT (run_id, source) DO UPDATE SET
        status = EXCLUDED.status,
        items_count = EXCLUDED.items_count,
        error = EXCLUDED.error,
        duration_ms = EXCLUDED.duration_ms
"""


@dataclass(frozen=True)
class PersistSummary:
    """Resumo da persistência de uma execução."""

    status: str
    items_count: int
    sources_requested: list
    sources_ok: list
    sources_failed: list


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def item_id(source: str, uf: str | None, external_id: str) -> str:
    """Gera a chave determinística de um item."""
    return _sha256(f"{source}|{uf or '-'}|{external_id}")


def parse_published_date(value, fallback: date) -> date:
    """Converte a data do item (`dd/mm/aaaa` ou ISO) para `date`."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(value.strip(), fmt).date()
            except ValueError:
                continue
    return fallback


def external_id_for(item: dict) -> str:
    """Escolhe o identificador externo do item, com fallback estável."""
    for key in ("href", "url"):
        value = (item.get(key) or "").strip()
        if value:
            return value

    parts = [
        item.get("source") or "",
        item.get("section") or "",
        item.get("title") or "",
        item.get("date") or "",
    ]
    return "|".join(parts)


def _item_count(grouped_result: dict) -> int:
    return sum(
        len(items)
        for terms in (grouped_result or {}).values()
        for departments in terms.values()
        for items in departments.values()
    )


def build_item_rows(search_results, reference_date: date) -> list[dict]:
    """Normaliza os itens de todas as buscas em linhas de `clipping.item`.

    Itens repetidos dentro da mesma execução são deduplicados pelo `id`.
    """
    now = datetime.now(UTC)
    rows: dict[str, dict] = {}

    for search in search_results or []:
        if not search:
            continue
        grouped = search.get("result") or {}
        for terms in grouped.values():
            for term, departments in (terms or {}).items():
                for department, items in (departments or {}).items():
                    for item in items or []:
                        source = item.get("source") or "UNKNOWN"
                        uf = item.get("uf") or _SOURCE_UFS.get(source)
                        url = item.get("href") or item.get("url") or ""
                        external_id = external_id_for(item)
                        title = item.get("title") or ""
                        abstract = item.get("abstract") or ""

                        row = {
                            "id": item_id(source, uf, external_id),
                            "source": source,
                            "uf": uf,
                            "gazette": item.get("gazette"),
                            "external_id": external_id,
                            "published_date": parse_published_date(
                                item.get("date"), reference_date
                            ),
                            "section": item.get("section"),
                            "org": (
                                None
                                if department == "single_department"
                                else department
                            ),
                            "title": title,
                            "abstract": abstract,
                            "url": url,
                            "matched_terms": [term] if term else [],
                            "content_hash": _sha256(f"{title}|{abstract}|{url}"),
                            "created_at": now,
                            "updated_at": now,
                        }
                        rows.setdefault(row["id"], row)

    return list(rows.values())


def _merge_status(current: dict | None, new: dict) -> dict:
    if current is None:
        return {
            "status": new.get("status", STATUS_FAILED),
            "error": new.get("error"),
            "items_count": new.get("items_count", 0),
            "duration_ms": new.get("duration_ms"),
        }

    if current["status"] == STATUS_FAILED or new.get("status") == STATUS_FAILED:
        status = STATUS_FAILED
    elif STATUS_OK in (current["status"], new.get("status")):
        status = STATUS_OK
    else:
        status = STATUS_EMPTY

    return {
        "status": status,
        "error": new.get("error") or current.get("error"),
        "items_count": (current.get("items_count") or 0)
        + (new.get("items_count") or 0),
        "duration_ms": (current.get("duration_ms") or 0)
        + (new.get("duration_ms") or 0),
    }


def aggregate_sources_status(
    search_results, configured_sources=()
) -> tuple[dict, list]:
    """Consolida o status por fonte de todas as buscas da execução."""
    requested: list[str] = []
    statuses: dict[str, dict] = {}

    for search in search_results or []:
        if not search:
            continue
        for source, status in (search.get("sources_status") or {}).items():
            if source not in requested:
                requested.append(source)
            statuses[source] = _merge_status(statuses.get(source), status)

    for source in configured_sources or []:
        if source not in requested:
            requested.append(source)
        if source not in statuses:
            statuses[source] = {
                "status": STATUS_FAILED,
                "error": "busca não concluiu nesta execução",
                "items_count": 0,
                "duration_ms": None,
            }

    return statuses, requested


def overall_status(statuses: dict) -> str:
    """Deriva o status da execução a partir do status de cada fonte."""
    if not statuses:
        return RUN_FAILED

    has_failure = any(
        status["status"] == STATUS_FAILED for status in statuses.values()
    )
    has_success = any(
        status["status"] in (STATUS_OK, STATUS_EMPTY)
        for status in statuses.values()
    )

    if has_failure and not has_success:
        return RUN_FAILED
    if has_failure:
        return RUN_PARTIAL
    return RUN_SUCCESS


def persist_run(
    conn,
    *,
    dag_id: str,
    run_id: str,
    reference_date: date,
    search_results,
    configured_sources=(),
) -> PersistSummary:
    """Grava a execução, o status por fonte e os itens coletados.

    A operação é idempotente: repetir a mesma execução atualiza os itens pelo
    `id` e preserva o `run_id` da primeira inserção.
    """
    items = build_item_rows(search_results, reference_date)
    statuses, requested = aggregate_sources_status(
        search_results, configured_sources
    )
    status = overall_status(statuses)

    sources_ok = [
        source
        for source, source_status in statuses.items()
        if source_status["status"] in (STATUS_OK, STATUS_EMPTY)
    ]
    now = datetime.now(UTC)
    sources_failed = [
        {
            "source": source,
            "error": source_status.get("error") or "erro não informado",
            "at": now.isoformat(),
        }
        for source, source_status in statuses.items()
        if source_status["status"] == STATUS_FAILED
    ]

    with conn.cursor() as cur:
        cur.execute(
            RUN_UPSERT,
            {
                "id": run_id,
                "dag_id": dag_id,
                "dag_run_id": run_id,
                "reference_date": reference_date,
                "sources_requested": requested,
                "started_at": now,
            },
        )

        for item in items:
            item["run_id"] = run_id
        if items:
            cur.executemany(ITEM_UPSERT, items)

        for source, source_status in statuses.items():
            cur.execute(
                SOURCE_RUN_UPSERT,
                {
                    "run_id": run_id,
                    "source": source,
                    "status": source_status["status"],
                    "items_count": source_status.get("items_count") or 0,
                    "error": source_status.get("error"),
                    "duration_ms": source_status.get("duration_ms"),
                },
            )

        cur.execute(
            RUN_FINISH_UPDATE,
            {
                "id": run_id,
                "status": status,
                "sources_ok": sources_ok,
                "sources_failed": json.dumps(sources_failed, ensure_ascii=False),
                "items_count": len(items),
                "finished_at": now,
            },
        )

    conn.commit()

    return PersistSummary(
        status=status,
        items_count=len(items),
        sources_requested=requested,
        sources_ok=sources_ok,
        sources_failed=sources_failed,
    )


def mark_run_failed(
    conn,
    *,
    run_id: str,
    dag_id: str,
    reference_date: date,
    error: str,
    configured_sources=(),
) -> None:
    """Garante que a execução fique registrada como `failed`."""
    now = datetime.now(UTC)
    with conn.cursor() as cur:
        cur.execute(
            RUN_MARK_FAILED,
            {
                "id": run_id,
                "dag_id": dag_id,
                "dag_run_id": run_id,
                "reference_date": reference_date,
                "sources_requested": list(configured_sources),
                "sources_failed": json.dumps(
                    [{"source": "*", "error": error, "at": now.isoformat()}],
                    ensure_ascii=False,
                ),
                "now": now,
            },
        )
    conn.commit()
