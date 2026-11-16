"""Expurgo por retenção do banco `clipping`.

Remove itens mais antigos que o período configurado (default: 24 meses), em
lotes, além de execuções órfãs e registros de auditoria antigos. As operações
são idempotentes e seguras para rodar periodicamente.

Uso administrativo:
    CLIPPING_DB_URL=<conexão com escrita> \
        python scripts/purge.py --months 24 --dry-run

    CLIPPING_DB_URL=<conexão com escrita> \
        python scripts/purge.py --months 24 --vacuum
"""

import argparse
import json
import os
import sys

import psycopg2

DEFAULT_MONTHS = 24
DEFAULT_API_REQUEST_DAYS = 90
DEFAULT_BATCH_SIZE = 1000

def _item_cutoff(alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return (
        f"{prefix}published_date < "
        "(CURRENT_DATE - make_interval(months => %(months)s))"
    )


ITEM_CUTOFF = _item_cutoff()
RUN_CUTOFF = "r.finished_at < (CURRENT_DATE - make_interval(months => %(months)s))"
API_CUTOFF = "created_at < (now() - make_interval(days => %(days)s))"


def purge(
    dsn: str,
    *,
    months: int = DEFAULT_MONTHS,
    api_request_days: int = DEFAULT_API_REQUEST_DAYS,
    batch_size: int = DEFAULT_BATCH_SIZE,
    dry_run: bool = False,
    vacuum: bool = False,
) -> dict:
    if months < 1:
        raise ValueError("months deve ser >= 1")
    if api_request_days < 1:
        raise ValueError("api_request_days deve ser >= 1")
    if batch_size < 1:
        raise ValueError("batch_size deve ser >= 1")

    summary = {
        "dry_run": dry_run,
        "months": months,
        "items": 0,
        "runs": 0,
        "api_requests": 0,
    }

    connection = psycopg2.connect(dsn)
    try:
        connection.autocommit = False
        with connection.cursor() as cursor:
            if dry_run:
                cursor.execute(
                    f"SELECT count(*) FROM clipping.item WHERE {ITEM_CUTOFF}",
                    {"months": months},
                )
                summary["items"] = cursor.fetchone()[0]

                cursor.execute(
                    f"""
                    SELECT count(*) FROM clipping.run r
                     WHERE {RUN_CUTOFF}
                       AND NOT EXISTS (
                           SELECT 1 FROM clipping.item i
                            WHERE i.run_id = r.id
                              AND NOT ({_item_cutoff("i")})
                       )
                    """,
                    {"months": months},
                )
                summary["runs"] = cursor.fetchone()[0]

                cursor.execute(
                    f"SELECT count(*) FROM clipping.api_request WHERE {API_CUTOFF}",
                    {"days": api_request_days},
                )
                summary["api_requests"] = cursor.fetchone()[0]
                return summary

            while True:
                cursor.execute(
                    f"""
                    DELETE FROM clipping.item
                     WHERE id IN (
                         SELECT id FROM clipping.item
                          WHERE {ITEM_CUTOFF}
                          LIMIT %(batch_size)s
                     )
                    """,
                    {"months": months, "batch_size": batch_size},
                )
                deleted = cursor.rowcount
                connection.commit()
                summary["items"] += deleted
                if deleted < batch_size:
                    break

            cursor.execute(
                f"""
                DELETE FROM clipping.run r
                 WHERE {RUN_CUTOFF}
                   AND NOT EXISTS (
                       SELECT 1 FROM clipping.item i WHERE i.run_id = r.id
                   )
                """,
                {"months": months},
            )
            summary["runs"] = cursor.rowcount
            connection.commit()

            cursor.execute(
                f"DELETE FROM clipping.api_request WHERE {API_CUTOFF}",
                {"days": api_request_days},
            )
            summary["api_requests"] = cursor.rowcount
            connection.commit()
    finally:
        connection.close()

    if vacuum:
        connection = psycopg2.connect(dsn)
        connection.autocommit = True
        try:
            with connection.cursor() as cursor:
                cursor.execute("VACUUM ANALYZE clipping.item")
        finally:
            connection.close()

    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dsn",
        default=os.getenv("CLIPPING_DB_URL"),
        help="conexão com escrita no banco clipping (default: CLIPPING_DB_URL)",
    )
    parser.add_argument("--months", type=int, default=DEFAULT_MONTHS)
    parser.add_argument(
        "--api-request-days", type=int, default=DEFAULT_API_REQUEST_DAYS
    )
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--vacuum", action="store_true")
    args = parser.parse_args()

    if not args.dsn:
        parser.error("defina CLIPPING_DB_URL ou passe --dsn")

    summary = purge(
        args.dsn,
        months=args.months,
        api_request_days=args.api_request_days,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        vacuum=args.vacuum,
    )
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
