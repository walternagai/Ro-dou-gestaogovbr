"""Backfill administrativo do clipping por período.

Dispara execuções manuais da DAG para cada dia do intervalo, usando a API REST
do Airflow. Datas que já têm execução são ignoradas e os itens são gravados com
upsert idempotente pela DAG.

Uso (administrativo, com token do Airflow):
    AIRFLOW_API_TOKEN=<token> \
    AIRFLOW_API_URL=http://airflow-api-server:8080 \
    CLIPPING_DAG_ID=rodou_clipping \
        python scripts/backfill.py --from 2026-10-01 --to 2026-10-31

As execuções de backfill suprimem notificações (conf `backfill: true`).
"""

import argparse
import os
import sys
import time
from datetime import date, timedelta

import httpx

MAX_DAYS = 31


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"data inválida: {value!r} (use AAAA-MM-DD)"
        ) from exc


def date_range(start: date, end: date, include_weekends: bool = False) -> list[date]:
    if end < start:
        raise ValueError("--to deve ser maior ou igual a --from")
    if (end - start).days + 1 > MAX_DAYS:
        raise ValueError(f"a janela máxima é de {MAX_DAYS} dias")

    days = []
    current = start
    while current <= end:
        if include_weekends or current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def logical_date(day: date) -> str:
    return f"{day.isoformat()}T12:00:00-03:00"


def _headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def has_run(client: httpx.Client, base_url: str, token: str, dag_id: str, day: date) -> bool:
    response = client.get(
        f"{base_url}/api/v2/dags/{dag_id}/dagRuns",
        params={
            "logical_date_gte": f"{day.isoformat()}T00:00:00-03:00",
            "logical_date_lte": f"{day.isoformat()}T23:59:59-03:00",
            "limit": 1,
        },
        headers=_headers(token),
        timeout=15,
    )
    if response.status_code >= 400:
        return False

    payload = response.json() or {}
    runs = payload.get("dag_runs") or payload.get("dagRuns") or []
    return bool(runs)


def trigger_day(
    client: httpx.Client,
    base_url: str,
    token: str,
    dag_id: str,
    day: date,
    sources: list[str] | None,
) -> str:
    response = client.post(
        f"{base_url}/api/v2/dags/{dag_id}/dagRuns",
        json={
            "logical_date": logical_date(day),
            "conf": {
                "reference_date": day.isoformat(),
                "sources": sources or [],
                "backfill": True,
            },
        },
        headers=_headers(token),
        timeout=30,
    )
    if response.status_code == 202 or response.status_code == 200:
        return "queued"
    if response.status_code == 409:
        return "exists"
    return f"failed: HTTP {response.status_code}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="date_from", required=True, type=parse_date)
    parser.add_argument("--to", dest="date_to", required=True, type=parse_date)
    parser.add_argument(
        "--sources",
        action="append",
        choices=("DOU", "QD", "INLABS", "DOE:SP", "DOE:MG"),
        help="Fontes (pode repetir). Sem isso, usa a configuração da DAG.",
    )
    parser.add_argument(
        "--include-weekends",
        action="store_true",
        help="Inclui sábados e domingos (por padrão são pulados).",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--interval", type=float, default=2.0)
    args = parser.parse_args()

    try:
        days = date_range(args.date_from, args.date_to, args.include_weekends)
    except ValueError as exc:
        parser.error(str(exc))

    token = os.getenv("AIRFLOW_API_TOKEN", "")
    if not token and not args.dry_run:
        parser.error("defina AIRFLOW_API_TOKEN")

    base_url = os.getenv("AIRFLOW_API_URL", "http://airflow-api-server:8080").rstrip("/")
    dag_id = os.getenv("CLIPPING_DAG_ID", "rodou_clipping")

    print(f"DAG {dag_id} | {len(days)} dia(s) | API {base_url}")

    queued = skipped = failed = 0
    with httpx.Client() as client:
        for day in days:
            if args.dry_run:
                print(f"[DRY-RUN] {day} -> {logical_date(day)}")
                continue

            if has_run(client, base_url, token, dag_id, day):
                print(f"[SKIP] {day} já tem execução")
                skipped += 1
                continue

            result = trigger_day(
                client, base_url, token, dag_id, day, args.sources
            )
            print(f"[{result.upper()}] {day}")
            if result == "queued":
                queued += 1
            elif result == "exists":
                skipped += 1
            else:
                failed += 1

            if args.interval:
                time.sleep(args.interval)

    print(
        f"\nResumo: {queued} disparada(s), {skipped} ignorada(s), "
        f"{failed} falha(s)."
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
