"""Cria uma chave de API do Rodou Clipping.

A chave em texto puro é exibida uma única vez; o banco guarda apenas o hash.

Uso:
    CLIPPING_DB_URL=postgresql://clipping_rw:senha@postgres:5432/clipping \
        python scripts/create_key.py --label "agente-ia" --scope clipping:read \
        --scope clipping:trigger
"""

import argparse
import hashlib
import os
import secrets

import psycopg2


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", required=True, help="Identificação do consumidor")
    parser.add_argument(
        "--scope",
        action="append",
        dest="scopes",
        choices=("clipping:read", "clipping:trigger"),
        help="Escopo concedido (pode repetir). Default: clipping:read",
    )
    parser.add_argument("--id", dest="key_id", default=None, help="ID opcional")
    args = parser.parse_args()

    dsn = os.getenv("CLIPPING_DB_URL")
    if not dsn:
        raise SystemExit("Defina CLIPPING_DB_URL com a conexão do banco clipping.")

    raw_key = secrets.token_urlsafe(32)
    key_id = args.key_id or f"key-{secrets.token_hex(6)}"
    scopes = args.scopes or ["clipping:read"]

    with (
        psycopg2.connect(dsn) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            "INSERT INTO clipping.api_key (id, key_hash, label, scope) "
            "VALUES (%s, %s, %s, %s)",
            (key_id, hashlib.sha256(raw_key.encode()).hexdigest(), args.label, scopes),
        )

    print(f"key_id={key_id}")
    print(f"api_key={raw_key}")


if __name__ == "__main__":
    main()
