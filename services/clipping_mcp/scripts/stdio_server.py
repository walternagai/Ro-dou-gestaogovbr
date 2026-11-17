"""Ponto de entrada CLI para rodar o servidor MCP em modo stdio.

Uso:
    CLIPPING_API_BASE_URL=https://rodou.ninja-apps.work \
    CLIPPING_API_KEY=<sua-chave> \
        python scripts/stdio_server.py
"""

import argparse
import asyncio
import os
import sys

# Adiciona o diretório raiz do serviço ao PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.stdio import run_stdio_server


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--api-key",
        default=os.getenv("CLIPPING_API_KEY"),
        help="Chave de API do clipping (padrão: $CLIPPING_API_KEY)",
    )
    args = parser.parse_args()

    try:
        asyncio.run(run_stdio_server(api_key=args.api_key))
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == "__main__":
    main()
