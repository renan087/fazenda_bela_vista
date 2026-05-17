from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.db.session import SessionLocal
from app.repositories.farm import FarmRepository
from app.services.coffee_quotes import parse_cepea_xls_quotes


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Importa arquivos .xls oficiais do CEPEA para a tabela historica coffee_quotes."
    )
    parser.add_argument("files", nargs="+", help="Caminhos dos arquivos .xls do CEPEA.")
    args = parser.parse_args()

    total = 0
    with SessionLocal() as db:
        repository = FarmRepository(db)
        for raw_path in args.files:
            file_path = Path(raw_path).expanduser()
            quotes = parse_cepea_xls_quotes(file_path)
            imported = repository.upsert_coffee_quotes(quotes)
            total += imported
            quote_type = quotes[0].quote_type if quotes else "desconhecido"
            first_date = min((quote.quote_date for quote in quotes), default=None)
            last_date = max((quote.quote_date for quote in quotes), default=None)
            print(
                f"{file_path.name}: {imported} registros {quote_type} "
                f"({first_date or '-'} ate {last_date or '-'})."
            )
    print(f"Importacao concluida: {total} registros processados.")


if __name__ == "__main__":
    main()
