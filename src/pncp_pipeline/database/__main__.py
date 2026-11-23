from __future__ import annotations

import argparse
import logging
from pathlib import Path

import psycopg
from dotenv import load_dotenv

from pncp_pipeline.config import Settings
from pncp_pipeline.database.loader import apply_schema, load_bronze_to_postgres


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m pncp_pipeline.database",
        description="Gerencia schema, carga Bronze -> PostgreSQL e consultas analíticas.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init-db", help="Aplica schema.sql no PostgreSQL.")
    sub.add_parser("load", help="Carrega arquivos Bronze para o PostgreSQL (idempotente).")
    query_cmd = sub.add_parser("query", help="Executa um arquivo SQL e imprime as linhas.")
    query_cmd.add_argument("sql_file", type=Path, help="Caminho para arquivo .sql")
    return parser


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    args = build_parser().parse_args(argv)
    settings = Settings.from_env()
    log = logging.getLogger(__name__)

    if args.command == "init-db":
        with psycopg.connect(settings.database_url) as conn:
            apply_schema(conn)
            conn.commit()
        log.info("schema aplicado com sucesso")
        return 0

    if args.command == "load":
        summary = load_bronze_to_postgres(settings.raw_data_dir, settings.database_url)
        log.info(
            "carga concluída run_id=%s files=%s rows_in=%s rows_dedup=%s duration_s=%s",
            summary.run_id,
            summary.files_read,
            summary.rows_in,
            summary.rows_deduplicated,
            summary.duration_s,
        )
        return 0

    sql_text = args.sql_file.read_text(encoding="utf-8")
    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_text)
            cols = [desc.name for desc in cur.description] if cur.description else []
            rows = cur.fetchall() if cur.description else []
    if cols:
        print(" | ".join(cols))
        print("-" * 80)
        for row in rows:
            print(" | ".join(str(v) for v in row))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
