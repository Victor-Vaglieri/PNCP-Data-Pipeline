from __future__ import annotations

import argparse
import logging
from datetime import datetime

from dotenv import load_dotenv

from pncp_pipeline.common.logging import configure_logging
from pncp_pipeline.config import Settings
from pncp_pipeline.ingestion.bronze_writer import ingest_contratacoes
from pncp_pipeline.ingestion.client import PncpClient


def normalize_api_date(raw: str) -> str:
    """Aceita YYYY-MM-DD ou YYYYMMDD e devolve YYYYMMDD validado."""
    cleaned = raw.strip().replace("-", "")
    try:
        datetime.strptime(cleaned, "%Y%m%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Data inválida '{raw}'. Use YYYY-MM-DD ou YYYYMMDD."
        ) from exc
    return cleaned


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m pncp_pipeline.ingestion",
        description="Coleta contratações da API pública do PNCP para a camada Bronze.",
    )
    parser.add_argument("--start", required=True, type=normalize_api_date, help="Data inicial")
    parser.add_argument("--end", required=True, type=normalize_api_date, help="Data final")
    parser.add_argument(
        "--modality",
        type=int,
        default=6,
        help="Código da modalidade (ex.: 6 = Pregão Eletrônico, 8 = Dispensa)",
    )
    parser.add_argument(
        "--endpoint",
        default="contratacoes/publicacao",
        choices=("contratacoes/publicacao", "contratacoes/atualizacao"),
        help="Endpoint de consulta do PNCP",
    )
    parser.add_argument("--uf", default=None, help="Sigla da UF opcional (ex.: SP, RJ)")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Limita o número de páginas coletadas (útil em testes manuais)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    args = build_parser().parse_args(argv)
    if args.start > args.end:
        raise SystemExit(f"Data inicial ({args.start}) maior que data final ({args.end}).")

    settings = Settings.from_env()
    configure_logging(settings.log_level, settings.log_format)
    with PncpClient(settings) as client:
        result = ingest_contratacoes(
            client=client,
            raw_dir=settings.raw_data_dir,
            start_date=args.start,
            end_date=args.end,
            modality=args.modality,
            endpoint=args.endpoint,
            uf=args.uf,
            max_pages=args.max_pages,
        )

    logging.getLogger(__name__).info(
        "ingestão concluída run_id=%s endpoint=%s pages=%s records=%s",
        result.run_id,
        result.endpoint,
        result.pages_saved,
        result.records_fetched,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
