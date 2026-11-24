from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path

import psycopg
import pyarrow.parquet as pq
from dotenv import load_dotenv

from pncp_pipeline.common.logging import configure_logging
from pncp_pipeline.config import Settings


@dataclass(frozen=True)
class QualityReport:
    silver_rows: int
    gold_rows: int | None
    fact_rows: int
    duplicate_keys: int
    null_required_fields: int
    negative_values: int


def _count_parquet_rows(directory: Path) -> int:
    parquet_files = sorted(directory.rglob("*.parquet"))
    if not parquet_files:
        raise ValueError(f"Nenhum arquivo Parquet encontrado em {directory}")
    return sum(pq.read_metadata(path).num_rows for path in parquet_files)


def _sum_gold_contract_count(gold_dir: Path) -> int | None:
    resumo_dir = gold_dir / "resumo_uf_mes"
    parquet_files = sorted(resumo_dir.rglob("*.parquet"))
    if not parquet_files:
        return None
    table = pq.read_table(parquet_files, columns=["qtd_contratacoes"])
    return int(sum(val.as_py() for val in table["qtd_contratacoes"]))


def run_quality_checks(
    silver_dir: Path, database_url: str, *, gold_dir: Path | None = None
) -> QualityReport:
    """Valida contagem, unicidade, campos obrigatórios e consistência entre camadas."""
    silver_rows = _count_parquet_rows(silver_dir / "contratacoes")
    if silver_rows == 0:
        raise ValueError("Camada Silver possui 0 registros")

    gold_rows = _sum_gold_contract_count(gold_dir) if gold_dir is not None else None
    if gold_rows is not None and gold_rows != silver_rows:
        raise ValueError(
            f"Inconsistência Silver ({silver_rows}) vs Gold ({gold_rows}) em qtd_contratacoes"
        )

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            fact_rows = cur.execute("SELECT COUNT(*) FROM fact_contratacao").fetchone()[0]
            duplicate_keys = cur.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT numero_controle_pncp
                    FROM fact_contratacao
                    GROUP BY numero_controle_pncp
                    HAVING COUNT(*) > 1
                ) dup
                """
            ).fetchone()[0]
            null_required_fields = cur.execute(
                """
                SELECT COUNT(*) FROM fact_contratacao
                WHERE numero_controle_pncp IS NULL
                   OR orgao_cnpj IS NULL
                   OR modalidade_id IS NULL
                   OR data_publicacao_pncp IS NULL
                """
            ).fetchone()[0]
            negative_values = cur.execute(
                """
                SELECT COUNT(*) FROM fact_contratacao
                WHERE valor_total_estimado < 0 OR valor_total_homologado < 0
                """
            ).fetchone()[0]

    if fact_rows == 0:
        raise ValueError("fact_contratacao possui 0 registros")
    if duplicate_keys > 0:
        raise ValueError(f"Encontradas {duplicate_keys} chaves duplicadas em fact_contratacao")
    if null_required_fields > 0:
        raise ValueError(
            f"Encontrados {null_required_fields} registros com campo obrigatório nulo"
        )
    if negative_values > 0:
        raise ValueError(f"Encontrados {negative_values} valores negativos em fact_contratacao")

    return QualityReport(
        silver_rows=silver_rows,
        gold_rows=gold_rows,
        fact_rows=fact_rows,
        duplicate_keys=duplicate_keys,
        null_required_fields=null_required_fields,
        negative_values=negative_values,
    )


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    argparse.ArgumentParser(
        prog="python -m pncp_pipeline.common.quality",
        description="Executa validações de qualidade entre Silver, Gold e PostgreSQL.",
    ).parse_args(argv)
    settings = Settings.from_env()
    configure_logging(settings.log_level, settings.log_format)
    report = run_quality_checks(
        settings.silver_data_dir,
        settings.database_url,
        gold_dir=settings.gold_data_dir,
    )
    logging.getLogger(__name__).info(
        "quality_checks_passed",
        extra={
            "silver_rows": report.silver_rows,
            "gold_rows": report.gold_rows,
            "fact_rows": report.fact_rows,
            "duplicate_keys": report.duplicate_keys,
            "null_required_fields": report.null_required_fields,
            "negative_values": report.negative_values,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
