from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import psycopg
import pyarrow.parquet as pq


@dataclass(frozen=True)
class QualityReport:
    silver_rows: int
    fact_rows: int
    duplicate_keys: int
    negative_values: int


def run_quality_checks(silver_dir: Path, database_url: str) -> QualityReport:
    """Valida contagem mínima, unicidade e consistência de valores entre Silver e PostgreSQL."""
    silver_path = silver_dir / "contratacoes"
    parquet_files = sorted(silver_path.rglob("*.parquet"))
    if not parquet_files:
        raise ValueError(f"Nenhum arquivo Parquet encontrado em {silver_path}")

    silver_rows = sum(pq.read_metadata(path).num_rows for path in parquet_files)
    if silver_rows == 0:
        raise ValueError("Camada Silver possui 0 registros")

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
    if negative_values > 0:
        raise ValueError(f"Encontrados {negative_values} valores negativos em fact_contratacao")

    return QualityReport(
        silver_rows=silver_rows,
        fact_rows=fact_rows,
        duplicate_keys=duplicate_keys,
        negative_values=negative_values,
    )
