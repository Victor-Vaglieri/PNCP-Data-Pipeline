from __future__ import annotations

import argparse
import logging

import pyarrow.parquet as pq
from dotenv import load_dotenv

from pncp_pipeline.config import Settings
from pncp_pipeline.transformations.pandas_baseline import run_pandas_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m pncp_pipeline.transformations",
        description="Executa transformações Bronze -> Silver -> Gold em Pandas ou PySpark.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("pandas", help="Executa baseline Pandas + Parquet.")
    sub.add_parser("spark", help="Executa pipeline PySpark + Parquet.")
    inspect_cmd = sub.add_parser(
        "inspect", help="Inspeciona partições Parquet (e plano Spark quando disponível)."
    )
    inspect_cmd.add_argument(
        "--uf", default="SP", help="UF usada para demonstrar partition pruning"
    )
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

    if args.command == "pandas":
        summary = run_pandas_pipeline(
            settings.raw_data_dir, settings.silver_data_dir, settings.gold_data_dir
        )
        log.info(
            "transformação concluída engine=%s files=%s in=%s valid_dedup=%s rejected=%s gold=%s",
            summary.engine,
            summary.files_read,
            summary.rows_in,
            summary.rows_valid_dedup,
            summary.rows_rejected,
            summary.gold_uf_rows,
        )
        return 0

    if args.command == "spark":
        from pncp_pipeline.transformations.spark_jobs import (
            create_spark_session,
            run_spark_pipeline,
        )

        spark = create_spark_session()
        try:
            summary = run_spark_pipeline(
                spark, settings.raw_data_dir, settings.silver_data_dir, settings.gold_data_dir
            )
        finally:
            spark.stop()
        log.info(
            "transformação concluída engine=%s files=%s in=%s valid_dedup=%s rejected=%s gold=%s",
            summary.engine,
            summary.files_read,
            summary.rows_in,
            summary.rows_valid_dedup,
            summary.rows_rejected,
            summary.gold_uf_rows,
        )
        return 0

    silver_contratacoes = settings.silver_data_dir / "contratacoes"
    parquet_files = sorted(silver_contratacoes.rglob("*.parquet"))
    print(f"Arquivos Parquet em {silver_contratacoes}: {len(parquet_files)}")
    for file_path in parquet_files[:10]:
        print(" -", file_path.relative_to(silver_contratacoes).as_posix())
    if parquet_files:
        meta = pq.read_metadata(parquet_files[0])
        print(
            f"Exemplo metadata ({parquet_files[0].name}): "
            f"rows={meta.num_rows} cols={meta.num_columns}"
        )

    try:
        from pncp_pipeline.transformations.spark_jobs import (
            create_spark_session,
            inspect_parquet_pruning,
        )
    except ImportError:
        return 0

    spark = create_spark_session("pncp-inspect")
    try:
        print(inspect_parquet_pruning(spark, settings.silver_data_dir, uf=args.uf))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
