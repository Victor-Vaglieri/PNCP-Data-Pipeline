from __future__ import annotations

import argparse
import logging

import pyarrow.parquet as pq
from dotenv import load_dotenv

from pncp_pipeline.common.logging import configure_logging
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
    sub.add_parser("delta", help="Executa pipeline PySpark + Delta Lake (MERGE ACID).")
    sub.add_parser("delta-history", help="Exibe histórico de versões (Time Travel) do Delta.")
    inspect_cmd = sub.add_parser(
        "inspect", help="Inspeciona partições Parquet (e plano Spark quando disponível)."
    )
    inspect_cmd.add_argument(
        "--uf", default="SP", help="UF usada para demonstrar partition pruning"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    args = build_parser().parse_args(argv)
    settings = Settings.from_env()
    configure_logging(settings.log_level, settings.log_format)
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

    if args.command == "delta":
        from pncp_pipeline.transformations.delta_jobs import (
            create_delta_spark_session,
            run_delta_pipeline,
        )

        spark = create_delta_spark_session()
        try:
            summary = run_delta_pipeline(spark, settings.raw_data_dir, settings.delta_data_dir)
        finally:
            spark.stop()
        log.info(
            "delta_pipeline_concluido",
            extra={
                "files": summary.files_read,
                "rows_in": summary.rows_in,
                "rows_silver_batch": summary.rows_silver_batch,
                "total_silver_delta": summary.total_silver_delta,
                "gold_rows": summary.gold_rows,
                "delta_version": summary.delta_version,
            },
        )
        return 0

    if args.command == "delta-history":
        from pncp_pipeline.transformations.delta_jobs import (
            create_delta_spark_session,
            get_delta_history,
        )

        silver_delta = settings.delta_data_dir / "silver" / "contratacoes"
        spark = create_delta_spark_session("pncp-delta-history")
        try:
            for item in get_delta_history(spark, silver_delta):
                print(
                    f"v{item['version']} | {item['timestamp']} | "
                    f"{item['operation']} | {item['operationMetrics']}"
                )
        finally:
            spark.stop()
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

    if args.command == "delta":
        from pncp_pipeline.transformations.delta_jobs import (
            create_delta_spark_session,
            run_delta_pipeline,
        )

        spark = create_delta_spark_session()
        try:
            summary = run_delta_pipeline(spark, settings.raw_data_dir, settings.delta_data_dir)
        finally:
            spark.stop()
        log.info(
            "delta_pipeline_concluido",
            extra={
                "files": summary.files_read,
                "rows_in": summary.rows_in,
                "rows_silver_batch": summary.rows_silver_batch,
                "total_silver_delta": summary.total_silver_delta,
                "gold_rows": summary.gold_rows,
                "delta_version": summary.delta_version,
            },
        )
        return 0

    if args.command == "delta-history":
        from pncp_pipeline.transformations.delta_jobs import (
            create_delta_spark_session,
            get_delta_history,
        )

        silver_delta = settings.delta_data_dir / "silver" / "contratacoes"
        spark = create_delta_spark_session("pncp-delta-history")
        try:
            for item in get_delta_history(spark, silver_delta):
                print(
                    f"v{item['version']} | {item['timestamp']} | "
                    f"{item['operation']} | {item['operationMetrics']}"
                )
        finally:
            spark.stop()
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
