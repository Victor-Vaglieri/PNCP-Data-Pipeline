from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.sdk import dag, get_current_context, task

from pncp_pipeline.common.quality import run_quality_checks
from pncp_pipeline.config import Settings
from pncp_pipeline.database.loader import load_bronze_to_postgres
from pncp_pipeline.ingestion.bronze_writer import ingest_contratacoes
from pncp_pipeline.ingestion.client import PncpClient
from pncp_pipeline.transformations.spark_jobs import create_spark_session, run_spark_pipeline

log = logging.getLogger(__name__)

DEFAULT_MODALITIES = [6, 8]  # 6 = Pregão Eletrônico, 8 = Dispensa


@dag(
    dag_id="pncp_contratacoes_pipeline",
    description="Orquestra ingestão PNCP -> Bronze -> PySpark Silver/Gold -> PostgreSQL -> QA",
    schedule="@daily",
    start_date=datetime(2024, 1, 2, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["pncp", "medallion", "pyspark"],
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "execution_timeout": timedelta(minutes=15),
    },
)
def pncp_contratacoes_pipeline() -> None:
    @task
    def resolve_window() -> dict[str, Any]:
        ctx = get_current_context()
        dag_run = ctx.get("dag_run")
        conf = (dag_run.conf or {}) if dag_run else {}
        target_date = conf.get("target_date")
        if target_date:
            api_date = str(target_date).replace("-", "")
        else:
            interval_start = ctx.get("data_interval_start") or ctx["logical_date"]
            api_date = interval_start.strftime("%Y%m%d")

        max_pages = conf.get("max_pages")
        modalities = conf.get("modalities") or DEFAULT_MODALITIES
        return {
            "start_date": api_date,
            "end_date": api_date,
            "max_pages": int(max_pages) if max_pages is not None else None,
            "modalities": [int(m) for m in modalities],
        }

    @task
    def list_ingestion_targets(window: dict[str, Any]) -> list[dict[str, Any]]:
        return [
            {
                "start_date": window["start_date"],
                "end_date": window["end_date"],
                "modality": m,
                "max_pages": window["max_pages"],
            }
            for m in window["modalities"]
        ]

    @task
    def ingest_modality(target: dict[str, Any]) -> dict[str, Any]:
        settings = Settings.from_env()
        with PncpClient(settings) as client:
            result = ingest_contratacoes(
                client=client,
                raw_dir=settings.raw_data_dir,
                start_date=target["start_date"],
                end_date=target["end_date"],
                modality=target["modality"],
                max_pages=target["max_pages"],
            )
        log.info(
            "ingest_modality modality=%s pages=%s records=%s run_id=%s",
            target["modality"],
            result.pages_saved,
            result.records_fetched,
            result.run_id,
        )
        return {
            "modality": target["modality"],
            "run_id": result.run_id,
            "pages_saved": result.pages_saved,
            "records_fetched": result.records_fetched,
        }

    @task
    def transform_silver_gold(ingest_summaries: list[dict[str, Any]]) -> dict[str, Any]:
        settings = Settings.from_env()
        spark = create_spark_session("pncp-airflow-spark")
        try:
            summary = run_spark_pipeline(
                spark,
                settings.raw_data_dir,
                settings.silver_data_dir,
                settings.gold_data_dir,
            )
        finally:
            spark.stop()
        log.info(
            "spark_transform in=%s valid_dedup=%s rejected=%s gold=%s",
            summary.rows_in,
            summary.rows_valid_dedup,
            summary.rows_rejected,
            summary.gold_uf_rows,
        )
        return {
            "rows_in": summary.rows_in,
            "rows_valid_dedup": summary.rows_valid_dedup,
            "rows_rejected": summary.rows_rejected,
            "gold_uf_rows": summary.gold_uf_rows,
            "ingested_modalities": len(ingest_summaries),
        }

    @task
    def load_postgres(_transform_summary: dict[str, Any]) -> dict[str, Any]:
        settings = Settings.from_env()
        summary = load_bronze_to_postgres(settings.raw_data_dir, settings.database_url)
        log.info(
            "postgres_load run_id=%s files=%s rows_in=%s rows_dedup=%s",
            summary.run_id,
            summary.files_read,
            summary.rows_in,
            summary.rows_deduplicated,
        )
        return {
            "run_id": summary.run_id,
            "rows_in": summary.rows_in,
            "rows_deduplicated": summary.rows_deduplicated,
        }

    @task
    def data_quality_checks(_load_summary: dict[str, Any]) -> dict[str, int]:
        settings = Settings.from_env()
        report = run_quality_checks(settings.silver_data_dir, settings.database_url)
        log.info(
            "qa_passed silver_rows=%s fact_rows=%s duplicates=%s negatives=%s",
            report.silver_rows,
            report.fact_rows,
            report.duplicate_keys,
            report.negative_values,
        )
        return {
            "silver_rows": report.silver_rows,
            "fact_rows": report.fact_rows,
            "duplicate_keys": report.duplicate_keys,
            "negative_values": report.negative_values,
        }

    window = resolve_window()
    targets = list_ingestion_targets(window)
    ingested = ingest_modality.expand(target=targets)
    transformed = transform_silver_gold(ingested)
    loaded = load_postgres(transformed)
    data_quality_checks(loaded)


pncp_contratacoes_pipeline()
