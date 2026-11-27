from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.sdk import dag, get_current_context, task

from pncp_pipeline.common.quality import run_quality_checks
from pncp_pipeline.config import Settings
from pncp_pipeline.database.loader import load_bronze_to_postgres
from pncp_pipeline.ingestion.bronze_checkpoint import full_refresh_from_env
from pncp_pipeline.ingestion.bronze_writer import ingest_contratacoes
from pncp_pipeline.ingestion.client import PncpClient
from pncp_pipeline.transformations.spark_jobs import create_spark_session, run_spark_pipeline

log = logging.getLogger(__name__)

DEFAULT_MODALITIES = [6, 8]  # 6 = Pregão Eletrônico, 8 = Dispensa

_TRUE_STRINGS = frozenset({"1", "true", "t", "yes", "y", "on"})
_FALSE_STRINGS = frozenset({"0", "false", "f", "no", "n", "off", ""})


def _parse_optional_bool(value: Any, *, field_name: str = "full_refresh") -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in _TRUE_STRINGS:
            return True
        if normalized in _FALSE_STRINGS:
            return False
    raise ValueError(f"Valor booleano inválido para {field_name}: {value!r}")


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
        full_refresh = _parse_optional_bool(conf.get("full_refresh"))
        return {
            "start_date": api_date,
            "end_date": api_date,
            "max_pages": int(max_pages) if max_pages is not None else None,
            "modalities": [int(m) for m in modalities],
            "full_refresh": full_refresh,
        }

    @task
    def list_ingestion_targets(window: dict[str, Any]) -> list[dict[str, Any]]:
        full_refresh = _parse_optional_bool(window.get("full_refresh"))
        return [
            {
                "start_date": window["start_date"],
                "end_date": window["end_date"],
                "modality": m,
                "max_pages": window["max_pages"],
                "full_refresh": full_refresh,
            }
            for m in window["modalities"]
        ]

    @task
    def ingest_modality(target: dict[str, Any]) -> dict[str, Any]:
        settings = Settings.from_env()
        full_refresh = _parse_optional_bool(target.get("full_refresh"))
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
            "full_refresh": full_refresh,
        }

    @task
    def transform_silver_gold(ingest_summaries: list[dict[str, Any]]) -> dict[str, Any]:
        settings = Settings.from_env()
        flags = [_parse_optional_bool(item.get("full_refresh")) for item in ingest_summaries]
        if any(flag is True for flag in flags):
            explicit_full_refresh: bool | None = True
        elif any(flag is False for flag in flags):
            explicit_full_refresh = False
        else:
            explicit_full_refresh = None
        effective_full_refresh = full_refresh_from_env(explicit_full_refresh)
        spark = create_spark_session("pncp-airflow-spark")
        try:
            summary = run_spark_pipeline(
                spark,
                settings.raw_data_dir,
                settings.silver_data_dir,
                settings.gold_data_dir,
                full_refresh=effective_full_refresh,
            )
        finally:
            spark.stop()
        log.info(
            "spark_transform in=%s valid_dedup=%s rejected=%s gold=%s full_refresh=%s",
            summary.rows_in,
            summary.rows_valid_dedup,
            summary.rows_rejected,
            summary.gold_uf_rows,
            effective_full_refresh,
        )
        return {
            "rows_in": summary.rows_in,
            "rows_valid_dedup": summary.rows_valid_dedup,
            "rows_rejected": summary.rows_rejected,
            "gold_uf_rows": summary.gold_uf_rows,
            "ingested_modalities": len(ingest_summaries),
            "full_refresh": explicit_full_refresh,
            "effective_full_refresh": effective_full_refresh,
        }

    @task
    def load_postgres(transform_summary: dict[str, Any]) -> dict[str, Any]:
        settings = Settings.from_env()
        explicit_full_refresh = _parse_optional_bool(transform_summary.get("full_refresh"))
        effective_full_refresh = full_refresh_from_env(explicit_full_refresh)
        summary = load_bronze_to_postgres(
            settings.raw_data_dir,
            settings.database_url,
            full_refresh=effective_full_refresh,
        )
        log.info(
            "postgres_load run_id=%s files=%s rows_in=%s rows_dedup=%s full_refresh=%s",
            summary.run_id,
            summary.files_read,
            summary.rows_in,
            summary.rows_deduplicated,
            effective_full_refresh,
        )
        return {
            "run_id": summary.run_id,
            "rows_in": summary.rows_in,
            "rows_deduplicated": summary.rows_deduplicated,
            "full_refresh": explicit_full_refresh,
            "effective_full_refresh": effective_full_refresh,
        }

    @task
    def data_quality_checks(_load_summary: dict[str, Any]) -> dict[str, int | None]:
        settings = Settings.from_env()
        report = run_quality_checks(
            settings.silver_data_dir,
            settings.database_url,
            gold_dir=settings.gold_data_dir,
        )
        log.info(
            "qa_passed silver_rows=%s gold_rows=%s fact_rows=%s duplicates=%s negatives=%s",
            report.silver_rows,
            report.gold_rows,
            report.fact_rows,
            report.duplicate_keys,
            report.negative_values,
        )
        return {
            "silver_rows": report.silver_rows,
            "gold_rows": report.gold_rows,
            "fact_rows": report.fact_rows,
            "duplicate_keys": report.duplicate_keys,
            "null_required_fields": report.null_required_fields,
            "negative_values": report.negative_values,
        }

    window = resolve_window()
    targets = list_ingestion_targets(window)
    ingested = ingest_modality.expand(target=targets)
    transformed = transform_silver_gold(ingested)
    loaded = load_postgres(transformed)
    data_quality_checks(loaded)


pncp_contratacoes_pipeline()
