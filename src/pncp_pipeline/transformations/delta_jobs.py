from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pncp_pipeline.transformations.spark_jobs import (
    build_gold_uf_mes_spark,
    create_spark_session,
    read_bronze_spark,
    transform_silver_spark,
)


@dataclass(frozen=True)
class DeltaSummary:
    files_read: int
    rows_in: int
    rows_silver_batch: int
    total_silver_delta: int
    gold_rows: int
    delta_version: int


def create_delta_spark_session(app_name: str = "pncp-delta") -> SparkSession:
    return create_spark_session(app_name)


def merge_silver_delta(spark: SparkSession, silver_df: DataFrame, delta_silver_path: Path) -> int:
    """Cria ou atualiza tabela Silver Delta via MERGE com guarda monotônica em data_atualizacao."""
    target_str = str(delta_silver_path)
    if not DeltaTable.isDeltaTable(spark, target_str):
        (
            silver_df.repartition("pub_year", "pub_month", "uf_sigla")
            .write.format("delta")
            .mode("overwrite")
            .partitionBy("pub_year", "pub_month", "uf_sigla")
            .save(target_str)
        )
    else:
        delta_table = DeltaTable.forPath(spark, target_str)
        (
            delta_table.alias("target")
            .merge(
                silver_df.alias("src"),
                "target.numero_controle_pncp = src.numero_controle_pncp",
            )
            .whenMatchedUpdateAll(
                condition=(
                    "target.data_atualizacao IS NULL "
                    "OR src.data_atualizacao >= target.data_atualizacao"
                )
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    latest_history = DeltaTable.forPath(spark, target_str).history(1).collect()
    return int(latest_history[0]["version"])


def run_delta_pipeline(
    spark: SparkSession, raw_dir: Path, delta_dir: Path
) -> DeltaSummary:
    """Executa ingestão Bronze (PNCP) -> Silver Delta (MERGE) -> Gold Delta."""
    pncp_raw = raw_dir / "source=pncp"
    scan_dir = pncp_raw if pncp_raw.exists() else raw_dir
    files_read, flat_df = read_bronze_spark(spark, scan_dir)
    delta_silver_path = delta_dir / "silver" / "contratacoes"
    delta_gold_path = delta_dir / "gold" / "resumo_uf_mes"

    if files_read == 0:
        return DeltaSummary(
            files_read=0,
            rows_in=0,
            rows_silver_batch=0,
            total_silver_delta=0,
            gold_rows=0,
            delta_version=-1,
        )

    rows_in = flat_df.count()
    silver_df, _ = transform_silver_spark(flat_df)
    rows_silver_batch = silver_df.count()
    if rows_silver_batch == 0:
        return DeltaSummary(
            files_read=files_read,
            rows_in=rows_in,
            rows_silver_batch=0,
            total_silver_delta=0,
            gold_rows=0,
            delta_version=-1,
        )

    version = merge_silver_delta(spark, silver_df, delta_silver_path)

    full_silver = spark.read.format("delta").load(str(delta_silver_path))
    total_silver = full_silver.count()
    gold_df = build_gold_uf_mes_spark(full_silver)
    gold_rows = gold_df.count()
    gold_df.coalesce(1).write.format("delta").mode("overwrite").save(str(delta_gold_path))

    return DeltaSummary(
        files_read=files_read,
        rows_in=rows_in,
        rows_silver_batch=rows_silver_batch,
        total_silver_delta=total_silver,
        gold_rows=gold_rows,
        delta_version=version,
    )


def get_delta_history(spark: SparkSession, delta_table_path: Path) -> list[dict[str, Any]]:
    table = DeltaTable.forPath(spark, str(delta_table_path))
    rows = (
        table.history()
        .select("version", "timestamp", "operation", "operationMetrics")
        .orderBy(F.col("version").desc())
        .collect()
    )
    return [row.asDict() for row in rows]


def read_delta_version(spark: SparkSession, delta_table_path: Path, version: int) -> DataFrame:
    return spark.read.format("delta").option("versionAsOf", version).load(str(delta_table_path))
