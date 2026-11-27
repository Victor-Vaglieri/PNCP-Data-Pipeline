from __future__ import annotations

import importlib.util
import io
import os
import shutil
from collections.abc import Sequence
from contextlib import redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from pncp_pipeline.ingestion.bronze_checkpoint import (
    discover_bronze_files,
    fingerprint_bronze_files,
    full_refresh_from_env,
    locked_bronze_checkpoint,
)
from pncp_pipeline.transformations.pandas_baseline import TransformSummary

ORGAO_SCHEMA = StructType(
    [
        StructField("cnpj", StringType(), True),
        StructField("razaoSocial", StringType(), True),
        StructField("poderId", StringType(), True),
        StructField("esferaId", StringType(), True),
    ]
)

UNIDADE_SCHEMA = StructType(
    [
        StructField("codigoIbge", StringType(), True),
        StructField("municipioNome", StringType(), True),
        StructField("ufSigla", StringType(), True),
        StructField("ufNome", StringType(), True),
    ]
)

CONTRATACAO_ITEM_SCHEMA = StructType(
    [
        StructField("numeroControlePNCP", StringType(), True),
        StructField("orgaoEntidade", ORGAO_SCHEMA, True),
        StructField("unidadeOrgao", UNIDADE_SCHEMA, True),
        StructField("modalidadeId", IntegerType(), True),
        StructField("modalidadeNome", StringType(), True),
        StructField("anoCompra", IntegerType(), True),
        StructField("sequencialCompra", IntegerType(), True),
        StructField("numeroCompra", StringType(), True),
        StructField("objetoCompra", StringType(), True),
        StructField("srp", BooleanType(), True),
        StructField("situacaoCompraId", IntegerType(), True),
        StructField("situacaoCompraNome", StringType(), True),
        StructField("valorTotalEstimado", DoubleType(), True),
        StructField("valorTotalHomologado", DoubleType(), True),
        StructField("dataInclusao", StringType(), True),
        StructField("dataPublicacaoPncp", StringType(), True),
        StructField("dataAtualizacao", StringType(), True),
    ]
)

BRONZE_ENVELOPE_SCHEMA = StructType(
    [
        StructField(
            "metadata",
            StructType(
                [
                    StructField("run_id", StringType(), True),
                    StructField("ingested_at", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "payload",
            StructType([StructField("data", ArrayType(CONTRATACAO_ITEM_SCHEMA), True)]),
            True,
        ),
    ]
)


def create_spark_session(app_name: str = "pncp-pipeline") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    if importlib.util.find_spec("delta") is not None:
        from delta import configure_spark_with_delta_pip

        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()
    return builder.getOrCreate()


def _norm_str(col: F.Column, *, upper: bool = False) -> F.Column:
    collapsed = F.trim(F.regexp_replace(col, r"\s+", " "))
    null_if_blank = F.when(collapsed == "", F.lit(None)).otherwise(collapsed)
    return F.upper(null_if_blank) if upper else null_if_blank


def read_bronze_spark(
    spark: SparkSession,
    raw_dir: Path,
    *,
    files: Sequence[Path] | None = None,
) -> tuple[int, DataFrame]:
    selected = list(files) if files is not None else discover_bronze_files(raw_dir)
    if not selected:
        empty = spark.createDataFrame([], BRONZE_ENVELOPE_SCHEMA)
        return 0, empty

    raw = (
        spark.read.option("multiLine", "true")
        .schema(BRONZE_ENVELOPE_SCHEMA)
        .json([str(f) for f in selected])
        .withColumn("source_file", F.input_file_name())
    )
    exploded = raw.select(
        F.col("metadata.run_id").alias("run_id"),
        F.to_timestamp(F.col("metadata.ingested_at")).alias("ingested_at"),
        F.col("source_file"),
        F.explode(F.col("payload.data")).alias("item"),
    )
    flat = exploded.select(
        _norm_str(F.col("item.numeroControlePNCP")).alias("numero_controle_pncp"),
        _norm_str(F.col("item.orgaoEntidade.cnpj")).alias("orgao_cnpj"),
        F.coalesce(_norm_str(F.col("item.orgaoEntidade.razaoSocial"), upper=True), F.lit("")).alias(
            "orgao_razao_social"
        ),
        _norm_str(F.col("item.orgaoEntidade.poderId")).alias("orgao_poder_id"),
        _norm_str(F.col("item.orgaoEntidade.esferaId")).alias("orgao_esfera_id"),
        _norm_str(F.col("item.unidadeOrgao.codigoIbge")).alias("codigo_ibge"),
        _norm_str(F.col("item.unidadeOrgao.municipioNome")).alias("municipio_nome"),
        F.coalesce(_norm_str(F.col("item.unidadeOrgao.ufSigla"), upper=True), F.lit("ND")).alias(
            "uf_sigla"
        ),
        F.col("item.modalidadeId").alias("modalidade_id"),
        F.coalesce(_norm_str(F.col("item.modalidadeNome")), F.lit("")).alias("modalidade_nome"),
        F.col("item.anoCompra").alias("ano_compra"),
        F.col("item.sequencialCompra").alias("sequencial_compra"),
        _norm_str(F.col("item.numeroCompra")).alias("numero_compra"),
        _norm_str(F.col("item.objetoCompra")).alias("objeto_compra"),
        F.col("item.srp").alias("srp"),
        F.col("item.situacaoCompraId").alias("situacao_compra_id"),
        _norm_str(F.col("item.situacaoCompraNome")).alias("situacao_compra_nome"),
        F.col("item.valorTotalEstimado").alias("valor_total_estimado"),
        F.col("item.valorTotalHomologado").alias("valor_total_homologado"),
        F.to_timestamp(F.col("item.dataInclusao")).alias("data_inclusao"),
        F.to_timestamp(F.col("item.dataPublicacaoPncp")).alias("data_publicacao_pncp"),
        F.to_timestamp(F.col("item.dataAtualizacao")).alias("data_atualizacao"),
        F.col("ingested_at"),
        F.col("run_id"),
        F.col("source_file"),
    )
    return len(selected), flat


def _deduplicate_silver_spark(df: DataFrame) -> DataFrame:
    dedup_window = Window.partitionBy("numero_controle_pncp").orderBy(
        F.col("data_atualizacao").desc_nulls_last(),
        F.col("ingested_at").desc_nulls_last(),
    )
    return (
        df.withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def transform_silver_spark(flat_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Aplica validação com quarentena, deduplicação por janela e colunas derivadas."""
    reason = (
        F.when(F.col("numero_controle_pncp").isNull(), F.lit("missing_numero_controle_pncp"))
        .when(
            F.col("orgao_cnpj").isNull() | ~F.col("orgao_cnpj").rlike(r"^\d{14}$"),
            F.lit("invalid_cnpj"),
        )
        .when(F.col("modalidade_id").isNull(), F.lit("missing_modalidade_id"))
        .when(
            F.col("ano_compra").isNull() | ~F.col("ano_compra").between(2000, 2100),
            F.lit("invalid_ano_compra"),
        )
        .when(F.col("data_publicacao_pncp").isNull(), F.lit("missing_data_publicacao"))
        .when(
            F.col("valor_total_estimado").isNotNull() & (F.col("valor_total_estimado") < 0),
            F.lit("negative_valor_estimado"),
        )
        .when(
            F.col("valor_total_homologado").isNotNull() & (F.col("valor_total_homologado") < 0),
            F.lit("negative_valor_homologado"),
        )
    )

    tagged = flat_df.withColumn("rejection_reason", reason)
    rejected = tagged.filter(F.col("rejection_reason").isNotNull())
    valid_raw = tagged.filter(F.col("rejection_reason").isNull()).drop("rejection_reason")

    deduped = _deduplicate_silver_spark(valid_raw)

    silver = (
        deduped.withColumn("pub_year", F.year("data_publicacao_pncp").cast("int"))
        .withColumn("pub_month", F.month("data_publicacao_pncp").cast("int"))
        .withColumn("tem_homologacao", F.col("valor_total_homologado").isNotNull())
        .withColumn(
            "variacao_homologado_pct",
            F.when(
                F.col("tem_homologacao") & (F.col("valor_total_estimado") > 0),
                F.round(
                    (F.col("valor_total_homologado") - F.col("valor_total_estimado"))
                    / F.col("valor_total_estimado")
                    * 100.0,
                    2,
                ),
            ),
        )
    )
    return silver, rejected


def build_gold_uf_mes_spark(silver_df: DataFrame) -> DataFrame:
    economia = F.when(
        F.col("valor_total_homologado").isNotNull() & F.col("valor_total_estimado").isNotNull(),
        F.col("valor_total_estimado") - F.col("valor_total_homologado"),
    ).otherwise(F.lit(0.0))

    return (
        silver_df.groupBy(
            "pub_year", "pub_month", "uf_sigla", "modalidade_id", "modalidade_nome"
        )
        .agg(
            F.count("numero_controle_pncp").alias("qtd_contratacoes"),
            F.count("valor_total_homologado").alias("qtd_homologadas"),
            F.round(F.sum(F.coalesce(F.col("valor_total_estimado"), F.lit(0.0))), 2).alias(
                "total_estimado"
            ),
            F.round(F.sum(F.coalesce(F.col("valor_total_homologado"), F.lit(0.0))), 2).alias(
                "total_homologado"
            ),
            F.round(F.sum(economia), 2).alias("economia_homologada"),
        )
        .orderBy(F.col("pub_year").asc(), F.col("pub_month").asc(), F.col("total_estimado").desc())
    )


def build_gold_ranking_orgaos_spark(silver_df: DataFrame) -> DataFrame:
    agg = silver_df.groupBy(
        "pub_year", "pub_month", "uf_sigla", "orgao_cnpj", "orgao_razao_social"
    ).agg(
        F.count("numero_controle_pncp").alias("qtd_contratacoes"),
        F.round(F.sum(F.coalesce(F.col("valor_total_estimado"), F.lit(0.0))), 2).alias(
            "total_estimado"
        ),
        F.round(F.sum(F.coalesce(F.col("valor_total_homologado"), F.lit(0.0))), 2).alias(
            "total_homologado"
        ),
    )
    rank_win = Window.partitionBy("pub_year", "pub_month", "uf_sigla").orderBy(
        F.col("total_estimado").desc(), F.col("orgao_cnpj").asc()
    )
    return agg.withColumn("rank_uf", F.dense_rank().over(rank_win))


@dataclass
class _DirSwapRecord:
    target: Path
    backup: Path | None = None
    staging: Path | None = None
    staging_moved: bool = False


def _commit_dir_changes(
    replacements: Sequence[tuple[Path, Path]],
    removals: Sequence[Path] = (),
) -> None:
    """Troca e/ou remove múltiplos diretórios com rollback coletivo em caso de falha."""
    records: list[_DirSwapRecord] = []
    try:
        for staging, target in replacements:
            target.parent.mkdir(parents=True, exist_ok=True)
            rec = _DirSwapRecord(target=target, staging=staging)
            if target.exists():
                backup = target.parent / f".bak_{target.name}_{uuid4().hex[:8]}"
                os.replace(target, backup)
                rec.backup = backup
            records.append(rec)
            os.replace(staging, target)
            rec.staging_moved = True

        for target in removals:
            if target.exists():
                backup = target.parent / f".bak_{target.name}_{uuid4().hex[:8]}"
                os.replace(target, backup)
                records.append(_DirSwapRecord(target=target, backup=backup))
    except BaseException:
        for rec in reversed(records):
            if rec.staging_moved and rec.staging is not None and rec.target.exists():
                try:
                    os.replace(rec.target, rec.staging)
                except OSError:
                    shutil.rmtree(rec.target, ignore_errors=True)
            if rec.backup is not None and rec.backup.exists() and not rec.target.exists():
                os.replace(rec.backup, rec.target)
        raise
    else:
        for rec in records:
            if rec.backup is not None and rec.backup.exists():
                shutil.rmtree(rec.backup, ignore_errors=True)


def _swap_dir(staging: Path, target: Path) -> None:
    _commit_dir_changes([(staging, target)])


def _merge_rejected_spark(
    existing_rejected: DataFrame, batch_rejected_df: DataFrame
) -> DataFrame:
    """Mescla rejeitados substituindo apenas os source_file do lote, preservando NULLs."""
    batch_sources = batch_rejected_df.select("source_file").distinct()
    kept_existing = existing_rejected.join(batch_sources, on="source_file", how="left_anti")
    return kept_existing.unionByName(batch_rejected_df, allowMissingColumns=True)


def run_spark_pipeline(
    spark: SparkSession,
    raw_dir: Path,
    silver_dir: Path,
    gold_dir: Path,
    *,
    incremental: bool = True,
    full_refresh: bool | None = None,
    checkpoint_path: Path | None = None,
) -> TransformSummary:
    effective_full_refresh = full_refresh_from_env(full_refresh)
    clear_empty_outputs = effective_full_refresh or not incremental
    silver_contratacoes = silver_dir / "contratacoes"
    silver_rejected = silver_dir / "rejected" / "contratacoes"
    gold_uf_path = gold_dir / "resumo_uf_mes"
    gold_orgaos_path = gold_dir / "ranking_orgaos"
    ckpt = checkpoint_path or (silver_dir / "_checkpoints" / "bronze_to_silver.json")
    run_id = uuid4().hex[:12]

    # Calcula SHA-256 fora da seção crítica do lock exclusivo
    precomputed = fingerprint_bronze_files(raw_dir)

    with locked_bronze_checkpoint(ckpt, consumer="spark_silver_gold") as session:
        pending = session.pending(
            raw_dir,
            full_refresh=clear_empty_outputs,
            precomputed=precomputed,
        )
        if not pending:
            if clear_empty_outputs:
                _commit_dir_changes(
                    [],
                    removals=[
                        silver_contratacoes,
                        silver_rejected,
                        gold_uf_path,
                        gold_orgaos_path,
                    ],
                )
                if incremental:
                    session.commit([], run_id=run_id, replace_all=True)
            return TransformSummary(
                engine="pyspark",
                files_read=0,
                rows_in=0,
                rows_valid_dedup=0,
                rows_rejected=0,
                gold_uf_rows=0,
                silver_path=silver_contratacoes,
                gold_path=gold_uf_path,
            )

        files_read, flat_df = read_bronze_spark(spark, raw_dir, files=[fp.path for fp in pending])
        rows_in = flat_df.count()
        batch_silver_df, batch_rejected_df = transform_silver_spark(flat_df)

        has_existing_silver = (
            incremental
            and not effective_full_refresh
            and silver_contratacoes.exists()
            and any(silver_contratacoes.rglob("*.parquet"))
        )
        batch_valid_count = batch_silver_df.count()
        if batch_valid_count > 0 and has_existing_silver:
            existing_silver_df = spark.read.parquet(str(silver_contratacoes))
            silver_df = _deduplicate_silver_spark(
                existing_silver_df.unionByName(batch_silver_df, allowMissingColumns=True)
            )
        else:
            silver_df = batch_silver_df

        silver_df.cache()
        silver_cached = True
        rows_valid = silver_df.count()
        rows_rejected = batch_rejected_df.count()

        staging_dirs: list[tuple[Path, Path]] = []
        removal_dirs: list[Path] = []
        try:
            if rows_valid > 0:
                staging_silver = silver_dir / f".tmp_contratacoes_{uuid4().hex[:8]}"
                (
                    silver_df.repartition("pub_year", "pub_month", "uf_sigla")
                    .write.mode("overwrite")
                    .partitionBy("pub_year", "pub_month", "uf_sigla")
                    .parquet(str(staging_silver))
                )
                staging_dirs.append((staging_silver, silver_contratacoes))

                gold_uf_df = build_gold_uf_mes_spark(silver_df)
                gold_uf_rows = gold_uf_df.count()
                staging_gold_uf = gold_dir / f".tmp_resumo_uf_mes_{uuid4().hex[:8]}"
                gold_uf_df.coalesce(1).write.mode("overwrite").parquet(str(staging_gold_uf))
                staging_dirs.append((staging_gold_uf, gold_uf_path))

                staging_gold_orgaos = gold_dir / f".tmp_ranking_orgaos_{uuid4().hex[:8]}"
                build_gold_ranking_orgaos_spark(silver_df).coalesce(1).write.mode(
                    "overwrite"
                ).parquet(str(staging_gold_orgaos))
                staging_dirs.append((staging_gold_orgaos, gold_orgaos_path))
            else:
                gold_uf_rows = 0
                if clear_empty_outputs:
                    removal_dirs.extend([silver_contratacoes, gold_uf_path, gold_orgaos_path])

            if rows_rejected > 0:
                rejected_to_write = batch_rejected_df
                if (
                    incremental
                    and not effective_full_refresh
                    and silver_rejected.exists()
                    and any(silver_rejected.rglob("*.parquet"))
                ):
                    existing_rejected = spark.read.parquet(str(silver_rejected))
                    rejected_to_write = _merge_rejected_spark(
                        existing_rejected, batch_rejected_df
                    )
                staging_rejected = silver_rejected.parent / f".tmp_contratacoes_{uuid4().hex[:8]}"
                rejected_to_write.coalesce(1).write.mode("overwrite").parquet(
                    str(staging_rejected)
                )
                staging_dirs.append((staging_rejected, silver_rejected))
            elif clear_empty_outputs:
                removal_dirs.append(silver_rejected)

            silver_df.unpersist()
            silver_cached = False

            _commit_dir_changes(staging_dirs, removals=removal_dirs)
            staging_dirs.clear()
        finally:
            if silver_cached:
                silver_df.unpersist()
            for staging, _ in staging_dirs:
                shutil.rmtree(staging, ignore_errors=True)

        if incremental:
            session.commit(pending, run_id=run_id, replace_all=effective_full_refresh)

        return TransformSummary(
            engine="pyspark",
            files_read=files_read,
            rows_in=rows_in,
            rows_valid_dedup=rows_valid,
            rows_rejected=rows_rejected,
            gold_uf_rows=gold_uf_rows,
            silver_path=silver_contratacoes,
            gold_path=gold_uf_path,
        )


def inspect_parquet_pruning(spark: SparkSession, silver_dir: Path, uf: str = "SP") -> str:
    """Lê Silver Parquet filtrando por UF e captura o plano físico com PartitionFilters."""
    contratacoes = silver_dir / "contratacoes"
    df = spark.read.parquet(str(contratacoes)).filter(F.col("uf_sigla") == uf)
    buf = io.StringIO()
    with redirect_stdout(buf):
        print(f"Schema Silver ({contratacoes}):")
        df.printSchema()
        print(f"Total registros para uf_sigla='{uf}': {df.count()}")
        df.explain(mode="formatted")
    return buf.getvalue()
