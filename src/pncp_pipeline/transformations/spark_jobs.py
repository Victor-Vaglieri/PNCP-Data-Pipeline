from __future__ import annotations

import importlib.util
import io
from contextlib import redirect_stdout
from pathlib import Path

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


def read_bronze_spark(spark: SparkSession, raw_dir: Path) -> tuple[int, DataFrame]:
    scan_dir = raw_dir / "source=pncp" if (raw_dir / "source=pncp").exists() else raw_dir
    files = sorted(scan_dir.rglob("*.json"))
    if not files:
        empty = spark.createDataFrame([], BRONZE_ENVELOPE_SCHEMA)
        return 0, empty

    raw = (
        spark.read.option("multiLine", "true")
        .schema(BRONZE_ENVELOPE_SCHEMA)
        .json([str(f) for f in files])
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
    return len(files), flat


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

    dedup_window = Window.partitionBy("numero_controle_pncp").orderBy(
        F.col("data_atualizacao").desc_nulls_last(),
        F.col("ingested_at").desc_nulls_last(),
    )
    deduped = (
        valid_raw.withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

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


def run_spark_pipeline(
    spark: SparkSession, raw_dir: Path, silver_dir: Path, gold_dir: Path
) -> TransformSummary:
    files_read, flat_df = read_bronze_spark(spark, raw_dir)
    if files_read == 0:
        return TransformSummary(
            engine="pyspark",
            files_read=0,
            rows_in=0,
            rows_valid_dedup=0,
            rows_rejected=0,
            gold_uf_rows=0,
            silver_path=silver_dir / "contratacoes",
            gold_path=gold_dir / "resumo_uf_mes",
        )

    rows_in = flat_df.count()
    silver_df, rejected_df = transform_silver_spark(flat_df)
    silver_df.cache()

    silver_contratacoes = silver_dir / "contratacoes"
    silver_rejected = silver_dir / "rejected" / "contratacoes"
    gold_uf_path = gold_dir / "resumo_uf_mes"
    gold_orgaos_path = gold_dir / "ranking_orgaos"

    rows_valid = silver_df.count()
    rows_rejected = rejected_df.count()

    if rows_valid > 0:
        (
            silver_df.repartition("pub_year", "pub_month", "uf_sigla")
            .write.mode("overwrite")
            .partitionBy("pub_year", "pub_month", "uf_sigla")
            .parquet(str(silver_contratacoes))
        )
        gold_uf_df = build_gold_uf_mes_spark(silver_df)
        gold_uf_rows = gold_uf_df.count()
        gold_uf_df.coalesce(1).write.mode("overwrite").parquet(str(gold_uf_path))
        build_gold_ranking_orgaos_spark(silver_df).coalesce(1).write.mode("overwrite").parquet(
            str(gold_orgaos_path)
        )
    else:
        gold_uf_rows = 0

    if rows_rejected > 0:
        rejected_df.coalesce(1).write.mode("overwrite").parquet(str(silver_rejected))

    silver_df.unpersist()

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
