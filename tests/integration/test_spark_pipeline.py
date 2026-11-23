from __future__ import annotations

import json
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

from pncp_pipeline.transformations.pandas_baseline import (  # noqa: E402
    build_gold_uf_mes_pandas,
    read_bronze_dataframe,
    transform_silver_pandas,
)
from pncp_pipeline.transformations.spark_jobs import (  # noqa: E402
    build_gold_uf_mes_spark,
    create_spark_session,
    inspect_parquet_pruning,
    read_bronze_spark,
    run_spark_pipeline,
    transform_silver_spark,
)


def _sample_item(
    numero_controle: str,
    *,
    cnpj: str = "87613071000148",
    uf: str | None = "SP",
    estimado: float = 1000.0,
    homologado: float | None = 850.0,
    data_atualizacao: str = "2024-05-24T12:03:45",
) -> dict:
    return {
        "numeroControlePNCP": numero_controle,
        "orgaoEntidade": {
            "cnpj": cnpj,
            "razaoSocial": "  Municipio Teste ",
            "poderId": "N",
            "esferaId": "M",
        },
        "unidadeOrgao": {
            "codigoIbge": "3550308",
            "municipioNome": "São Paulo",
            "ufSigla": uf,
            "ufNome": "São Paulo",
        },
        "modalidadeId": 6,
        "modalidadeNome": "Pregão - Eletrônico",
        "anoCompra": 2024,
        "sequencialCompra": 1,
        "numeroCompra": "0001",
        "objetoCompra": "material de escritório",
        "srp": False,
        "situacaoCompraId": 1,
        "situacaoCompraNome": "Divulgada no PNCP",
        "valorTotalEstimado": estimado,
        "valorTotalHomologado": homologado,
        "dataInclusao": "2024-01-02T00:01:21",
        "dataPublicacaoPncp": "2024-01-02T00:01:21",
        "dataAtualizacao": data_atualizacao,
    }


def test_spark_and_pandas_produce_identical_silver_and_gold(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    raw_dir.mkdir()

    (raw_dir / "p1.json").write_text(
        json.dumps(
            {
                "metadata": {"run_id": "r1", "ingested_at": "2026-11-23T10:00:00+00:00"},
                "payload": {
                    "data": [
                        _sample_item(
                            "ID-1",
                            uf="SP",
                            homologado=None,
                            data_atualizacao="2024-01-02T10:00:00",
                        ),
                        _sample_item("ID-2", uf="RJ", estimado=500.0, homologado=450.0),
                        _sample_item("ID-BAD", cnpj="000"),
                    ]
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (raw_dir / "p2.json").write_text(
        json.dumps(
            {
                "metadata": {"run_id": "r2", "ingested_at": "2026-11-23T11:00:00+00:00"},
                "payload": {
                    "data": [
                        _sample_item(
                            "ID-1",
                            uf="SP",
                            homologado=800.0,
                            data_atualizacao="2024-05-24T12:03:45",
                        ),
                    ]
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    _, pd_raw = read_bronze_dataframe(raw_dir)
    pd_silver, pd_rejected = transform_silver_pandas(pd_raw)
    pd_gold = build_gold_uf_mes_pandas(pd_silver)

    spark = create_spark_session("pncp-parity-test")
    try:
        _, sp_raw = read_bronze_spark(spark, raw_dir)
        sp_silver, sp_rejected = transform_silver_spark(sp_raw)
        sp_gold = build_gold_uf_mes_spark(sp_silver)

        assert sp_rejected.count() == len(pd_rejected) == 1
        assert sp_silver.count() == len(pd_silver) == 2

        sp_keys = sorted(r["numero_controle_pncp"] for r in sp_silver.collect())
        pd_keys = sorted(pd_silver["numero_controle_pncp"].tolist())
        assert sp_keys == pd_keys == ["ID-1", "ID-2"]

        sp_gold_rows = [r.asDict() for r in sp_gold.collect()]
        assert sp_gold_rows == pd_gold.to_dict(orient="records")

        summary = run_spark_pipeline(spark, raw_dir, silver_dir, gold_dir)
        assert summary.rows_valid_dedup == 2
        assert summary.rows_rejected == 1

        pruning_output = inspect_parquet_pruning(spark, silver_dir, uf="SP")
        assert "PartitionFilters:" in pruning_output
        assert "uf_sigla" in pruning_output
    finally:
        spark.stop()
