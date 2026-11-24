from __future__ import annotations

import json
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
delta = pytest.importorskip("delta")

from pncp_pipeline.transformations.delta_jobs import (  # noqa: E402
    create_delta_spark_session,
    get_delta_history,
    read_delta_version,
    run_delta_pipeline,
)


def _sample_item(
    numero_controle: str,
    *,
    data_atualizacao: str,
    homologado: float | None,
) -> dict:
    return {
        "numeroControlePNCP": numero_controle,
        "orgaoEntidade": {
            "cnpj": "87613071000148",
            "razaoSocial": "MUNICIPIO DELTA",
            "poderId": "N",
            "esferaId": "M",
        },
        "unidadeOrgao": {
            "codigoIbge": "3550308",
            "municipioNome": "São Paulo",
            "ufSigla": "SP",
            "ufNome": "São Paulo",
        },
        "modalidadeId": 6,
        "modalidadeNome": "Pregão - Eletrônico",
        "anoCompra": 2024,
        "sequencialCompra": 1,
        "numeroCompra": "0001",
        "objetoCompra": "equipamentos",
        "srp": False,
        "situacaoCompraId": 1,
        "situacaoCompraNome": "Divulgada no PNCP",
        "valorTotalEstimado": 1000.0,
        "valorTotalHomologado": homologado,
        "dataInclusao": "2024-01-02T00:01:21",
        "dataPublicacaoPncp": "2024-01-02T00:01:21",
        "dataAtualizacao": data_atualizacao,
    }


def _write_bronze(path: Path, run_id: str, ingested_at: str, items: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "metadata": {"run_id": run_id, "ingested_at": ingested_at},
                "payload": {"data": items},
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def test_delta_merge_monotonic_update_and_time_travel(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    delta_dir = tmp_path / "delta"

    _write_bronze(
        raw_dir / "source=pncp" / "v0.json",
        "run0",
        "2026-11-24T10:00:00+00:00",
        [_sample_item("DELTA-1", data_atualizacao="2024-01-02T10:00:00", homologado=None)],
    )

    spark = create_delta_spark_session("pncp-delta-test")
    try:
        s0 = run_delta_pipeline(spark, raw_dir, delta_dir)
        assert s0.delta_version == 0
        assert s0.total_silver_delta == 1

        # Atualiza o registro existente com dataAtualizacao mais nova
        (raw_dir / "source=pncp" / "v0.json").unlink()
        _write_bronze(
            raw_dir / "source=pncp" / "v1.json",
            "run1",
            "2026-11-24T11:00:00+00:00",
            [_sample_item("DELTA-1", data_atualizacao="2024-05-24T12:00:00", homologado=820.0)],
        )
        s1 = run_delta_pipeline(spark, raw_dir, delta_dir)
        assert s1.delta_version == 1
        assert s1.total_silver_delta == 1

        # Tenta reprocessar versão antiga: MERGE deve preservar 820.0
        (raw_dir / "source=pncp" / "v1.json").unlink()
        _write_bronze(
            raw_dir / "source=pncp" / "v2.json",
            "run2",
            "2026-11-24T12:00:00+00:00",
            [_sample_item("DELTA-1", data_atualizacao="2024-01-02T10:00:00", homologado=999.0)],
        )
        s2 = run_delta_pipeline(spark, raw_dir, delta_dir)
        assert s2.delta_version == 2

        silver_delta = delta_dir / "silver" / "contratacoes"
        v0_row = read_delta_version(spark, silver_delta, 0).collect()[0]
        v2_row = spark.read.format("delta").load(str(silver_delta)).collect()[0]
        assert v0_row["valor_total_homologado"] is None
        assert v2_row["valor_total_homologado"] == 820.0

        history = get_delta_history(spark, silver_delta)
        assert [h["version"] for h in history] == [2, 1, 0]
    finally:
        spark.stop()
