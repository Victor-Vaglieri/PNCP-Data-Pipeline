from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq

from pncp_pipeline.transformations.pandas_baseline import (
    build_gold_uf_mes_pandas,
    read_bronze_dataframe,
    run_pandas_pipeline,
    transform_silver_pandas,
)


def _bronze_record(
    numero_controle: str = "87613071000148-1-000330/2023",
    cnpj: str = "87613071000148",
    razao: str = "  Municipio  de Santo Angelo ",
    uf: str | None = "rs",
    estimado: float | None = 1000.0,
    homologado: float | None = 800.0,
    data_atualizacao: str = "2024-05-24T12:03:45",
    ano_compra: int = 2023,
) -> dict:
    return {
        "numeroControlePNCP": numero_controle,
        "orgaoEntidade": {
            "cnpj": cnpj,
            "razaoSocial": razao,
            "poderId": "N",
            "esferaId": "M",
        },
        "unidadeOrgao": {
            "codigoIbge": "4317509",
            "municipioNome": "Santo Ângelo",
            "ufSigla": uf,
            "ufNome": "Rio Grande do Sul",
        },
        "modalidadeId": 6,
        "modalidadeNome": "Pregão - Eletrônico",
        "anoCompra": ano_compra,
        "sequencialCompra": 330,
        "numeroCompra": "0020",
        "objetoCompra": "registro de preços",
        "srp": True,
        "situacaoCompraId": 1,
        "situacaoCompraNome": "Divulgada no PNCP",
        "valorTotalEstimado": estimado,
        "valorTotalHomologado": homologado,
        "dataInclusao": "2024-01-02T00:01:21",
        "dataPublicacaoPncp": "2024-01-02T00:01:21",
        "dataAtualizacao": data_atualizacao,
    }


def _write_bronze(path: Path, run_id: str, ingested_at: str, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "metadata": {"run_id": run_id, "ingested_at": ingested_at},
                "payload": {"data": records},
            }
        ),
        encoding="utf-8",
    )


def test_pandas_silver_quarantines_invalid_and_deduplicates(tmp_path: Path) -> None:
    _write_bronze(
        tmp_path / "p1.json",
        "run1",
        "2026-11-23T10:00:00+00:00",
        [
            _bronze_record(homologado=None, data_atualizacao="2024-01-02T10:00:00"),
            _bronze_record(numero_controle="BAD-CNPJ", cnpj="123"),
            _bronze_record(numero_controle="NEG-VAL", estimado=-10.0),
        ],
    )
    _write_bronze(
        tmp_path / "p2.json",
        "run2",
        "2026-11-23T11:00:00+00:00",
        [
            _bronze_record(homologado=800.0, data_atualizacao="2024-05-24T12:03:45"),
            _bronze_record(numero_controle="NO-UF", cnpj="11111111000191", uf=None),
        ],
    )

    files_read, raw_df = read_bronze_dataframe(tmp_path)
    valid_df, rejected_df = transform_silver_pandas(raw_df)

    assert files_read == 2
    assert len(raw_df) == 5
    assert len(rejected_df) == 2
    assert set(rejected_df["rejection_reason"]) == {"invalid_cnpj", "negative_valor_estimado"}

    assert len(valid_df) == 2
    deduped_row = valid_df.loc[
        valid_df["numero_controle_pncp"] == "87613071000148-1-000330/2023"
    ].iloc[0]
    assert deduped_row["orgao_razao_social"] == "MUNICIPIO DE SANTO ANGELO"
    assert deduped_row["uf_sigla"] == "RS"
    assert deduped_row["valor_total_homologado"] == 800.0
    assert deduped_row["variacao_homologado_pct"] == -20.0

    no_uf_row = valid_df.loc[valid_df["numero_controle_pncp"] == "NO-UF"].iloc[0]
    assert no_uf_row["uf_sigla"] == "ND"


def test_run_pandas_pipeline_writes_partitioned_parquet_and_gold(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"

    _write_bronze(
        raw_dir / "p1.json",
        "run1",
        "2026-11-23T10:00:00+00:00",
        [
            _bronze_record(numero_controle="ID-1", uf="SP", estimado=1000.0, homologado=900.0),
            _bronze_record(numero_controle="ID-2", uf="SP", estimado=500.0, homologado=None),
            _bronze_record(numero_controle="ID-BAD", cnpj="invalid"),
        ],
    )

    summary = run_pandas_pipeline(raw_dir, silver_dir, gold_dir)

    assert summary.rows_in == 3
    assert summary.rows_valid_dedup == 2
    assert summary.rows_rejected == 1
    assert summary.gold_uf_rows == 1

    parquet_files = list((silver_dir / "contratacoes").rglob("*.parquet"))
    assert len(parquet_files) == 1
    assert "pub_year=2023" not in str(parquet_files[0])
    assert "pub_year=2024" in str(parquet_files[0])
    assert "uf_sigla=SP" in str(parquet_files[0])

    gold_table = pq.read_table(gold_dir / "resumo_uf_mes" / "resumo_uf_mes.parquet").to_pylist()
    assert gold_table == [
        {
            "pub_year": 2024,
            "pub_month": 1,
            "uf_sigla": "SP",
            "modalidade_id": 6,
            "modalidade_nome": "Pregão - Eletrônico",
            "qtd_contratacoes": 2,
            "qtd_homologadas": 1,
            "total_estimado": 1500.0,
            "total_homologado": 900.0,
            "economia_homologada": 100.0,
        }
    ]


def test_build_gold_uf_mes_handles_empty() -> None:
    _, empty = read_bronze_dataframe(Path("nonexistent_dir_xyz"))
    valid, _ = transform_silver_pandas(empty)
    assert build_gold_uf_mes_pandas(valid).empty
