from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from pncp_pipeline.database.loader import (
    SCHEMA_PATH,
    deduplicate_contratacoes,
    extract_record,
    read_bronze_records,
)


def _sample_raw(
    numero_controle: str = "87613071000148-1-000330/2023",
    data_atualizacao: str = "2024-05-24T12:03:45",
    valor_homologado: float | None = 709178.0,
    codigo_ibge: str | None = "4317509",
) -> dict:
    return {
        "numeroControlePNCP": numero_controle,
        "anoCompra": 2023,
        "sequencialCompra": 330,
        "numeroCompra": "0020",
        "processo": "485/2023",
        "objetoCompra": "registro de preços de fraldas",
        "srp": True,
        "modalidadeId": 6,
        "modalidadeNome": "Pregão - Eletrônico",
        "modoDisputaId": 1,
        "modoDisputaNome": "Aberto",
        "situacaoCompraId": 1,
        "situacaoCompraNome": "Divulgada no PNCP",
        "valorTotalEstimado": 938036.0,
        "valorTotalHomologado": valor_homologado,
        "dataInclusao": "2024-01-02T00:01:21",
        "dataPublicacaoPncp": "2024-01-02T00:01:21",
        "dataAberturaProposta": "2024-01-02T08:00:00",
        "dataEncerramentoProposta": "2024-01-25T08:00:00",
        "dataAtualizacao": data_atualizacao,
        "dataAtualizacaoGlobal": data_atualizacao,
        "orgaoEntidade": {
            "cnpj": "87613071000148",
            "razaoSocial": "MUNICIPIO DE SANTO ANGELO",
            "poderId": "N",
            "esferaId": "M",
        },
        "unidadeOrgao": {
            "codigoIbge": codigo_ibge,
            "municipioNome": "Santo Ângelo",
            "ufSigla": "RS",
            "ufNome": "Rio Grande do Sul",
            "codigoUnidade": "109819",
            "nomeUnidade": "PREFEITURA MUNICIPAL DE SANTO ÂNGELO",
        },
        "amparoLegal": {
            "codigo": 1,
            "nome": "Lei 14.133/2021, Art. 28, I",
        },
    }


def test_extract_record_maps_nested_fields_and_decimals() -> None:
    ingested_at = datetime(2026, 11, 23, 12, 0, tzinfo=UTC)
    row = extract_record(
        _sample_raw(valor_homologado=None, codigo_ibge=None),
        snapshot_run_id="run1",
        ingested_at=ingested_at,
        source_file="page_0001.json",
    )

    assert row["numero_controle_pncp"] == "87613071000148-1-000330/2023"
    assert row["orgao_cnpj"] == "87613071000148"
    assert row["codigo_ibge"] is None
    assert row["valor_total_estimado"] == Decimal("938036.0")
    assert row["valor_total_homologado"] is None
    assert row["data_atualizacao"] == datetime(2024, 5, 24, 12, 3, 45)


def test_extract_record_requires_primary_keys() -> None:
    bad = _sample_raw()
    del bad["numeroControlePNCP"]

    with pytest.raises(ValueError, match="chave obrigatória"):
        extract_record(
            bad,
            snapshot_run_id="run1",
            ingested_at=datetime.now(UTC),
            source_file="x.json",
        )


def test_deduplicate_contratacoes_keeps_latest_update_and_ingestion() -> None:
    t1 = datetime(2026, 11, 23, 10, 0, tzinfo=UTC)
    t2 = datetime(2026, 11, 23, 11, 0, tzinfo=UTC)
    older = extract_record(
        _sample_raw(data_atualizacao="2024-01-02T10:00:00", valor_homologado=None),
        snapshot_run_id="run1",
        ingested_at=t2,
        source_file="old.json",
    )
    newer = extract_record(
        _sample_raw(data_atualizacao="2024-05-24T12:03:45", valor_homologado=500.0),
        snapshot_run_id="run2",
        ingested_at=t1,
        source_file="new.json",
    )

    deduped = deduplicate_contratacoes([older, newer])

    assert len(deduped) == 1
    assert deduped[0]["snapshot_run_id"] == "run2"
    assert deduped[0]["valor_total_homologado"] == Decimal("500.0")


def test_read_bronze_records_reads_nested_json_files(tmp_path: Path) -> None:
    bronze_file = tmp_path / "source=pncp" / "page=0001.json"
    bronze_file.parent.mkdir(parents=True)
    bronze_file.write_text(
        json.dumps(
            {
                "metadata": {
                    "run_id": "abc123",
                    "ingested_at": "2026-11-23T12:33:03+00:00",
                },
                "payload": {"data": [_sample_raw()]},
            }
        ),
        encoding="utf-8",
    )

    files_read, records = read_bronze_records(tmp_path)

    assert files_read == 1
    assert len(records) == 1
    assert records[0]["snapshot_run_id"] == "abc123"


def test_schema_sql_contains_expected_tables() -> None:
    sql = SCHEMA_PATH.read_text(encoding="utf-8")
    for table in (
        "dim_orgao",
        "dim_municipio",
        "dim_modalidade",
        "fact_contratacao",
        "stg_contratacao_snapshot",
        "pipeline_runs",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in sql
