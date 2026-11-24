from __future__ import annotations

import json
import os
from decimal import Decimal
from pathlib import Path
from urllib.parse import quote
from uuid import uuid4

import psycopg
import pytest

from pncp_pipeline.common.quality import run_quality_checks
from pncp_pipeline.database.loader import load_bronze_to_postgres
from pncp_pipeline.transformations.pandas_baseline import run_pandas_pipeline


def _db_url_with_schema(base_url: str, schema: str) -> str:
    sep = "&" if "?" in base_url else "?"
    opts = quote(f"-csearch_path={schema}")
    return f"{base_url}{sep}options={opts}"


def _sample_payload(
    numero_controle: str, *, data_atualizacao: str, valor_homologado: float | None
) -> dict:
    return {
        "numeroControlePNCP": numero_controle,
        "anoCompra": 2024,
        "sequencialCompra": 1,
        "numeroCompra": "0001",
        "processo": "10/2024",
        "objetoCompra": "serviços de infraestrutura",
        "srp": False,
        "modalidadeId": 6,
        "modalidadeNome": "Pregão - Eletrônico",
        "modoDisputaId": 1,
        "modoDisputaNome": "Aberto",
        "situacaoCompraId": 1,
        "situacaoCompraNome": "Divulgada no PNCP",
        "valorTotalEstimado": 1000.0,
        "valorTotalHomologado": valor_homologado,
        "dataInclusao": "2024-01-02T00:01:21",
        "dataPublicacaoPncp": "2024-01-02T00:01:21",
        "dataAberturaProposta": "2024-01-02T08:00:00",
        "dataEncerramentoProposta": "2024-01-10T08:00:00",
        "dataAtualizacao": data_atualizacao,
        "dataAtualizacaoGlobal": data_atualizacao,
        "orgaoEntidade": {
            "cnpj": "87613071000148",
            "razaoSocial": "MUNICIPIO DE TESTE",
            "poderId": "N",
            "esferaId": "M",
        },
        "unidadeOrgao": {
            "codigoIbge": "3550308",
            "municipioNome": "São Paulo",
            "ufSigla": "SP",
            "ufNome": "São Paulo",
            "codigoUnidade": "1",
            "nomeUnidade": "UNIDADE 1",
        },
        "amparoLegal": {"codigo": 1, "nome": "Lei 14.133/2021"},
    }


def _write_bronze(path: Path, run_id: str, ingested_at: str, items: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "metadata": {"run_id": run_id, "ingested_at": ingested_at},
                "payload": {"data": items},
            }
        ),
        encoding="utf-8",
    )


def test_postgres_loader_idempotency_monotonic_update_and_qa(tmp_path: Path) -> None:
    base_url = os.environ.get("DATABASE_URL", "postgresql://pncp:pncp@localhost:5432/pncp")
    try:
        admin_conn = psycopg.connect(base_url, connect_timeout=3)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL indisponível para teste de integração: {exc}")

    schema = f"test_it_{uuid4().hex[:10]}"
    scoped_url = _db_url_with_schema(base_url, schema)
    raw_dir = tmp_path / "raw"
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"

    try:
        with psycopg.connect(base_url, autocommit=True) as conn:
            conn.execute(f"CREATE SCHEMA {schema}")

        _write_bronze(
            raw_dir / "v1.json",
            "run1",
            "2026-11-24T10:00:00+00:00",
            [
                _sample_payload(
                    "PNCP-IT-1", data_atualizacao="2024-01-02T10:00:00", valor_homologado=None
                )
            ],
        )
        first = load_bronze_to_postgres(raw_dir, scoped_url, run_id="load1")
        assert first.rows_deduplicated == 1

        _write_bronze(
            raw_dir / "v2.json",
            "run2",
            "2026-11-24T11:00:00+00:00",
            [
                _sample_payload(
                    "PNCP-IT-1", data_atualizacao="2024-05-24T12:00:00", valor_homologado=850.0
                )
            ],
        )
        second = load_bronze_to_postgres(raw_dir, scoped_url, run_id="load2")
        assert second.rows_deduplicated == 1

        # Simula reexecução tardia de um arquivo antigo sem regressão no banco
        (raw_dir / "v2.json").unlink()
        third = load_bronze_to_postgres(raw_dir, scoped_url, run_id="load3")
        assert third.rows_deduplicated == 1

        with psycopg.connect(scoped_url) as conn:
            row = conn.execute(
                "SELECT valor_total_homologado FROM fact_contratacao "
                "WHERE numero_controle_pncp = 'PNCP-IT-1'"
            ).fetchone()
            assert row == (Decimal("850.00"),)
            runs = conn.execute(
                "SELECT COUNT(*) FROM pipeline_runs WHERE status = 'success'"
            ).fetchone()
            assert runs == (3,)

        run_pandas_pipeline(raw_dir, silver_dir, gold_dir)
        report = run_quality_checks(silver_dir, scoped_url, gold_dir=gold_dir)
        assert report.silver_rows == 1
        assert report.gold_rows == 1
        assert report.fact_rows == 1
        assert report.duplicate_keys == 0
        assert report.null_required_fields == 0
        assert report.negative_values == 0
    finally:
        admin_conn.close()
        with psycopg.connect(base_url, autocommit=True) as conn:
            conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
