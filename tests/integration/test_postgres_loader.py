from __future__ import annotations

import json
import os
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from urllib.parse import quote
from uuid import uuid4

import psycopg
import pytest
from dotenv import load_dotenv

from pncp_pipeline.common.quality import run_quality_checks
from pncp_pipeline.config import Settings
from pncp_pipeline.database.loader import load_bronze_to_postgres
from pncp_pipeline.transformations.pandas_baseline import run_pandas_pipeline


def _db_url_with_schema(base_url: str, schema: str) -> str:
    sep = "&" if "?" in base_url else "?"
    opts = quote(f"-csearch_path={schema}")
    return f"{base_url}{sep}options={opts}"


def _sample_payload(
    numero_controle: str,
    *,
    data_atualizacao: str,
    valor_homologado: float | None,
    objeto_compra: str = "serviços de infraestrutura",
    processo: str = "10/2024",
    numero_compra: str = "0001",
    srp: bool = False,
    modo_disputa_id: int = 1,
    modo_disputa_nome: str = "Aberto",
    amparo_codigo: int = 1,
    amparo_nome: str = "Lei 14.133/2021",
    data_abertura: str = "2024-01-02T08:00:00",
    data_encerramento: str = "2024-01-10T08:00:00",
) -> dict:
    return {
        "numeroControlePNCP": numero_controle,
        "anoCompra": 2024,
        "sequencialCompra": 1,
        "numeroCompra": numero_compra,
        "processo": processo,
        "objetoCompra": objeto_compra,
        "srp": srp,
        "modalidadeId": 6,
        "modalidadeNome": "Pregão - Eletrônico",
        "modoDisputaId": modo_disputa_id,
        "modoDisputaNome": modo_disputa_nome,
        "situacaoCompraId": 1,
        "situacaoCompraNome": "Divulgada no PNCP",
        "valorTotalEstimado": 1000.0,
        "valorTotalHomologado": valor_homologado,
        "dataInclusao": "2024-01-02T00:01:21",
        "dataPublicacaoPncp": "2024-01-02T00:01:21",
        "dataAberturaProposta": data_abertura,
        "dataEncerramentoProposta": data_encerramento,
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
        "amparoLegal": {"codigo": amparo_codigo, "nome": amparo_nome},
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


def _is_ci() -> bool:
    return os.environ.get("CI", "").strip().lower() in {"1", "true", "yes"}


def test_postgres_loader_idempotency_monotonic_update_and_qa(tmp_path: Path) -> None:
    load_dotenv(override=False)
    base_url = os.environ.get("DATABASE_URL") or Settings.from_env().database_url
    try:
        admin_conn = psycopg.connect(base_url, connect_timeout=3)
    except psycopg.OperationalError as exc:
        if _is_ci():
            pytest.fail(f"PostgreSQL indisponível com CI=true: {exc}")
        pytest.skip(f"PostgreSQL indisponível para teste de integração: {exc}")

    schema = f"test_it_{uuid4().hex[:10]}"
    scoped_url = _db_url_with_schema(base_url, schema)
    raw_dir = tmp_path / "raw"
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"

    try:
        with psycopg.connect(base_url, autocommit=True) as conn:
            conn.execute(f"CREATE SCHEMA {schema}")

        # 0. Bronze vazio não falha
        empty = load_bronze_to_postgres(raw_dir, scoped_url, run_id="load0-empty")
        assert empty.files_read == 0
        assert empty.rows_in == 0

        # 1. Carga inicial
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
        assert first.files_read == 1
        assert first.rows_deduplicated == 1

        # 2. Retificação atualizando TODOS os campos mutáveis (somente novo arquivo lido)
        _write_bronze(
            raw_dir / "v2.json",
            "run2",
            "2026-11-24T11:00:00+00:00",
            [
                _sample_payload(
                    "PNCP-IT-1",
                    data_atualizacao="2024-05-24T12:00:00",
                    valor_homologado=850.0,
                    objeto_compra="serviços retificados v2",
                    processo="99/2024",
                    numero_compra="0009",
                    srp=True,
                    modo_disputa_id=3,
                    modo_disputa_nome="Aberto-Fechado",
                    amparo_codigo=2,
                    amparo_nome="Lei 14.133/2021, Art. 28, II",
                    data_abertura="2024-02-01T09:00:00",
                    data_encerramento="2024-02-15T18:00:00",
                )
            ],
        )
        second = load_bronze_to_postgres(raw_dir, scoped_url, run_id="load2")
        assert second.files_read == 1
        assert second.rows_deduplicated == 1

        # 3. Reexecução idempotente sem novos arquivos (0 arquivos lidos)
        noop = load_bronze_to_postgres(raw_dir, scoped_url, run_id="load-noop")
        assert noop.files_read == 0
        assert noop.rows_deduplicated == 0

        # 4. Backfill com versão antiga + empate com ingested_at menor: sem regressão
        _write_bronze(
            raw_dir / "v3_old_backfill.json",
            "run3-old",
            "2026-11-24T12:00:00+00:00",
            [
                _sample_payload(
                    "PNCP-IT-1",
                    data_atualizacao="2024-01-02T10:00:00",
                    valor_homologado=None,
                    objeto_compra="regressao antiga",
                )
            ],
        )
        _write_bronze(
            raw_dir / "v4_tie_older_ingest.json",
            "run4-tie",
            "2026-11-24T09:00:00+00:00",
            [
                _sample_payload(
                    "PNCP-IT-1",
                    data_atualizacao="2024-05-24T12:00:00",
                    valor_homologado=100.0,
                    objeto_compra="regressao empate",
                )
            ],
        )
        third = load_bronze_to_postgres(raw_dir, scoped_url, run_id="load3")
        assert third.files_read == 2

        with psycopg.connect(scoped_url) as conn:
            row = conn.execute(
                """
                SELECT valor_total_homologado, objeto_compra, processo, numero_compra,
                       srp, modo_disputa_id, modo_disputa_nome, amparo_legal_codigo,
                       amparo_legal_nome, data_abertura_proposta, data_encerramento_proposta
                FROM fact_contratacao
                WHERE numero_controle_pncp = 'PNCP-IT-1'
                """
            ).fetchone()
            assert row == (
                Decimal("850.00"),
                "serviços retificados v2",
                "99/2024",
                "0009",
                True,
                3,
                "Aberto-Fechado",
                2,
                "Lei 14.133/2021, Art. 28, II",
                datetime(2024, 2, 1, 9, 0, 0),
                datetime(2024, 2, 15, 18, 0, 0),
            )
            runs = conn.execute(
                "SELECT COUNT(*) FROM pipeline_runs WHERE status = 'success'"
            ).fetchone()
            assert runs == (5,)

        # 5. Reprocessamento explícito (full_refresh) mantém integridade
        refreshed = load_bronze_to_postgres(
            raw_dir, scoped_url, run_id="load-full", full_refresh=True
        )
        assert refreshed.files_read == 4

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
