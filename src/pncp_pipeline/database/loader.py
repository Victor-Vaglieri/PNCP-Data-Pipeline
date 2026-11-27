from __future__ import annotations

import json
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg
from psycopg.types.json import Jsonb

from pncp_pipeline.ingestion.bronze_checkpoint import (
    BronzeCheckpointState,
    compute_pending_files,
    discover_bronze_files,
    fingerprint_bronze_files,
    full_refresh_from_env,
)

SCHEMA_PATH = Path(__file__).with_name("schema.sql")
CHECKPOINT_RUN_ID = "__bronze_checkpoint__"
CHECKPOINT_STAGE = "bronze_to_postgres_checkpoint"
CHECKPOINT_CONSUMER = "bronze_to_postgres"

FACT_UPDATABLE_COLUMNS: tuple[str, ...] = (
    "orgao_cnpj",
    "codigo_ibge",
    "modalidade_id",
    "codigo_unidade",
    "nome_unidade",
    "uf_sigla",
    "ano_compra",
    "sequencial_compra",
    "numero_compra",
    "processo",
    "objeto_compra",
    "srp",
    "modo_disputa_id",
    "modo_disputa_nome",
    "situacao_compra_id",
    "situacao_compra_nome",
    "amparo_legal_codigo",
    "amparo_legal_nome",
    "valor_total_estimado",
    "valor_total_homologado",
    "data_inclusao",
    "data_publicacao_pncp",
    "data_abertura_proposta",
    "data_encerramento_proposta",
    "data_atualizacao",
    "data_atualizacao_global",
    "source_file",
    "ingested_at",
)

_FACT_UPDATE_SET_SQL = ",\n            ".join(
    f"{col} = EXCLUDED.{col}" for col in FACT_UPDATABLE_COLUMNS
)


@dataclass(frozen=True)
class LoadSummary:
    run_id: str
    files_read: int
    rows_in: int
    rows_deduplicated: int
    duration_s: float


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def _parse_decimal(value: float | int | str | None) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def extract_record(
    raw: dict[str, Any],
    *,
    snapshot_run_id: str,
    ingested_at: datetime,
    source_file: str,
) -> dict[str, Any]:
    """Achata um item do JSON Bronze do PNCP para o formato relacional."""
    orgao = raw.get("orgaoEntidade") or {}
    unidade = raw.get("unidadeOrgao") or {}
    amparo = raw.get("amparoLegal") or {}

    numero_controle = raw.get("numeroControlePNCP")
    orgao_cnpj = orgao.get("cnpj")
    modalidade_id = raw.get("modalidadeId")
    if not numero_controle or not orgao_cnpj or modalidade_id is None:
        raise ValueError(
            "Registro PNCP sem chave obrigatória (numeroControlePNCP/cnpj/modalidadeId)"
        )

    return {
        "numero_controle_pncp": str(numero_controle),
        "snapshot_run_id": snapshot_run_id,
        "orgao_cnpj": str(orgao_cnpj),
        "orgao_razao_social": str(orgao.get("razaoSocial") or ""),
        "orgao_poder_id": orgao.get("poderId"),
        "orgao_esfera_id": orgao.get("esferaId"),
        "codigo_ibge": unidade.get("codigoIbge"),
        "municipio_nome": unidade.get("municipioNome"),
        "uf_sigla": unidade.get("ufSigla"),
        "uf_nome": unidade.get("ufNome"),
        "codigo_unidade": unidade.get("codigoUnidade"),
        "nome_unidade": unidade.get("nomeUnidade"),
        "modalidade_id": int(modalidade_id),
        "modalidade_nome": str(raw.get("modalidadeNome") or ""),
        "ano_compra": int(raw["anoCompra"]),
        "sequencial_compra": int(raw["sequencialCompra"]),
        "numero_compra": raw.get("numeroCompra"),
        "processo": raw.get("processo"),
        "objeto_compra": raw.get("objetoCompra"),
        "srp": raw.get("srp"),
        "modo_disputa_id": raw.get("modoDisputaId"),
        "modo_disputa_nome": raw.get("modoDisputaNome"),
        "situacao_compra_id": raw.get("situacaoCompraId"),
        "situacao_compra_nome": raw.get("situacaoCompraNome"),
        "amparo_legal_codigo": amparo.get("codigo"),
        "amparo_legal_nome": amparo.get("nome"),
        "valor_total_estimado": _parse_decimal(raw.get("valorTotalEstimado")),
        "valor_total_homologado": _parse_decimal(raw.get("valorTotalHomologado")),
        "data_inclusao": _parse_ts(raw.get("dataInclusao")),
        "data_publicacao_pncp": _parse_ts(raw.get("dataPublicacaoPncp")),
        "data_abertura_proposta": _parse_ts(raw.get("dataAberturaProposta")),
        "data_encerramento_proposta": _parse_ts(raw.get("dataEncerramentoProposta")),
        "data_atualizacao": _parse_ts(raw.get("dataAtualizacao")),
        "data_atualizacao_global": _parse_ts(raw.get("dataAtualizacaoGlobal")),
        "source_file": source_file,
        "ingested_at": ingested_at,
    }


def _recency_key(record: dict[str, Any]) -> tuple[datetime, datetime, datetime]:
    atualizacao = record["data_atualizacao"] or datetime.min
    if atualizacao.tzinfo is not None:
        atualizacao = atualizacao.replace(tzinfo=None)
    atualizacao_global = record.get("data_atualizacao_global") or datetime.min
    if atualizacao_global.tzinfo is not None:
        atualizacao_global = atualizacao_global.replace(tzinfo=None)
    ingested = record["ingested_at"]
    if ingested.tzinfo is None:
        ingested = ingested.replace(tzinfo=UTC)
    return (atualizacao, atualizacao_global, ingested)


def deduplicate_contratacoes(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Mantém apenas a versão mais recente por numero_controle_pncp."""
    latest: dict[str, dict[str, Any]] = {}
    for record in records:
        key = record["numero_controle_pncp"]
        current = latest.get(key)
        if current is None or _recency_key(record) > _recency_key(current):
            latest[key] = record
    return list(latest.values())


def read_bronze_records(
    raw_dir: Path, *, files: Sequence[Path] | None = None
) -> tuple[int, list[dict[str, Any]]]:
    selected = list(files) if files is not None else discover_bronze_files(raw_dir)
    extracted: list[dict[str, Any]] = []
    for path in selected:
        content = json.loads(path.read_text(encoding="utf-8"))
        meta = content["metadata"]
        run_id = str(meta["run_id"])
        ingested_at = datetime.fromisoformat(meta["ingested_at"])
        rel_path = path.relative_to(raw_dir).as_posix()
        for item in content["payload"]["data"]:
            extracted.append(
                extract_record(
                    item,
                    snapshot_run_id=run_id,
                    ingested_at=ingested_at,
                    source_file=rel_path,
                )
            )
    return len(selected), extracted


def apply_schema(conn: psycopg.Connection[Any]) -> None:
    conn.execute(SCHEMA_PATH.read_text(encoding="utf-8"))


def _ensure_schema_locked(conn: psycopg.Connection[Any]) -> None:
    """Serializa a criação idempotente de DDL para evitar corrida em cold-start."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_advisory_xact_lock("
            "hashtext('pncp_schema_init:' || current_schema()))"
        )
    apply_schema(conn)
    conn.commit()


def _upsert_dimensions(
    cur: psycopg.Cursor[Any], deduped: list[dict[str, Any]]
) -> None:
    orgaos = {
        r["orgao_cnpj"]: (
            r["orgao_cnpj"],
            r["orgao_razao_social"],
            r["orgao_poder_id"],
            r["orgao_esfera_id"],
        )
        for r in deduped
    }
    municipios = {
        r["codigo_ibge"]: (
            r["codigo_ibge"],
            r["municipio_nome"] or "",
            r["uf_sigla"] or "",
            r["uf_nome"] or "",
        )
        for r in deduped
        if r["codigo_ibge"]
    }
    modalidades = {
        r["modalidade_id"]: (r["modalidade_id"], r["modalidade_nome"]) for r in deduped
    }

    cur.executemany(
        """
        INSERT INTO dim_orgao (cnpj, razao_social, poder_id, esfera_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (cnpj) DO UPDATE SET
            razao_social = EXCLUDED.razao_social,
            poder_id = EXCLUDED.poder_id,
            esfera_id = EXCLUDED.esfera_id,
            updated_at = NOW()
        """,
        list(orgaos.values()),
    )
    if municipios:
        cur.executemany(
            """
            INSERT INTO dim_municipio (codigo_ibge, municipio_nome, uf_sigla, uf_nome)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (codigo_ibge) DO UPDATE SET
                municipio_nome = EXCLUDED.municipio_nome,
                uf_sigla = EXCLUDED.uf_sigla,
                uf_nome = EXCLUDED.uf_nome,
                updated_at = NOW()
            """,
            list(municipios.values()),
        )
    cur.executemany(
        """
        INSERT INTO dim_modalidade (modalidade_id, modalidade_nome)
        VALUES (%s, %s)
        ON CONFLICT (modalidade_id) DO UPDATE SET
            modalidade_nome = EXCLUDED.modalidade_nome,
            updated_at = NOW()
        """,
        list(modalidades.values()),
    )


def _upsert_facts_and_snapshots(
    cur: psycopg.Cursor[Any],
    deduped: list[dict[str, Any]],
    all_records: list[dict[str, Any]],
) -> None:
    cur.executemany(
        f"""
        INSERT INTO fact_contratacao (
            numero_controle_pncp, orgao_cnpj, codigo_ibge, modalidade_id,
            codigo_unidade, nome_unidade, uf_sigla, ano_compra, sequencial_compra,
            numero_compra, processo, objeto_compra, srp, modo_disputa_id,
            modo_disputa_nome, situacao_compra_id, situacao_compra_nome,
            amparo_legal_codigo, amparo_legal_nome, valor_total_estimado,
            valor_total_homologado, data_inclusao, data_publicacao_pncp,
            data_abertura_proposta, data_encerramento_proposta,
            data_atualizacao, data_atualizacao_global, source_file, ingested_at
        )
        VALUES (
            %(numero_controle_pncp)s, %(orgao_cnpj)s, %(codigo_ibge)s, %(modalidade_id)s,
            %(codigo_unidade)s, %(nome_unidade)s, %(uf_sigla)s, %(ano_compra)s,
            %(sequencial_compra)s, %(numero_compra)s, %(processo)s, %(objeto_compra)s,
            %(srp)s, %(modo_disputa_id)s, %(modo_disputa_nome)s, %(situacao_compra_id)s,
            %(situacao_compra_nome)s, %(amparo_legal_codigo)s, %(amparo_legal_nome)s,
            %(valor_total_estimado)s, %(valor_total_homologado)s, %(data_inclusao)s,
            %(data_publicacao_pncp)s, %(data_abertura_proposta)s,
            %(data_encerramento_proposta)s, %(data_atualizacao)s,
            %(data_atualizacao_global)s, %(source_file)s, %(ingested_at)s
        )
        ON CONFLICT (numero_controle_pncp) DO UPDATE SET
            {_FACT_UPDATE_SET_SQL},
            loaded_at = NOW()
        WHERE (
            EXCLUDED.data_atualizacao IS NOT NULL
            AND (
                fact_contratacao.data_atualizacao IS NULL
                OR EXCLUDED.data_atualizacao > fact_contratacao.data_atualizacao
            )
        )
        OR (
            EXCLUDED.data_atualizacao IS NOT DISTINCT FROM fact_contratacao.data_atualizacao
            AND (
                (
                    EXCLUDED.data_atualizacao_global IS NOT NULL
                    AND (
                        fact_contratacao.data_atualizacao_global IS NULL
                        OR EXCLUDED.data_atualizacao_global
                           > fact_contratacao.data_atualizacao_global
                    )
                )
                OR (
                    EXCLUDED.data_atualizacao_global
                    IS NOT DISTINCT FROM fact_contratacao.data_atualizacao_global
                    AND EXCLUDED.ingested_at >= fact_contratacao.ingested_at
                )
            )
        )
        """,
        deduped,
    )

    cur.executemany(
        """
        INSERT INTO stg_contratacao_snapshot (
            numero_controle_pncp, run_id, orgao_cnpj, codigo_ibge, uf_sigla,
            modalidade_id, valor_total_estimado, valor_total_homologado,
            data_publicacao_pncp, data_atualizacao, ingested_at, source_file
        )
        VALUES (
            %(numero_controle_pncp)s, %(snapshot_run_id)s, %(orgao_cnpj)s,
            %(codigo_ibge)s, %(uf_sigla)s, %(modalidade_id)s,
            %(valor_total_estimado)s, %(valor_total_homologado)s,
            %(data_publicacao_pncp)s, %(data_atualizacao)s,
            %(ingested_at)s, %(source_file)s
        )
        ON CONFLICT (numero_controle_pncp, run_id) DO NOTHING
        """,
        all_records,
    )


def _load_db_checkpoint(cur: psycopg.Cursor[Any]) -> BronzeCheckpointState:
    cur.execute(
        """
        SELECT details FROM pipeline_runs
        WHERE run_id = %s AND stage = %s
        """,
        (CHECKPOINT_RUN_ID, CHECKPOINT_STAGE),
    )
    row = cur.fetchone()
    if not row or not row[0]:
        return BronzeCheckpointState(consumer=CHECKPOINT_CONSUMER)
    return BronzeCheckpointState.from_dict(row[0], consumer=CHECKPOINT_CONSUMER)


def _save_db_checkpoint(
    cur: psycopg.Cursor[Any],
    state: BronzeCheckpointState,
    started_at: datetime,
) -> None:
    cur.execute(
        """
        INSERT INTO pipeline_runs (
            run_id, stage, status, started_at, finished_at,
            duration_s, rows_in, rows_out, details
        )
        VALUES (%s, %s, %s, %s, NOW(), %s, %s, %s, %s)
        ON CONFLICT (run_id, stage) DO UPDATE SET
            status = EXCLUDED.status,
            finished_at = EXCLUDED.finished_at,
            rows_in = EXCLUDED.rows_in,
            rows_out = EXCLUDED.rows_out,
            details = EXCLUDED.details
        """,
        (
            CHECKPOINT_RUN_ID,
            CHECKPOINT_STAGE,
            "checkpoint",
            started_at,
            0.0,
            len(state.processed_files),
            len(state.processed_files),
            Jsonb(state.to_dict()),
        ),
    )


def load_bronze_to_postgres(
    raw_dir: Path,
    database_url: str,
    *,
    run_id: str | None = None,
    incremental: bool = True,
    full_refresh: bool | None = None,
) -> LoadSummary:
    """Carrega JSONs Bronze para PostgreSQL com checkpoint transacional e guarda monotônica."""
    current_run_id = run_id or uuid4().hex[:12]
    started_at = datetime.now(UTC)
    t0 = time.monotonic()
    effective_full_refresh = full_refresh_from_env(full_refresh)

    files_read = 0
    all_records: list[dict[str, Any]] = []
    deduped: list[dict[str, Any]] = []

    # Calcula SHA-256 fora da transação SQL e do advisory lock
    precomputed = fingerprint_bronze_files(raw_dir)

    with psycopg.connect(database_url) as conn:
        _ensure_schema_locked(conn)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock("
                    "hashtext('pncp_bronze_to_postgres:' || current_schema()))"
                )
                discovered = discover_bronze_files(raw_dir)
                checkpoint_state = (
                    _load_db_checkpoint(cur)
                    if incremental
                    else BronzeCheckpointState(consumer=CHECKPOINT_CONSUMER)
                )
                pending = compute_pending_files(
                    raw_dir,
                    discovered,
                    checkpoint_state,
                    full_refresh=(effective_full_refresh or not incremental),
                    precomputed=precomputed,
                )
                files_read, all_records = read_bronze_records(
                    raw_dir, files=[fp.path for fp in pending]
                )
                deduped = deduplicate_contratacoes(all_records)

                if deduped:
                    _upsert_dimensions(cur, deduped)
                    _upsert_facts_and_snapshots(cur, deduped, all_records)

                if incremental and (pending or effective_full_refresh):
                    checkpoint_state.mark_processed(
                        pending,
                        run_id=current_run_id,
                        replace_all=effective_full_refresh,
                    )
                    _save_db_checkpoint(cur, checkpoint_state, started_at)

                duration_s = round(time.monotonic() - t0, 3)
                cur.execute(
                    """
                    INSERT INTO pipeline_runs (
                        run_id, stage, status, started_at, finished_at,
                        duration_s, rows_in, rows_out, details
                    )
                    VALUES (%s, %s, %s, %s, NOW(), %s, %s, %s, %s)
                    ON CONFLICT (run_id, stage) DO UPDATE SET
                        status = EXCLUDED.status,
                        finished_at = EXCLUDED.finished_at,
                        duration_s = EXCLUDED.duration_s,
                        rows_in = EXCLUDED.rows_in,
                        rows_out = EXCLUDED.rows_out,
                        details = EXCLUDED.details
                    """,
                    (
                        current_run_id,
                        "bronze_to_postgres",
                        "success",
                        started_at,
                        duration_s,
                        len(all_records),
                        len(deduped),
                        Jsonb(
                            {
                                "files_read": files_read,
                                "incremental": incremental,
                                "full_refresh": effective_full_refresh,
                            }
                        ),
                    ),
                )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            duration_s = round(time.monotonic() - t0, 3)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pipeline_runs (
                        run_id, stage, status, started_at, finished_at,
                        duration_s, rows_in, rows_out, details
                    )
                    VALUES (%s, %s, %s, %s, NOW(), %s, %s, %s, %s)
                    ON CONFLICT (run_id, stage) DO UPDATE SET
                        status = EXCLUDED.status,
                        finished_at = EXCLUDED.finished_at,
                        duration_s = EXCLUDED.duration_s,
                        details = EXCLUDED.details
                    """,
                    (
                        current_run_id,
                        "bronze_to_postgres",
                        "failed",
                        started_at,
                        duration_s,
                        len(all_records),
                        0,
                        Jsonb({"error": str(exc)}),
                    ),
                )
            conn.commit()
            raise

    return LoadSummary(
        run_id=current_run_id,
        files_read=files_read,
        rows_in=len(all_records),
        rows_deduplicated=len(deduped),
        duration_s=duration_s,
    )


def load_ibge_to_postgres(raw_dir: Path, database_url: str) -> int:
    """Carrega população municipal do IBGE (Bronze) para dim_municipio_populacao."""
    from pncp_pipeline.ingestion.ibge_client import read_latest_ibge_population

    rows = read_latest_ibge_population(raw_dir)
    if not rows:
        return 0

    with psycopg.connect(database_url) as conn:
        _ensure_schema_locked(conn)
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO dim_municipio_populacao (codigo_ibge, municipio_nome, ano, populacao)
                VALUES (%(codigo_ibge)s, %(municipio_nome)s, %(ano)s, %(populacao)s)
                ON CONFLICT (codigo_ibge) DO UPDATE SET
                    municipio_nome = EXCLUDED.municipio_nome,
                    ano = EXCLUDED.ano,
                    populacao = EXCLUDED.populacao,
                    updated_at = NOW()
                """,
                rows,
            )
        conn.commit()
    return len(rows)
