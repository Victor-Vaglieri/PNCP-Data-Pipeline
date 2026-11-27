from __future__ import annotations

import copy
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from pncp_pipeline.database import loader as loader_mod
from pncp_pipeline.database.loader import (
    CHECKPOINT_RUN_ID,
    CHECKPOINT_STAGE,
    FACT_UPDATABLE_COLUMNS,
    SCHEMA_PATH,
    deduplicate_contratacoes,
    extract_record,
    load_bronze_to_postgres,
    load_ibge_to_postgres,
    read_bronze_records,
)
from pncp_pipeline.ingestion import bronze_checkpoint
from pncp_pipeline.ingestion.bronze_checkpoint import BronzeCheckpointError


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


def test_deduplicate_contratacoes_breaks_ties_by_global_update_and_ingestion() -> None:
    t1 = datetime(2026, 11, 23, 10, 0, tzinfo=UTC)
    t2 = datetime(2026, 11, 23, 11, 0, tzinfo=UTC)

    base = _sample_raw(data_atualizacao="2024-05-24T12:00:00", valor_homologado=100.0)
    base["dataAtualizacaoGlobal"] = "2024-05-24T12:00:00"
    rectified = _sample_raw(data_atualizacao="2024-05-24T12:00:00", valor_homologado=200.0)
    rectified["dataAtualizacaoGlobal"] = "2024-05-25T09:00:00"
    rectified["objetoCompra"] = "objeto retificado"

    older_global = extract_record(
        base, snapshot_run_id="r1", ingested_at=t2, source_file="a.json"
    )
    newer_global = extract_record(
        rectified, snapshot_run_id="r2", ingested_at=t1, source_file="b.json"
    )

    deduped = deduplicate_contratacoes([older_global, newer_global])

    assert len(deduped) == 1
    assert deduped[0]["snapshot_run_id"] == "r2"
    assert deduped[0]["objeto_compra"] == "objeto retificado"


class _FakeFactDB:
    """Simula Postgres com guarda monotônica, advisory lock, rollback transacional e checkpoint."""

    def __init__(
        self,
        *,
        fail_on_fact_upsert: bool = False,
        fail_on_snapshot_insert: bool = False,
        fail_on_checkpoint_save: bool = False,
        fail_on_success_run_insert: bool = False,
    ) -> None:
        self.facts: dict[str, dict] = {}
        self.snapshots: set[tuple[str, str]] = set()
        self.pipeline_runs: dict[tuple[str, str], dict] = {}
        self.ibge_rows: dict[str, dict] = {}
        self.advisory_locks: list[str] = []
        self.events: list[str] = []
        self.last_fact_sql: str = ""
        self.fail_on_fact_upsert = fail_on_fact_upsert
        self.fail_on_snapshot_insert = fail_on_snapshot_insert
        self.fail_on_checkpoint_save = fail_on_checkpoint_save
        self.fail_on_success_run_insert = fail_on_success_run_insert
        self._tx_snapshot: tuple[dict, set, dict, dict] | None = None

    def ensure_tx(self) -> None:
        """Simula início implícito de transação do psycopg após commit/rollback."""
        if self._tx_snapshot is None:
            self._tx_snapshot = (
                copy.deepcopy(self.facts),
                set(self.snapshots),
                copy.deepcopy(self.pipeline_runs),
                copy.deepcopy(self.ibge_rows),
            )

    def commit(self) -> None:
        self.events.append("commit")
        self._tx_snapshot = None

    def rollback(self) -> None:
        self.events.append("rollback")
        if self._tx_snapshot is not None:
            self.facts, self.snapshots, self.pipeline_runs, self.ibge_rows = self._tx_snapshot
            self._tx_snapshot = None


class _FakePgCursor:
    def __init__(self, db: _FakeFactDB) -> None:
        self._db = db
        self._result: tuple | None = None

    def __enter__(self) -> _FakePgCursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: tuple | None = None) -> _FakePgCursor:
        self._db.ensure_tx()
        normalized = " ".join(sql.split())
        if "pg_advisory_xact_lock" in normalized:
            self._db.advisory_locks.append(normalized)
            if "pncp_schema_init" in normalized:
                self._db.events.append("lock:schema")
            elif "pncp_bronze_to_postgres" in normalized:
                self._db.events.append("lock:load")
            self._result = (None,)
        elif "SELECT details FROM pipeline_runs" in normalized:
            assert params is not None
            row = self._db.pipeline_runs.get((params[0], params[1]))
            self._result = (copy.deepcopy(row["details"]),) if row else None
        elif "INSERT INTO pipeline_runs" in normalized:
            assert params is not None
            run_id, stage, status = params[0], params[1], params[2]
            if stage == CHECKPOINT_STAGE and self._db.fail_on_checkpoint_save:
                raise RuntimeError("simulated db failure during checkpoint save")
            if (
                stage == "bronze_to_postgres"
                and status == "success"
                and self._db.fail_on_success_run_insert
            ):
                raise RuntimeError("simulated db failure during final pipeline_runs insert")
            details_obj = params[-1]
            details = copy.deepcopy(getattr(details_obj, "obj", details_obj))
            self._db.pipeline_runs[(run_id, stage)] = {
                "run_id": run_id,
                "stage": stage,
                "status": status,
                "rows_in": params[5],
                "rows_out": params[6],
                "details": details,
            }
            self._result = None
        return self

    def fetchone(self) -> tuple | None:
        return self._result

    def executemany(self, sql: str, seq: list) -> None:
        self._db.ensure_tx()
        normalized = " ".join(sql.split())
        if "INSERT INTO fact_contratacao" in normalized:
            self._db.last_fact_sql = normalized
            if self._db.fail_on_fact_upsert:
                raise RuntimeError("simulated db failure during fact upsert")
            for row in seq:
                key = row["numero_controle_pncp"]
                existing = self._db.facts.get(key)
                if existing is None or _should_update_monotonic(existing, row):
                    self._db.facts[key] = dict(row)
        elif "INSERT INTO stg_contratacao_snapshot" in normalized:
            if self._db.fail_on_snapshot_insert:
                raise RuntimeError("simulated db failure during snapshot insert")
            for row in seq:
                self._db.snapshots.add((row["numero_controle_pncp"], row["snapshot_run_id"]))
        elif "INSERT INTO dim_municipio_populacao" in normalized:
            for row in seq:
                self._db.ibge_rows[row["codigo_ibge"]] = dict(row)


def _should_update_monotonic(existing: dict, incoming: dict) -> bool:
    ex_upd = existing["data_atualizacao"]
    in_upd = incoming["data_atualizacao"]
    if in_upd is not None and (ex_upd is None or in_upd > ex_upd):
        return True
    if in_upd == ex_upd:
        ex_glob = existing["data_atualizacao_global"]
        in_glob = incoming["data_atualizacao_global"]
        if in_glob is not None and (ex_glob is None or in_glob > ex_glob):
            return True
        if in_glob == ex_glob and incoming["ingested_at"] >= existing["ingested_at"]:
            return True
    return False


class _FakePgConn:
    def __init__(self, db: _FakeFactDB) -> None:
        self._db = db

    def __enter__(self) -> _FakePgConn:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str) -> None:
        self._db.ensure_tx()
        if "CREATE TABLE" in sql:
            self._db.events.append("apply_schema")

    def cursor(self) -> _FakePgCursor:
        return _FakePgCursor(self._db)

    def commit(self) -> None:
        self._db.commit()

    def rollback(self) -> None:
        self._db.rollback()


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


def test_fact_upsert_sql_updates_all_mutable_columns_with_monotonic_guard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = _FakeFactDB()
    monkeypatch.setattr("pncp_pipeline.database.loader.psycopg.connect", lambda _u: _FakePgConn(db))

    _write_bronze(
        tmp_path / "source=pncp" / "p1.json",
        "r1",
        "2026-11-24T10:00:00+00:00",
        [_sample_raw()],
    )
    load_bronze_to_postgres(tmp_path, "postgresql://fake", run_id="load-sql-check")

    sql = db.last_fact_sql
    expected_mutable = {
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
    }
    assert set(FACT_UPDATABLE_COLUMNS) == expected_mutable
    for col in expected_mutable:
        assert f"{col} = EXCLUDED.{col}" in sql
    assert "EXCLUDED.data_atualizacao > fact_contratacao.data_atualizacao" in sql
    assert "IS NOT DISTINCT FROM" in sql
    assert "EXCLUDED.ingested_at >= fact_contratacao.ingested_at" in sql


def test_load_bronze_to_postgres_incremental_idempotent_backfill_and_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = _FakeFactDB()
    monkeypatch.setattr("pncp_pipeline.database.loader.psycopg.connect", lambda _u: _FakePgConn(db))

    # Empty Bronze directory should succeed without failing
    empty_summary = load_bronze_to_postgres(tmp_path, "postgresql://fake", run_id="empty-0")
    assert empty_summary.files_read == 0
    assert empty_summary.rows_in == 0
    assert empty_summary.rows_deduplicated == 0
    assert db.advisory_locks

    # Run 1: initial file
    f1 = tmp_path / "source=pncp" / "start=20240102_end=20240102" / "v1.json"
    _write_bronze(
        f1,
        "run1",
        "2026-11-24T10:00:00+00:00",
        [_sample_raw(data_atualizacao="2024-01-02T10:00:00", valor_homologado=None)],
    )
    s1 = load_bronze_to_postgres(tmp_path, "postgresql://fake", run_id="load-1")
    assert s1.files_read == 1
    assert s1.rows_deduplicated == 1

    # Run 2: idempotent no-op when no new files arrived
    s2 = load_bronze_to_postgres(tmp_path, "postgresql://fake", run_id="load-2")
    assert s2.files_read == 0
    assert s2.rows_in == 0

    # Run 3: simulated failure does NOT advance checkpoint; retry recovers cleanly
    f2 = tmp_path / "source=pncp" / "start=20240524_end=20240524" / "v2.json"
    rectified = _sample_raw(data_atualizacao="2024-05-24T12:00:00", valor_homologado=850.0)
    rectified["objetoCompra"] = "objeto retificado v2"
    rectified["processo"] = "999/2024"
    _write_bronze(f2, "run2", "2026-11-24T11:00:00+00:00", [rectified])

    db.fail_on_fact_upsert = True
    with pytest.raises(RuntimeError, match="simulated db failure"):
        load_bronze_to_postgres(tmp_path, "postgresql://fake", run_id="load-3-fail")
    assert db.pipeline_runs[("load-3-fail", "bronze_to_postgres")]["status"] == "failed"

    db.fail_on_fact_upsert = False
    s3 = load_bronze_to_postgres(tmp_path, "postgresql://fake", run_id="load-3-ok")
    assert s3.files_read == 1
    assert db.facts["87613071000148-1-000330/2023"]["objeto_compra"] == "objeto retificado v2"
    assert db.facts["87613071000148-1-000330/2023"]["processo"] == "999/2024"

    # Run 4: older backfill file for earlier date is processed but does NOT regress v2
    f_bf = tmp_path / "source=pncp" / "start=20231201_end=20231201" / "bf.json"
    _write_bronze(
        f_bf,
        "run-bf",
        "2026-11-24T12:00:00+00:00",
        [
            _sample_raw(data_atualizacao="2024-01-01T08:00:00", valor_homologado=111.0),
            _sample_raw(
                numero_controle="87613071000148-1-000999/2023",
                data_atualizacao="2023-12-01T08:00:00",
                valor_homologado=300.0,
            ),
        ],
    )
    s4 = load_bronze_to_postgres(tmp_path, "postgresql://fake", run_id="load-4-bf")
    assert s4.files_read == 1
    assert db.facts["87613071000148-1-000330/2023"]["objeto_compra"] == "objeto retificado v2"
    assert db.facts["87613071000148-1-000330/2023"]["valor_total_homologado"] == Decimal("850.0")
    assert "87613071000148-1-000999/2023" in db.facts

    # Run 5: equal data_atualizacao with OLDER ingested_at and NULL data_atualizacao
    #        must NOT regress v2
    f_stale_tie = tmp_path / "source=pncp" / "start=20240524_end=20240524" / "v2_stale_tie.json"
    stale_tie = _sample_raw(data_atualizacao="2024-05-24T12:00:00", valor_homologado=1.0)
    stale_tie["objetoCompra"] = "nao pode regredir"
    _write_bronze(f_stale_tie, "run-stale-tie", "2026-11-24T09:00:00+00:00", [stale_tie])

    f_null_ts = tmp_path / "source=pncp" / "start=20240524_end=20240524" / "v2_null_ts.json"
    null_ts = _sample_raw(data_atualizacao="2024-05-24T12:00:00", valor_homologado=2.0)
    null_ts["dataAtualizacao"] = None
    null_ts["objetoCompra"] = "null nao pode regredir"
    _write_bronze(f_null_ts, "run-null-ts", "2026-11-24T15:00:00+00:00", [null_ts])

    s_stale = load_bronze_to_postgres(tmp_path, "postgresql://fake", run_id="load-stale")
    assert s_stale.files_read == 2
    assert db.facts["87613071000148-1-000330/2023"]["objeto_compra"] == "objeto retificado v2"
    assert db.facts["87613071000148-1-000330/2023"]["valor_total_homologado"] == Decimal("850.0")

    # Run 6: full_refresh reprocesses all 5 files without losing newer state
    s5 = load_bronze_to_postgres(
        tmp_path, "postgresql://fake", run_id="load-5-full", full_refresh=True
    )
    assert s5.files_read == 5
    assert db.facts["87613071000148-1-000330/2023"]["objeto_compra"] == "objeto retificado v2"


def test_fake_db_rollback_reverts_partial_facts_snapshots_and_checkpoint_on_mid_tx_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = _FakeFactDB()
    monkeypatch.setattr("pncp_pipeline.database.loader.psycopg.connect", lambda _u: _FakePgConn(db))

    f1 = tmp_path / "source=pncp" / "v1.json"
    _write_bronze(
        f1,
        "run1",
        "2026-11-24T10:00:00+00:00",
        [_sample_raw(data_atualizacao="2024-01-02T10:00:00", valor_homologado=100.0)],
    )
    load_bronze_to_postgres(tmp_path, "postgresql://fake", run_id="load-1")
    assert db.facts["87613071000148-1-000330/2023"]["valor_total_homologado"] == Decimal("100.0")
    ckpt_after_1 = copy.deepcopy(db.pipeline_runs[(CHECKPOINT_RUN_ID, CHECKPOINT_STAGE)]["details"])
    assert set(ckpt_after_1["processed_files"]) == {"source=pncp/v1.json"}

    # Failure AFTER fact_contratacao, stg_contratacao_snapshot AND checkpoint have been mutated
    f2 = tmp_path / "source=pncp" / "v2.json"
    rectified = _sample_raw(data_atualizacao="2024-05-24T12:00:00", valor_homologado=900.0)
    rectified["objetoCompra"] = "mutacao parcial que deve sofrer rollback"
    _write_bronze(f2, "run2", "2026-11-24T11:00:00+00:00", [rectified])

    db.fail_on_success_run_insert = True
    with pytest.raises(RuntimeError, match="final pipeline_runs insert"):
        load_bronze_to_postgres(tmp_path, "postgresql://fake", run_id="load-2-fail")

    # Partial mutations to facts, snapshots, and checkpoint MUST all be rolled back
    fact = db.facts["87613071000148-1-000330/2023"]
    assert fact["valor_total_homologado"] == Decimal("100.0")
    assert fact["objeto_compra"] == "registro de preços de fraldas"
    assert ("87613071000148-1-000330/2023", "run2") not in db.snapshots
    assert (
        db.pipeline_runs[(CHECKPOINT_RUN_ID, CHECKPOINT_STAGE)]["details"] == ckpt_after_1
    )
    assert db.pipeline_runs[("load-2-fail", "bronze_to_postgres")]["status"] == "failed"

    # Also validate rollback on snapshot failure (after fact upsert already ran)
    db.fail_on_success_run_insert = False
    db.fail_on_snapshot_insert = True
    with pytest.raises(RuntimeError, match="snapshot insert"):
        load_bronze_to_postgres(tmp_path, "postgresql://fake", run_id="load-2-snap-fail")
    assert db.facts["87613071000148-1-000330/2023"]["valor_total_homologado"] == Decimal("100.0")

    # Retry succeeds and applies v2 cleanly
    db.fail_on_snapshot_insert = False
    retry = load_bronze_to_postgres(tmp_path, "postgresql://fake", run_id="load-2-ok")
    assert retry.files_read == 1
    assert db.facts["87613071000148-1-000330/2023"]["valor_total_homologado"] == Decimal("900.0")
    assert ("87613071000148-1-000330/2023", "run2") in db.snapshots
    assert set(
        db.pipeline_runs[(CHECKPOINT_RUN_ID, CHECKPOINT_STAGE)]["details"]["processed_files"]
    ) == {"source=pncp/v1.json", "source=pncp/v2.json"}


def test_loader_locks_apply_schema_and_hashes_files_outside_db_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = _FakeFactDB()
    monkeypatch.setattr("pncp_pipeline.database.loader.psycopg.connect", lambda _u: _FakePgConn(db))

    _write_bronze(
        tmp_path / "source=pncp" / "p1.json",
        "r1",
        "2026-11-24T10:00:00+00:00",
        [_sample_raw()],
    )

    real_fp = bronze_checkpoint.file_fingerprint

    def traced_fp(raw_dir: Path, path: Path):
        db.events.append(f"hash:{path.name}")
        return real_fp(raw_dir, path)

    monkeypatch.setattr(bronze_checkpoint, "file_fingerprint", traced_fp)

    load_bronze_to_postgres(tmp_path, "postgresql://fake", run_id="order-check")

    # 1. File hashing happens BEFORE acquiring the schema/load DB locks (zero hashing inside lock!)
    assert db.events.index("hash:p1.json") < db.events.index("lock:schema")
    assert db.events.count("hash:p1.json") == 1
    # 2. apply_schema runs strictly AFTER schema advisory lock and commits before load lock
    lock_schema_idx = db.events.index("lock:schema")
    apply_idx = db.events.index("apply_schema")
    commit_after_schema_idx = db.events.index("commit")
    lock_load_idx = db.events.index("lock:load")
    assert lock_schema_idx < apply_idx < commit_after_schema_idx < lock_load_idx

    # 3. load_ibge_to_postgres also protects apply_schema under schema advisory lock
    db.events.clear()
    monkeypatch.setattr(
        "pncp_pipeline.ingestion.ibge_client.read_latest_ibge_population",
        lambda _raw: [
            {"codigo_ibge": "3550308", "municipio_nome": "São Paulo", "ano": 2026, "populacao": 10}
        ],
    )
    assert load_ibge_to_postgres(tmp_path, "postgresql://fake") == 1
    assert db.events.index("lock:schema") < db.events.index("apply_schema")


def test_loader_corrupted_db_checkpoint_and_full_refresh_empty_reset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = _FakeFactDB()
    monkeypatch.setattr("pncp_pipeline.database.loader.psycopg.connect", lambda _u: _FakePgConn(db))

    # Populate checkpoint, then reset via PNCP_FULL_REFRESH=1 with empty Bronze dir
    f1 = tmp_path / "source=pncp" / "p1.json"
    _write_bronze(f1, "r1", "2026-11-24T10:00:00+00:00", [_sample_raw()])
    load_bronze_to_postgres(tmp_path, "postgresql://fake", run_id="r1")
    f1.unlink()

    monkeypatch.setenv("PNCP_FULL_REFRESH", "1")
    empty_refresh = load_bronze_to_postgres(tmp_path, "postgresql://fake", run_id="r-empty-refresh")
    assert empty_refresh.files_read == 0
    assert (
        db.pipeline_runs[(CHECKPOINT_RUN_ID, CHECKPOINT_STAGE)]["details"]["processed_files"]
        == {}
    )
    assert db.pipeline_runs[(CHECKPOINT_RUN_ID, CHECKPOINT_STAGE)]["rows_in"] == 0

    # Corrupted DB checkpoint raises BronzeCheckpointError and records failed run
    monkeypatch.delenv("PNCP_FULL_REFRESH", raising=False)
    db.pipeline_runs[(CHECKPOINT_RUN_ID, CHECKPOINT_STAGE)]["details"] = {"schema_version": 999}
    with pytest.raises(BronzeCheckpointError):
        load_bronze_to_postgres(tmp_path, "postgresql://fake", run_id="r-corrupt")
    assert db.pipeline_runs[("r-corrupt", "bronze_to_postgres")]["status"] == "failed"
    assert loader_mod.CHECKPOINT_RUN_ID == CHECKPOINT_RUN_ID
