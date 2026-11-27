from __future__ import annotations

import importlib.util
import json
import os
import sys
import types
from pathlib import Path
from typing import Any

import pytest

from pncp_pipeline.ingestion import bronze_checkpoint
from pncp_pipeline.ingestion.bronze_checkpoint import load_checkpoint

SPARK_JOBS_FILE = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "pncp_pipeline"
    / "transformations"
    / "spark_jobs.py"
)


def _load_spark_jobs_module(monkeypatch: pytest.MonkeyPatch):
    """Carrega spark_jobs.py com stub mínimo de pyspark quando PySpark não está instalado."""
    if importlib.util.find_spec("pyspark") is None:
        pyspark_mod = types.ModuleType("pyspark")
        sql_mod = types.ModuleType("pyspark.sql")
        fn_mod = types.ModuleType("pyspark.sql.functions")
        types_mod = types.ModuleType("pyspark.sql.types")

        class _Dummy:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                pass

        sql_mod.DataFrame = _Dummy  # type: ignore[attr-defined]
        sql_mod.SparkSession = _Dummy  # type: ignore[attr-defined]
        sql_mod.Window = _Dummy  # type: ignore[attr-defined]
        sql_mod.functions = fn_mod  # type: ignore[attr-defined]
        for type_name in (
            "ArrayType",
            "BooleanType",
            "DoubleType",
            "IntegerType",
            "StringType",
            "StructField",
            "StructType",
        ):
            setattr(types_mod, type_name, _Dummy)
        pyspark_mod.sql = sql_mod  # type: ignore[attr-defined]

        monkeypatch.setitem(sys.modules, "pyspark", pyspark_mod)
        monkeypatch.setitem(sys.modules, "pyspark.sql", sql_mod)
        monkeypatch.setitem(sys.modules, "pyspark.sql.functions", fn_mod)
        monkeypatch.setitem(sys.modules, "pyspark.sql.types", types_mod)

    spec = importlib.util.spec_from_file_location("pncp_spark_jobs_under_test", SPARK_JOBS_FILE)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "pncp_spark_jobs_under_test", module)
    spec.loader.exec_module(module)
    return module


class _FakeSparkDF:
    """DataFrame leve em memória para validar orquestração, unpersist e merge de rejeitados."""

    def __init__(self, rows: list[dict[str, Any]], events: list[str] | None = None) -> None:
        self.rows = [dict(r) for r in rows]
        self.events = events if events is not None else []
        self.cached = False

    def count(self) -> int:
        return len(self.rows)

    def cache(self) -> _FakeSparkDF:
        self.cached = True
        self.events.append("cache")
        return self

    def unpersist(self) -> _FakeSparkDF:
        self.cached = False
        self.events.append("unpersist")
        return self

    def repartition(self, *_cols: str) -> _FakeSparkDF:
        return self

    def coalesce(self, _n: int) -> _FakeSparkDF:
        return self

    def select(self, col: str) -> _FakeSparkDF:
        return _FakeSparkDF([{col: r[col]} for r in self.rows], self.events)

    def distinct(self) -> _FakeSparkDF:
        seen: set[tuple[tuple[str, Any], ...]] = set()
        out: list[dict[str, Any]] = []
        for r in self.rows:
            key = tuple(sorted(r.items()))
            if key not in seen:
                seen.add(key)
                out.append(r)
        return _FakeSparkDF(out, self.events)

    def join(self, other: _FakeSparkDF, *, on: str, how: str = "inner") -> _FakeSparkDF:
        assert how == "left_anti"
        drop_vals = {r[on] for r in other.rows}
        kept = [r for r in self.rows if r[on] not in drop_vals]
        return _FakeSparkDF(kept, self.events)

    def unionByName(
        self, other: _FakeSparkDF, *, allowMissingColumns: bool = False
    ) -> _FakeSparkDF:
        return _FakeSparkDF(self.rows + other.rows, self.events)

    @property
    def write(self) -> _FakeWriter:
        return _FakeWriter(self.rows)


class _FakeWriter:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def mode(self, _mode: str) -> _FakeWriter:
        return self

    def partitionBy(self, *_cols: str) -> _FakeWriter:
        return self

    def parquet(self, path: str) -> None:
        out = Path(path)
        out.mkdir(parents=True, exist_ok=True)
        (out / "part-0000.parquet").write_text(json.dumps(self._rows), encoding="utf-8")


class _FakeSparkSession:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    @property
    def read(self) -> _FakeReader:
        return _FakeReader(self.events)


class _FakeReader:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def parquet(self, path: str) -> _FakeSparkDF:
        rows = json.loads((Path(path) / "part-0000.parquet").read_text(encoding="utf-8"))
        return _FakeSparkDF(rows, self.events)


def test_commit_dir_changes_rolls_back_all_swaps_and_removals_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mod = _load_spark_jobs_module(monkeypatch)

    target1 = tmp_path / "silver" / "contratacoes"
    target2 = tmp_path / "gold" / "resumo_uf_mes"
    target3 = tmp_path / "silver" / "rejected" / "contratacoes"
    for target, label in ((target1, "old1"), (target2, "old2"), (target3, "old3")):
        target.mkdir(parents=True)
        (target / "marker.txt").write_text(label, encoding="utf-8")

    staging1 = tmp_path / "silver" / ".tmp_1"
    staging2 = tmp_path / "gold" / ".tmp_2"
    staging1.mkdir(parents=True)
    staging2.mkdir(parents=True)
    (staging1 / "marker.txt").write_text("new1", encoding="utf-8")
    (staging2 / "marker.txt").write_text("new2", encoding="utf-8")

    real_replace = os.replace
    calls = 0

    def fail_on_second_target_promotion(src: Path, dst: Path) -> None:
        nonlocal calls
        calls += 1
        if Path(src) == staging2 and Path(dst) == target2:
            raise OSError("simulated swap failure on gold")
        real_replace(src, dst)

    monkeypatch.setattr(os, "replace", fail_on_second_target_promotion)

    with pytest.raises(OSError, match="simulated swap failure"):
        mod._commit_dir_changes([(staging1, target1), (staging2, target2)], removals=[target3])

    # Every pre-existing directory MUST be restored intact
    assert (target1 / "marker.txt").read_text(encoding="utf-8") == "old1"
    assert (target2 / "marker.txt").read_text(encoding="utf-8") == "old2"
    assert (target3 / "marker.txt").read_text(encoding="utf-8") == "old3"
    # No stray backup directories remain
    backups = list(tmp_path.rglob(".bak_*"))
    assert backups == []

    # Now allow swap + removal to succeed
    monkeypatch.setattr(os, "replace", real_replace)
    mod._commit_dir_changes([(staging1, target1), (staging2, target2)], removals=[target3])
    assert (target1 / "marker.txt").read_text(encoding="utf-8") == "new1"
    assert (target2 / "marker.txt").read_text(encoding="utf-8") == "new2"
    assert not target3.exists()
    assert list(tmp_path.rglob(".bak_*")) == []


def test_merge_rejected_spark_preserves_multiple_null_key_rows_and_replaces_reprocessed_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mod = _load_spark_jobs_module(monkeypatch)

    existing = _FakeSparkDF(
        [
            {
                "source_file": "file:///raw/p1.json",
                "numero_controle_pncp": None,
                "rejection_reason": "missing_numero_controle_pncp",
                "objeto_compra": "old-p1-row-1",
            },
            {
                "source_file": "file:///raw/p1.json",
                "numero_controle_pncp": None,
                "rejection_reason": "missing_numero_controle_pncp",
                "objeto_compra": "old-p1-row-2",
            },
        ]
    )
    new_batch = _FakeSparkDF(
        [
            {
                "source_file": "file:///raw/p2.json",
                "numero_controle_pncp": None,
                "rejection_reason": "missing_numero_controle_pncp",
                "objeto_compra": "p2-null-row-1",
            },
            {
                "source_file": "file:///raw/p2.json",
                "numero_controle_pncp": None,
                "rejection_reason": "missing_numero_controle_pncp",
                "objeto_compra": "p2-null-row-2",
            },
        ]
    )

    merged = mod._merge_rejected_spark(existing, new_batch)
    assert len(merged.rows) == 4
    assert [r["objeto_compra"] for r in merged.rows] == [
        "old-p1-row-1",
        "old-p1-row-2",
        "p2-null-row-1",
        "p2-null-row-2",
    ]

    # Reprocessing p1 replaces old p1 rows without dropping NULL rows from p2
    reprocessed_p1 = _FakeSparkDF(
        [
            {
                "source_file": "file:///raw/p1.json",
                "numero_controle_pncp": None,
                "rejection_reason": "missing_numero_controle_pncp",
                "objeto_compra": "new-p1-row-only",
            }
        ]
    )
    merged2 = mod._merge_rejected_spark(merged, reprocessed_p1)
    assert len(merged2.rows) == 3
    assert [r["objeto_compra"] for r in merged2.rows] == [
        "p2-null-row-1",
        "p2-null-row-2",
        "new-p1-row-only",
    ]


def test_spark_full_refresh_clears_outputs_and_resets_checkpoint_even_with_empty_bronze(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mod = _load_spark_jobs_module(monkeypatch)
    raw_dir = tmp_path / "raw"
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    raw_dir.mkdir()

    # Seed stale outputs + non-empty checkpoint
    stale_dirs = [
        silver_dir / "contratacoes",
        silver_dir / "rejected" / "contratacoes",
        gold_dir / "resumo_uf_mes",
        gold_dir / "ranking_orgaos",
    ]
    for d in stale_dirs:
        d.mkdir(parents=True)
        (d / "part-0000.parquet").write_text("[]", encoding="utf-8")

    ckpt_path = silver_dir / "_checkpoints" / "bronze_to_silver.json"
    ckpt_path.parent.mkdir(parents=True, exist_ok=True)
    ckpt_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "consumer": "spark_silver_gold",
                "updated_at": "2026-11-24T10:00:00+00:00",
                "processed_files": {"source=pncp/stale.json": {"sha256": "abc", "size": 10}},
            }
        ),
        encoding="utf-8",
    )

    # Incremental no-op must NOT touch stale dirs or checkpoint
    noop = mod.run_spark_pipeline(_FakeSparkSession([]), raw_dir, silver_dir, gold_dir)
    assert noop.files_read == 0
    assert all(d.exists() for d in stale_dirs)
    assert "source=pncp/stale.json" in load_checkpoint(
        ckpt_path, consumer="spark_silver_gold"
    ).processed_files

    # full_refresh=True with empty Bronze MUST clear all outputs AND reset checkpoint
    cleared = mod.run_spark_pipeline(
        _FakeSparkSession([]), raw_dir, silver_dir, gold_dir, full_refresh=True
    )
    assert cleared.files_read == 0
    assert all(not d.exists() for d in stale_dirs)
    assert load_checkpoint(ckpt_path, consumer="spark_silver_gold").processed_files == {}


def test_spark_pipeline_hashes_before_lock_unpersists_before_swap_and_recovers_on_swap_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mod = _load_spark_jobs_module(monkeypatch)
    raw_dir = tmp_path / "raw"
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    p1 = raw_dir / "source=pncp" / "p1.json"
    p1.parent.mkdir(parents=True)
    p1.write_text('{"ok": true}', encoding="utf-8")

    events: list[str] = []
    real_fp = bronze_checkpoint.file_fingerprint
    real_lock = mod.locked_bronze_checkpoint
    real_commit = mod._commit_dir_changes

    def traced_fp(r: Path, p: Path):
        events.append(f"hash:{p.name}")
        return real_fp(r, p)

    from contextlib import contextmanager

    @contextmanager
    def traced_lock(ckpt: Path, *, consumer: str, timeout_s: float = 60.0):
        events.append("enter_lock")
        with real_lock(ckpt, consumer=consumer, timeout_s=timeout_s) as sess:
            yield sess

    fail_once = {"fail": True}

    def traced_commit(replacements, removals=()):
        events.append("commit_dirs")
        if fail_once["fail"]:
            fail_once["fail"] = False
            raise OSError("simulated swap crash")
        real_commit(replacements, removals=removals)

    monkeypatch.setattr(bronze_checkpoint, "file_fingerprint", traced_fp)
    monkeypatch.setattr(mod, "locked_bronze_checkpoint", traced_lock)
    monkeypatch.setattr(mod, "_commit_dir_changes", traced_commit)
    monkeypatch.setattr(
        mod,
        "read_bronze_spark",
        lambda _s, _r, *, files=None: (len(files or []), _FakeSparkDF([{"id": 1}], events)),
    )
    monkeypatch.setattr(
        mod,
        "transform_silver_spark",
        lambda _df: (
            _FakeSparkDF([{"numero_controle_pncp": "ID-1"}], events),
            _FakeSparkDF([], events),
        ),
    )
    monkeypatch.setattr(
        mod, "build_gold_uf_mes_spark", lambda _df: _FakeSparkDF([{"uf": "SP"}], events)
    )
    monkeypatch.setattr(
        mod, "build_gold_ranking_orgaos_spark", lambda _df: _FakeSparkDF([{"cnpj": "1"}], events)
    )

    spark = _FakeSparkSession(events)
    ckpt_path = silver_dir / "_checkpoints" / "bronze_to_silver.json"

    # 1. Swap fails -> checkpoint NOT created, staging cleaned up, unpersist ran BEFORE commit_dirs
    with pytest.raises(OSError, match="simulated swap crash"):
        mod.run_spark_pipeline(spark, raw_dir, silver_dir, gold_dir)
    assert not ckpt_path.exists()
    assert events.index("hash:p1.json") < events.index("enter_lock")
    assert events.index("unpersist") < events.index("commit_dirs")
    assert list(silver_dir.rglob(".tmp_*")) == []

    # 2. Retry succeeds and commits checkpoint
    events.clear()
    summary = mod.run_spark_pipeline(spark, raw_dir, silver_dir, gold_dir)
    assert summary.files_read == 1
    assert summary.rows_valid_dedup == 1
    assert (silver_dir / "contratacoes" / "part-0000.parquet").exists()
    assert (gold_dir / "resumo_uf_mes" / "part-0000.parquet").exists()
    assert (gold_dir / "ranking_orgaos" / "part-0000.parquet").exists()
    assert set(load_checkpoint(ckpt_path, consumer="spark_silver_gold").processed_files) == {
        "source=pncp/p1.json"
    }
    assert events.index("hash:p1.json") < events.index("enter_lock")
    assert events.count("hash:p1.json") == 1
    assert events.index("unpersist") < events.index("commit_dirs")
