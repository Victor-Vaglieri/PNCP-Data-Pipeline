from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from pncp_pipeline.ingestion import bronze_checkpoint
from pncp_pipeline.ingestion.bronze_checkpoint import (
    BronzeCheckpointError,
    BronzeCheckpointState,
    BronzeFileFingerprint,
    compute_pending_files,
    discover_bronze_files,
    file_fingerprint,
    fingerprint_bronze_files,
    full_refresh_from_env,
    load_checkpoint,
    locked_bronze_checkpoint,
    save_checkpoint,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_discover_bronze_files_ignores_temp_and_hidden_files(tmp_path: Path) -> None:
    pncp = tmp_path / "source=pncp" / "endpoint=contratacoes_publicacao"
    valid = _write_json(pncp / "20261124T100000Z_r1_page=0001.json", {"ok": True})
    _write_json(pncp / ".tmp_partial.json", {"partial": True})
    _write_json(pncp / ".hidden.json", {"hidden": True})
    _write_json(pncp / ".checkpoints" / "state.json", {"state": True})

    found = discover_bronze_files(tmp_path)

    assert found == [valid]


def test_compute_pending_files_handles_legacy_idempotency_backfill_and_modified(
    tmp_path: Path,
) -> None:
    f_new = _write_json(
        tmp_path / "source=pncp" / "start=20240524_end=20240524" / "p1.json",
        {"v": 1},
    )
    # Legacy state (no checkpoint yet): all existing files are pending
    checkpoint = load_checkpoint(tmp_path / "_checkpoints" / "test.json", consumer="test")
    pending = compute_pending_files(tmp_path, [f_new], checkpoint)
    assert [p.path for p in pending] == [f_new]

    # Mark f_new processed and persist
    checkpoint.mark_processed(pending, run_id="run1")
    ckpt_path = tmp_path / "_checkpoints" / "test.json"
    save_checkpoint(ckpt_path, checkpoint)

    # Idempotent re-run: 0 pending
    reloaded = load_checkpoint(ckpt_path, consumer="test")
    assert compute_pending_files(tmp_path, [f_new], reloaded) == []

    # Backfill for an OLDER date partition MUST still be detected as pending
    f_backfill = _write_json(
        tmp_path / "source=pncp" / "start=20231201_end=20231201" / "p1.json",
        {"v": "backfill"},
    )
    pending_bf = compute_pending_files(tmp_path, [f_backfill, f_new], reloaded)
    assert [p.path for p in pending_bf] == [f_backfill]

    # Modified content for an already-processed file MUST be detected as pending
    _write_json(f_new, {"v": 2, "extra": "changed"})
    pending_mod = compute_pending_files(tmp_path, [f_new], reloaded)
    assert [p.path for p in pending_mod] == [f_new]

    # Full refresh returns all files regardless of checkpoint
    pending_full = compute_pending_files(tmp_path, [f_backfill, f_new], reloaded, full_refresh=True)
    assert [p.path for p in pending_full] == [f_backfill, f_new]


def test_save_checkpoint_is_atomic_on_replace_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ckpt_path = tmp_path / "_checkpoints" / "silver.json"
    f1 = _write_json(tmp_path / "source=pncp" / "f1.json", {"a": 1})
    state = load_checkpoint(ckpt_path, consumer="silver")
    state.mark_processed([file_fingerprint(tmp_path, f1)], run_id="r1")
    save_checkpoint(ckpt_path, state)
    original_text = ckpt_path.read_text(encoding="utf-8")

    def fail_replace(src: Path, dst: Path) -> None:
        raise OSError("simulated crash")

    monkeypatch.setattr(os, "replace", fail_replace)
    f2 = _write_json(tmp_path / "source=pncp" / "f2.json", {"a": 2})
    state.mark_processed([file_fingerprint(tmp_path, f2)], run_id="r2")

    with pytest.raises(OSError, match="simulated crash"):
        save_checkpoint(ckpt_path, state)

    assert ckpt_path.read_text(encoding="utf-8") == original_text
    leftovers = [p for p in ckpt_path.parent.iterdir() if p.name.startswith(".tmp_")]
    assert leftovers == []


def test_load_checkpoint_rejects_corrupted_json(tmp_path: Path) -> None:
    ckpt_path = tmp_path / "bad_checkpoint.json"
    ckpt_path.write_text("{not-valid-json", encoding="utf-8")

    with pytest.raises(BronzeCheckpointError, match="corrompido"):
        load_checkpoint(ckpt_path, consumer="silver")


def test_locked_bronze_checkpoint_serializes_concurrent_writers_without_data_loss(
    tmp_path: Path,
) -> None:
    ckpt_path = tmp_path / "_checkpoints" / "shared.json"
    files = [
        _write_json(tmp_path / "source=pncp" / f"file_{idx:02d}.json", {"idx": idx})
        for idx in range(6)
    ]
    errors: list[BaseException] = []
    overlap_detected = False
    active = 0
    guard = threading.Lock()

    def worker(subset: list[Path], run_id: str) -> None:
        nonlocal active, overlap_detected
        try:
            with locked_bronze_checkpoint(ckpt_path, consumer="concurrent-test") as session:
                with guard:
                    active += 1
                    if active > 1:
                        overlap_detected = True
                time.sleep(0.05)
                pending: list[BronzeFileFingerprint] = session.pending(tmp_path, subset)
                session.commit(pending, run_id=run_id)
                with guard:
                    active -= 1
        except BaseException as exc:  # pragma: no cover - surfaced by assert below
            errors.append(exc)

    threads = [
        threading.Thread(target=worker, args=(files[:3], "run-a")),
        threading.Thread(target=worker, args=(files[3:], "run-b")),
        threading.Thread(target=worker, args=(files, "run-c")),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert overlap_detected is False
    final_state = load_checkpoint(ckpt_path, consumer="concurrent-test")
    assert len(final_state.processed_files) == 6
    assert compute_pending_files(tmp_path, files, final_state) == []


def test_locked_bronze_checkpoint_recovers_after_uncommitted_failure(tmp_path: Path) -> None:
    ckpt_path = tmp_path / "_checkpoints" / "spark.json"
    f1 = _write_json(tmp_path / "source=pncp" / "p1.json", {"id": 1})

    with pytest.raises(RuntimeError, match="spark write failed"):
        with locked_bronze_checkpoint(ckpt_path, consumer="spark_silver_gold") as session:
            pending = session.pending(tmp_path)
            assert [p.path for p in pending] == [f1]
            raise RuntimeError("spark write failed")

    assert not ckpt_path.exists()

    with locked_bronze_checkpoint(ckpt_path, consumer="spark_silver_gold") as session:
        pending_retry = session.pending(tmp_path)
        assert [p.path for p in pending_retry] == [f1]
        session.commit(pending_retry, run_id="retry-ok")

    assert ckpt_path.exists()
    with locked_bronze_checkpoint(ckpt_path, consumer="spark_silver_gold") as session:
        assert session.pending(tmp_path) == []


def test_discover_bronze_files_ignores_ibge_source_even_without_pncp_dir(tmp_path: Path) -> None:
    _write_json(tmp_path / "source=ibge" / "populacao_period=-1.json", {"ibge": True})
    assert discover_bronze_files(tmp_path) == []

    flat = _write_json(tmp_path / "flat_fixture.json", {"ok": True})
    assert discover_bronze_files(tmp_path) == [flat]


def test_compute_pending_files_reuses_precomputed_hashes_and_rehashes_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    f1 = _write_json(tmp_path / "source=pncp" / "p1.json", {"v": 1})
    f2 = _write_json(tmp_path / "source=pncp" / "p2.json", {"v": 2})

    precomputed = fingerprint_bronze_files(tmp_path)
    state = BronzeCheckpointState(consumer="test")
    state.mark_processed([precomputed["source=pncp/p1.json"]], run_id="r1")

    hash_calls: list[Path] = []
    real_fp = bronze_checkpoint.file_fingerprint

    def counting_fp(raw_dir: Path, path: Path) -> BronzeFileFingerprint:
        hash_calls.append(path)
        return real_fp(raw_dir, path)

    monkeypatch.setattr(bronze_checkpoint, "file_fingerprint", counting_fp)

    # Unchanged files reuse precomputed hashes: ZERO file_fingerprint calls inside critical section
    pending = compute_pending_files(tmp_path, [f1, f2], state, precomputed=precomputed)
    assert [p.path for p in pending] == [f2]
    assert hash_calls == []

    # Newly added file or modified file triggers targeted re-hash only for that file
    f3 = _write_json(tmp_path / "source=pncp" / "p3.json", {"v": 3})
    _write_json(f1, {"v": 1, "modified": "bigger-payload"})
    pending_after = compute_pending_files(tmp_path, [f1, f2, f3], state, precomputed=precomputed)
    assert [p.path for p in pending_after] == [f1, f2, f3]
    assert set(hash_calls) == {f1, f3}


def test_from_dict_validates_consumer_and_processed_entries(tmp_path: Path) -> None:
    ckpt_path = tmp_path / "ckpt.json"
    _write_json(
        ckpt_path,
        {"schema_version": 1, "consumer": "other_consumer", "processed_files": {}},
    )
    with pytest.raises(BronzeCheckpointError, match="outro consumer"):
        load_checkpoint(ckpt_path, consumer="expected_consumer")

    _write_json(
        ckpt_path,
        {
            "schema_version": 1,
            "consumer": "expected_consumer",
            "processed_files": {"a.json": "not-a-dict"},
        },
    )
    with pytest.raises(BronzeCheckpointError, match="entrada inválida"):
        load_checkpoint(ckpt_path, consumer="expected_consumer")


def test_full_refresh_from_env_handles_variants_and_explicit_precedence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PNCP_FULL_REFRESH", "1")
    assert full_refresh_from_env(None) is True
    assert full_refresh_from_env(False) is False

    monkeypatch.setenv("PNCP_FULL_REFRESH", "false")
    assert full_refresh_from_env(None) is False
    assert full_refresh_from_env(True) is True


def test_locked_bronze_checkpoint_serializes_multiple_os_processes(tmp_path: Path) -> None:
    ckpt_path = tmp_path / "_checkpoints" / "mp.json"
    f1 = _write_json(tmp_path / "source=pncp" / "a.json", {"id": 1})
    f2 = _write_json(tmp_path / "source=pncp" / "b.json", {"id": 2})

    worker_code = (
        "import sys, time\n"
        "from pathlib import Path\n"
        "from pncp_pipeline.ingestion.bronze_checkpoint import locked_bronze_checkpoint\n"
        "raw, ckpt = Path(sys.argv[1]), Path(sys.argv[2])\n"
        "target, rid = Path(sys.argv[3]), sys.argv[4]\n"
        "with locked_bronze_checkpoint(ckpt, consumer='mp') as s:\n"
        "    t0 = time.monotonic()\n"
        "    time.sleep(0.15)\n"
        "    pending = s.pending(raw, [target])\n"
        "    s.commit(pending, run_id=rid)\n"
        "    t1 = time.monotonic()\n"
        "    print(f'{t0:.6f},{t1:.6f}')\n"
    )
    p1 = subprocess.Popen(
        [sys.executable, "-c", worker_code, str(tmp_path), str(ckpt_path), str(f1), "p1"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    p2 = subprocess.Popen(
        [sys.executable, "-c", worker_code, str(tmp_path), str(ckpt_path), str(f2), "p2"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    out1, err1 = p1.communicate(timeout=20)
    out2, err2 = p2.communicate(timeout=20)
    assert p1.returncode == 0, err1
    assert p2.returncode == 0, err2

    s1, e1 = (float(x) for x in out1.strip().split(","))
    s2, e2 = (float(x) for x in out2.strip().split(","))
    # Strict non-overlapping intervals prove OS-level serialization
    assert e1 <= s2 + 0.02 or e2 <= s1 + 0.02

    state = load_checkpoint(ckpt_path, consumer="mp")
    assert set(state.processed_files) == {"source=pncp/a.json", "source=pncp/b.json"}
