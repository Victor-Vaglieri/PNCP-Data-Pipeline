from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pncp_pipeline.ingestion.bronze_writer import write_json_atomic

CHECKPOINT_SCHEMA_VERSION = 1
_PROCESS_LOCKS: dict[Path, threading.Lock] = {}
_PROCESS_LOCKS_GUARD = threading.Lock()


class BronzeCheckpointError(RuntimeError):
    """Erro explícito quando o estado de checkpoint Bronze está inválido ou bloqueado."""


@dataclass(frozen=True)
class BronzeFileFingerprint:
    rel_path: str
    path: Path
    sha256: str
    size: int
    mtime_ns: int


@dataclass
class BronzeCheckpointState:
    consumer: str
    schema_version: int = CHECKPOINT_SCHEMA_VERSION
    updated_at: str | None = None
    processed_files: dict[str, dict[str, Any]] = field(default_factory=dict)

    def mark_processed(
        self,
        files: Sequence[BronzeFileFingerprint],
        *,
        run_id: str,
        processed_at: datetime | None = None,
        replace_all: bool = False,
    ) -> None:
        now_iso = (processed_at or datetime.now(UTC)).astimezone(UTC).isoformat()
        if replace_all:
            self.processed_files = {}
        for fp in files:
            self.processed_files[fp.rel_path] = {
                "sha256": fp.sha256,
                "size": fp.size,
                "mtime_ns": fp.mtime_ns,
                "run_id": run_id,
                "processed_at": now_iso,
            }
        self.updated_at = now_iso

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "consumer": self.consumer,
            "updated_at": self.updated_at,
            "processed_files": dict(sorted(self.processed_files.items())),
        }

    @classmethod
    def from_dict(cls, raw: Any, *, consumer: str) -> BronzeCheckpointState:
        if not isinstance(raw, dict):
            raise BronzeCheckpointError("Checkpoint Bronze corrompido: raiz não é objeto JSON")
        version = raw.get("schema_version")
        if version != CHECKPOINT_SCHEMA_VERSION:
            raise BronzeCheckpointError(
                f"Versão de checkpoint Bronze incompatível: {version!r}"
            )
        raw_consumer = raw.get("consumer")
        if raw_consumer is not None and str(raw_consumer) != consumer:
            raise BronzeCheckpointError(
                f"Checkpoint Bronze pertence a outro consumer: {raw_consumer!r} != {consumer!r}"
            )
        processed = raw.get("processed_files")
        if not isinstance(processed, dict):
            raise BronzeCheckpointError(
                "Checkpoint Bronze corrompido: 'processed_files' inválido"
            )
        validated_files: dict[str, dict[str, Any]] = {}
        for key, entry in processed.items():
            if not isinstance(key, str) or not isinstance(entry, dict):
                raise BronzeCheckpointError(
                    "Checkpoint Bronze corrompido: entrada inválida em 'processed_files'"
                )
            validated_files[key] = dict(entry)
        return cls(
            consumer=consumer,
            schema_version=CHECKPOINT_SCHEMA_VERSION,
            updated_at=raw.get("updated_at"),
            processed_files=validated_files,
        )


def full_refresh_from_env(explicit: bool | None) -> bool:
    if explicit is not None:
        return explicit
    return os.environ.get("PNCP_FULL_REFRESH", "").strip().lower() in {
        "1",
        "true",
        "t",
        "yes",
        "y",
        "on",
    }


def discover_bronze_files(raw_dir: Path) -> list[Path]:
    """Lista JSONs Bronze PNCP imutáveis ignorando arquivos temporários, ocultos e outras fontes."""
    if not raw_dir.exists():
        return []
    scan_dir = raw_dir / "source=pncp" if (raw_dir / "source=pncp").exists() else raw_dir
    discovered: list[Path] = []
    for path in sorted(scan_dir.rglob("*.json")):
        if not path.is_file():
            continue
        rel_parts = path.relative_to(raw_dir).parts
        if any(part.startswith(".") for part in rel_parts):
            continue
        if rel_parts and rel_parts[0].startswith("source=") and rel_parts[0] != "source=pncp":
            continue
        discovered.append(path)
    return discovered


def file_fingerprint(raw_dir: Path, path: Path) -> BronzeFileFingerprint:
    stat = path.stat()
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return BronzeFileFingerprint(
        rel_path=path.relative_to(raw_dir).as_posix(),
        path=path,
        sha256=digest.hexdigest(),
        size=stat.st_size,
        mtime_ns=stat.st_mtime_ns,
    )


def fingerprint_bronze_files(
    raw_dir: Path, files: Sequence[Path] | None = None
) -> dict[str, BronzeFileFingerprint]:
    """Calcula fingerprints SHA-256 dos arquivos Bronze fora de seções críticas/locks."""
    selected = list(files) if files is not None else discover_bronze_files(raw_dir)
    fingerprints: dict[str, BronzeFileFingerprint] = {}
    for path in selected:
        try:
            fp = file_fingerprint(raw_dir, path)
        except FileNotFoundError:
            continue
        fingerprints[fp.rel_path] = fp
    return fingerprints


def load_checkpoint(checkpoint_path: Path, *, consumer: str) -> BronzeCheckpointState:
    if not checkpoint_path.exists():
        return BronzeCheckpointState(consumer=consumer)
    try:
        raw = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise BronzeCheckpointError(
            f"Checkpoint Bronze corrompido em {checkpoint_path}: {exc}"
        ) from exc
    return BronzeCheckpointState.from_dict(raw, consumer=consumer)


def save_checkpoint(checkpoint_path: Path, state: BronzeCheckpointState) -> Path:
    return write_json_atomic(checkpoint_path, state.to_dict())


def compute_pending_files(
    raw_dir: Path,
    files: Sequence[Path],
    state: BronzeCheckpointState,
    *,
    full_refresh: bool = False,
    precomputed: Mapping[str, BronzeFileFingerprint] | None = None,
) -> list[BronzeFileFingerprint]:
    """Seleciona arquivos novos, backfills antigos ou modificados em relação ao checkpoint."""
    pending: list[BronzeFileFingerprint] = []
    for path in files:
        rel_path = path.relative_to(raw_dir).as_posix()
        cached = precomputed.get(rel_path) if precomputed is not None else None
        if cached is not None:
            stat = path.stat()
            if cached.size == stat.st_size and cached.mtime_ns == stat.st_mtime_ns:
                fp = cached
            else:
                fp = file_fingerprint(raw_dir, path)
        else:
            fp = file_fingerprint(raw_dir, path)
        if full_refresh:
            pending.append(fp)
            continue
        recorded = state.processed_files.get(fp.rel_path)
        if (
            recorded is None
            or recorded.get("sha256") != fp.sha256
            or recorded.get("size") != fp.size
        ):
            pending.append(fp)
    return pending


def _get_process_lock(lock_path: Path) -> threading.Lock:
    resolved = lock_path.resolve()
    with _PROCESS_LOCKS_GUARD:
        lock = _PROCESS_LOCKS.get(resolved)
        if lock is None:
            lock = threading.Lock()
            _PROCESS_LOCKS[resolved] = lock
        return lock


@contextmanager
def _exclusive_file_lock(lock_path: Path, *, timeout_s: float = 60.0) -> Iterator[None]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    proc_lock = _get_process_lock(lock_path)
    deadline = time.monotonic() + timeout_s

    acquired_proc = proc_lock.acquire(timeout=timeout_s)
    if not acquired_proc:
        raise BronzeCheckpointError(f"Timeout ao aguardar lock interno em {lock_path}")

    handle = None
    os_locked = False
    try:
        handle = lock_path.open("a+b")
        if os.name == "nt":
            import msvcrt

            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"0")
                handle.flush()
            while True:
                handle.seek(0)
                try:
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    os_locked = True
                    break
                except OSError as exc:
                    if time.monotonic() >= deadline:
                        raise BronzeCheckpointError(
                            f"Timeout ao aguardar lock exclusivo em {lock_path}"
                        ) from exc
                    time.sleep(0.02)
        else:
            import fcntl

            while True:
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    os_locked = True
                    break
                except BlockingIOError as exc:
                    if time.monotonic() >= deadline:
                        raise BronzeCheckpointError(
                            f"Timeout ao aguardar lock exclusivo em {lock_path}"
                        ) from exc
                    time.sleep(0.02)
        yield
    finally:
        if handle is not None:
            try:
                if os_locked:
                    if os.name == "nt":
                        import msvcrt

                        handle.seek(0)
                        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                    else:
                        import fcntl

                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()
        proc_lock.release()


class LockedCheckpointSession:
    """Sessão serializada de leitura e commit atômico do checkpoint Bronze."""

    def __init__(self, checkpoint_path: Path, state: BronzeCheckpointState) -> None:
        self.checkpoint_path = checkpoint_path
        self.state = state

    def pending(
        self,
        raw_dir: Path,
        files: Sequence[Path] | None = None,
        *,
        full_refresh: bool = False,
        precomputed: Mapping[str, BronzeFileFingerprint] | None = None,
    ) -> list[BronzeFileFingerprint]:
        discovered = list(files) if files is not None else discover_bronze_files(raw_dir)
        return compute_pending_files(
            raw_dir,
            discovered,
            self.state,
            full_refresh=full_refresh,
            precomputed=precomputed,
        )

    def commit(
        self,
        processed: Sequence[BronzeFileFingerprint],
        *,
        run_id: str,
        replace_all: bool = False,
    ) -> Path:
        self.state.mark_processed(processed, run_id=run_id, replace_all=replace_all)
        return save_checkpoint(self.checkpoint_path, self.state)


@contextmanager
def locked_bronze_checkpoint(
    checkpoint_path: Path,
    *,
    consumer: str,
    timeout_s: float = 60.0,
) -> Iterator[LockedCheckpointSession]:
    """Serializa consumidores Bronze via lock exclusivo e só persiste no commit explícito."""
    lock_path = checkpoint_path.with_suffix(f"{checkpoint_path.suffix}.lock")
    with _exclusive_file_lock(lock_path, timeout_s=timeout_s):
        state = load_checkpoint(checkpoint_path, consumer=consumer)
        yield LockedCheckpointSession(checkpoint_path, state)
