from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from pncp_pipeline.ingestion.client import FetchedPage, PncpClient


@dataclass(frozen=True)
class IngestionResult:
    run_id: str
    endpoint: str
    pages_saved: int
    records_fetched: int
    files: tuple[Path, ...]


def build_bronze_path(
    raw_dir: Path,
    endpoint: str,
    modality: int,
    start_date: str,
    end_date: str,
    ingested_at: datetime,
    run_id: str,
    page: int,
) -> Path:
    endpoint_slug = endpoint.strip("/").replace("/", "_")
    ts_slug = ingested_at.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    partition = (
        raw_dir
        / "source=pncp"
        / f"endpoint={endpoint_slug}"
        / f"modality={modality}"
        / f"start={start_date}_end={end_date}"
    )
    return partition / f"{ts_slug}_{run_id}_page={page:04d}.json"


def write_json_atomic(target: Path, content: dict[str, Any]) -> Path:
    """Escreve JSON em arquivo temporário no mesmo diretório e renomeia atomicamente."""
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".tmp_", suffix=".json", dir=target.parent)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(content, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, target)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise
    return target


def write_bronze_page(
    raw_dir: Path,
    endpoint: str,
    params: dict[str, Any],
    fetched: FetchedPage,
    *,
    run_id: str,
    ingested_at: datetime | None = None,
) -> Path:
    now = ingested_at or datetime.now(UTC)
    target = build_bronze_path(
        raw_dir=raw_dir,
        endpoint=endpoint,
        modality=int(params["codigoModalidadeContratacao"]),
        start_date=str(params["dataInicial"]),
        end_date=str(params["dataFinal"]),
        ingested_at=now,
        run_id=run_id,
        page=fetched.page,
    )
    envelope = {
        "metadata": {
            "source": "pncp",
            "endpoint": endpoint.strip("/"),
            "url": fetched.url,
            "params": params,
            "ingested_at": now.astimezone(UTC).isoformat(),
            "http_status": fetched.status_code,
            "run_id": run_id,
            "page": fetched.page,
            "record_count": len(fetched.payload["data"]),
        },
        "payload": fetched.payload,
    }
    return write_json_atomic(target, envelope)


def ingest_contratacoes(
    client: PncpClient,
    raw_dir: Path,
    *,
    start_date: str,
    end_date: str,
    modality: int,
    endpoint: str = "contratacoes/publicacao",
    uf: str | None = None,
    max_pages: int | None = None,
    run_id: str | None = None,
) -> IngestionResult:
    """Executa a coleta paginada e grava cada página na camada Bronze."""
    current_run_id = run_id or uuid4().hex[:12]
    params: dict[str, Any] = {
        "dataInicial": start_date,
        "dataFinal": end_date,
        "codigoModalidadeContratacao": modality,
    }
    if uf:
        params["uf"] = uf.upper()

    saved_files: list[Path] = []
    total_records = 0
    for fetched in client.iter_pages(endpoint, params, max_pages=max_pages):
        page_params = {**params, "pagina": fetched.page}
        file_path = write_bronze_page(
            raw_dir=raw_dir,
            endpoint=endpoint,
            params=page_params,
            fetched=fetched,
            run_id=current_run_id,
        )
        saved_files.append(file_path)
        total_records += len(fetched.payload["data"])

    return IngestionResult(
        run_id=current_run_id,
        endpoint=endpoint,
        pages_saved=len(saved_files),
        records_fetched=total_records,
        files=tuple(saved_files),
    )
