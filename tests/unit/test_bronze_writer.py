from __future__ import annotations

import json
import os
from pathlib import Path

import httpx
import pytest
import respx

from pncp_pipeline.config import Settings
from pncp_pipeline.ingestion.bronze_writer import ingest_contratacoes, write_json_atomic
from pncp_pipeline.ingestion.client import PncpClient


@respx.mock
def test_ingest_contratacoes_writes_bronze_with_metadata(tmp_path: Path) -> None:
    payload_p1 = {
        "totalRegistros": 2,
        "totalPaginas": 2,
        "numeroPagina": 1,
        "paginasRestantes": 1,
        "empty": False,
        "data": [{"numeroControlePNCP": "001"}],
    }
    payload_p2 = {
        "totalRegistros": 2,
        "totalPaginas": 2,
        "numeroPagina": 2,
        "paginasRestantes": 0,
        "empty": False,
        "data": [{"numeroControlePNCP": "002"}],
    }
    respx.get("https://pncp.test/v1/contratacoes/publicacao").mock(
        side_effect=[
            httpx.Response(200, json=payload_p1),
            httpx.Response(200, json=payload_p2),
        ]
    )

    settings = Settings(base_url="https://pncp.test/v1", raw_data_dir=tmp_path)
    with PncpClient(settings, sleep=lambda _: None) as client:
        result = ingest_contratacoes(
            client=client,
            raw_dir=tmp_path,
            start_date="20240102",
            end_date="20240102",
            modality=6,
            run_id="run123",
        )

    assert result.pages_saved == 2
    assert result.records_fetched == 2
    assert len(result.files) == 2

    saved = json.loads(result.files[0].read_text(encoding="utf-8"))
    assert saved["metadata"]["source"] == "pncp"
    assert saved["metadata"]["run_id"] == "run123"
    assert saved["metadata"]["page"] == 1
    assert saved["metadata"]["record_count"] == 1
    assert saved["payload"] == payload_p1


@respx.mock
def test_ingest_contratacoes_handles_204_empty_period(tmp_path: Path) -> None:
    respx.get("https://pncp.test/v1/contratacoes/publicacao").mock(
        return_value=httpx.Response(204)
    )

    settings = Settings(base_url="https://pncp.test/v1", raw_data_dir=tmp_path)
    with PncpClient(settings, sleep=lambda _: None) as client:
        result = ingest_contratacoes(
            client=client,
            raw_dir=tmp_path,
            start_date="20240101",
            end_date="20240101",
            modality=6,
        )

    assert result.pages_saved == 0
    assert result.records_fetched == 0
    assert result.files == ()


def test_write_json_atomic_leaves_no_temp_file_on_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "bronze.json"

    def fail_replace(src: Path, dst: Path) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(os, "replace", fail_replace)

    with pytest.raises(OSError, match="disk full"):
        write_json_atomic(target, {"ok": True})

    assert list(tmp_path.iterdir()) == []
