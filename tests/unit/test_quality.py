from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pncp_pipeline.common.quality import run_quality_checks


class _FakeCursor:
    def __init__(self, responses: list[tuple[int]]) -> None:
        self._responses = iter(responses)

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, _sql: str) -> _FakeCursor:
        return self

    def fetchone(self) -> tuple[int]:
        return next(self._responses)


class _FakeConn:
    def __init__(self, responses: list[tuple[int]]) -> None:
        self._responses = responses

    def __enter__(self) -> _FakeConn:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._responses)


def _write_dummy_silver(silver_dir: Path, rows: int = 3) -> None:
    target = silver_dir / "contratacoes" / "pub_year=2024" / "pub_month=1" / "uf_sigla=SP"
    target.mkdir(parents=True)
    df = pd.DataFrame({"numero_controle_pncp": [f"id-{i}" for i in range(rows)]})
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), target / "part-0.parquet")


def test_run_quality_checks_passes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_dummy_silver(tmp_path, rows=3)
    monkeypatch.setattr(
        "pncp_pipeline.common.quality.psycopg.connect",
        lambda _url: _FakeConn([(3,), (0,), (0,)]),
    )

    report = run_quality_checks(tmp_path, "postgresql://fake")

    assert report.silver_rows == 3
    assert report.fact_rows == 3
    assert report.duplicate_keys == 0
    assert report.negative_values == 0


def test_run_quality_checks_fails_on_duplicates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_dummy_silver(tmp_path, rows=2)
    monkeypatch.setattr(
        "pncp_pipeline.common.quality.psycopg.connect",
        lambda _url: _FakeConn([(2,), (1,), (0,)]),
    )

    with pytest.raises(ValueError, match="duplicadas"):
        run_quality_checks(tmp_path, "postgresql://fake")


def test_run_quality_checks_fails_without_silver_parquet(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Nenhum arquivo Parquet"):
        run_quality_checks(tmp_path, "postgresql://fake")
