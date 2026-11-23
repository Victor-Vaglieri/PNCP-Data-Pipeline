from __future__ import annotations

import argparse

import pytest

from pncp_pipeline.ingestion.__main__ import main, normalize_api_date


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("2024-01-02", "20240102"), ("20240102", "20240102")],
)
def test_normalize_api_date_valid(raw: str, expected: str) -> None:
    assert normalize_api_date(raw) == expected


@pytest.mark.parametrize("bad", ["2024-13-01", "02/01/2024", "abc"])
def test_normalize_api_date_invalid(bad: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        normalize_api_date(bad)


def test_cli_rejects_start_after_end() -> None:
    with pytest.raises(SystemExit, match="maior que data final"):
        main(["--start", "2024-01-10", "--end", "2024-01-02"])
