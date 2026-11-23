from __future__ import annotations

from pathlib import Path

import pytest

from pncp_pipeline.config import Settings


def test_settings_from_env_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PNCP_BASE_URL", "https://example.test/api/")
    monkeypatch.setenv("PNCP_PAGE_SIZE", "25")
    monkeypatch.setenv("PNCP_REQUEST_TIMEOUT_S", "15")
    monkeypatch.setenv("PNCP_MAX_RETRIES", "3")
    monkeypatch.setenv("RAW_DATA_DIR", "custom/raw")

    settings = Settings.from_env()

    assert settings.base_url == "https://example.test/api"
    assert settings.page_size == 25
    assert settings.request_timeout_s == 15.0
    assert settings.max_retries == 3
    assert settings.raw_data_dir == Path("custom/raw")


@pytest.mark.parametrize("bad_size", ["9", "51", "100"])
def test_settings_rejects_out_of_range_page_size(
    monkeypatch: pytest.MonkeyPatch, bad_size: str
) -> None:
    monkeypatch.setenv("PNCP_PAGE_SIZE", bad_size)

    with pytest.raises(ValueError, match="PNCP_PAGE_SIZE"):
        Settings.from_env()
