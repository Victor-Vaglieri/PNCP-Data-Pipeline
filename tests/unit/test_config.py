from __future__ import annotations

from pathlib import Path
from urllib.parse import unquote, urlsplit

import pytest

from pncp_pipeline.config import Settings, build_database_url


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


def test_settings_url_encodes_postgres_password_when_database_url_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("POSTGRES_USER", "pncp")
    monkeypatch.setenv("POSTGRES_PASSWORD", "S3cure!PgPass#2026@Prod/1?x%2")
    monkeypatch.setenv("POSTGRES_HOST", "db.internal")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    monkeypatch.setenv("POSTGRES_DB", "pncp")

    settings = Settings.from_env()
    parsed = urlsplit(settings.database_url)
    assert parsed.hostname == "db.internal"
    assert parsed.port == 5433
    assert parsed.path == "/pncp"
    assert parsed.fragment == ""
    assert parsed.query == ""
    assert parsed.password is not None
    assert unquote(parsed.password) == "S3cure!PgPass#2026@Prod/1?x%2"
    assert settings.database_url == build_database_url(
        user="pncp",
        password="S3cure!PgPass#2026@Prod/1?x%2",
        host="db.internal",
        port="5433",
        database="pncp",
    )
