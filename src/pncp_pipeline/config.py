from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PNCP_MIN_PAGE_SIZE = 10  # limites documentados no OpenAPI do PNCP (tamanhoPagina)
PNCP_MAX_PAGE_SIZE = 50


@dataclass(frozen=True)
class Settings:
    base_url: str = "https://pncp.gov.br/api/consulta/v1"
    page_size: int = PNCP_MAX_PAGE_SIZE
    request_timeout_s: float = 60.0
    max_retries: int = 5
    retry_wait_initial_s: float = 1.0
    user_agent: str = "pncp-pipeline-portfolio/0.1"
    raw_data_dir: Path = Path("data/raw")
    silver_data_dir: Path = Path("data/processed")
    gold_data_dir: Path = Path("data/gold")
    delta_data_dir: Path = Path("data/delta")
    ibge_base_url: str = "https://servicodados.ibge.gov.br/api/v3"
    database_url: str = "postgresql://pncp:pncp@localhost:5432/pncp"
    log_level: str = "INFO"
    log_format: str = "json"

    def __post_init__(self) -> None:
        if not PNCP_MIN_PAGE_SIZE <= self.page_size <= PNCP_MAX_PAGE_SIZE:
            raise ValueError(
                f"PNCP_PAGE_SIZE deve estar entre {PNCP_MIN_PAGE_SIZE} e "
                f"{PNCP_MAX_PAGE_SIZE} (limite da API), recebido {self.page_size}"
            )
        if self.max_retries < 1:
            raise ValueError("PNCP_MAX_RETRIES deve ser >= 1")

    @classmethod
    def from_env(cls) -> Settings:
        env = os.environ
        defaults = cls()
        return cls(
            base_url=env.get("PNCP_BASE_URL", defaults.base_url).rstrip("/"),
            page_size=int(env.get("PNCP_PAGE_SIZE", defaults.page_size)),
            request_timeout_s=float(env.get("PNCP_REQUEST_TIMEOUT_S", defaults.request_timeout_s)),
            max_retries=int(env.get("PNCP_MAX_RETRIES", defaults.max_retries)),
            user_agent=env.get("PNCP_USER_AGENT", defaults.user_agent),
            raw_data_dir=Path(env.get("RAW_DATA_DIR", str(defaults.raw_data_dir))),
            silver_data_dir=Path(env.get("SILVER_DATA_DIR", str(defaults.silver_data_dir))),
            gold_data_dir=Path(env.get("GOLD_DATA_DIR", str(defaults.gold_data_dir))),
            delta_data_dir=Path(env.get("DELTA_DATA_DIR", str(defaults.delta_data_dir))),
            ibge_base_url=env.get("IBGE_BASE_URL", defaults.ibge_base_url).rstrip("/"),
            database_url=env.get("DATABASE_URL", defaults.database_url),
            log_level=env.get("LOG_LEVEL", defaults.log_level),
            log_format=env.get("LOG_FORMAT", defaults.log_format),
        )
