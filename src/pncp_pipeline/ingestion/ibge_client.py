from __future__ import annotations

import argparse
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv

from pncp_pipeline.common.logging import configure_logging
from pncp_pipeline.config import Settings
from pncp_pipeline.ingestion.bronze_writer import write_json_atomic

IBGE_POPULATION_ENDPOINT = "agregados/6579/periodos/{period}/variaveis/9324"


def parse_ibge_population(payload: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Extrai registros (codigo_ibge, municipio_nome, ano, populacao) do agregado 6579."""
    if not payload:
        return []
    resultados = payload[0].get("resultados") or []
    if not resultados:
        return []
    series = resultados[0].get("series") or []

    rows: list[dict[str, Any]] = []
    for entry in series:
        localidade = entry.get("localidade") or {}
        codigo_ibge = str(localidade.get("id") or "").strip()
        nome = str(localidade.get("nome") or "").strip()
        serie = entry.get("serie") or {}
        for ano_str, valor_raw in serie.items():
            valor_str = str(valor_raw).strip()
            if not codigo_ibge or not valor_str.isdigit():
                continue
            rows.append(
                {
                    "codigo_ibge": codigo_ibge,
                    "municipio_nome": nome,
                    "ano": int(ano_str),
                    "populacao": int(valor_str),
                }
            )
    return rows


def ingest_ibge_population(
    settings: Settings,
    *,
    period: str = "-1",
    http: httpx.Client | None = None,
) -> tuple[Path, int]:
    """Coleta população municipal do IBGE e grava JSON Bronze atômico."""
    url = f"{settings.ibge_base_url}/{IBGE_POPULATION_ENDPOINT.format(period=period)}"
    owns_http = http is None
    client = http or httpx.Client(
        timeout=settings.request_timeout_s,
        headers={"User-Agent": settings.user_agent, "Accept": "application/json"},
    )
    try:
        response = client.get(url, params={"localidades": "N6[all]"})
        response.raise_for_status()
        payload = response.json()
    finally:
        if owns_http:
            client.close()

    rows = parse_ibge_population(payload)
    now = datetime.now(UTC)
    target = settings.raw_data_dir / "source=ibge" / f"populacao_period={period}.json"
    envelope = {
        "metadata": {
            "source": "ibge",
            "url": str(response.url),
            "period": period,
            "ingested_at": now.isoformat(),
            "record_count": len(rows),
        },
        "payload": payload,
    }
    write_json_atomic(target, envelope)
    return target, len(rows)


def read_latest_ibge_population(raw_dir: Path) -> list[dict[str, Any]]:
    ibge_dir = raw_dir / "source=ibge"
    if not ibge_dir.exists():
        return []
    files = sorted(ibge_dir.glob("*.json"))
    if not files:
        return []
    content = json.loads(files[-1].read_text(encoding="utf-8"))
    return parse_ibge_population(content.get("payload") or [])


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(
        prog="python -m pncp_pipeline.ingestion.ibge_client",
        description="Coleta população municipal do IBGE (agregado 6579) para a camada Bronze.",
    )
    parser.add_argument("--period", default="-1", help="Ano ou -1 para o período mais recente")
    args = parser.parse_args(argv)

    settings = Settings.from_env()
    configure_logging(settings.log_level, settings.log_format)
    path, count = ingest_ibge_population(settings, period=args.period)
    logging.getLogger(__name__).info(
        "ibge_ingested", extra={"path": str(path), "municipalities": count}
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
