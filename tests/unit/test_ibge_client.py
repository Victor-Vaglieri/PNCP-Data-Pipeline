from __future__ import annotations

import json
from pathlib import Path

import httpx
import respx

from pncp_pipeline.config import Settings
from pncp_pipeline.ingestion.ibge_client import (
    ingest_ibge_population,
    parse_ibge_population,
)

SAMPLE_IBGE_PAYLOAD = [
    {
        "id": "9324",
        "variavel": "População residente estimada",
        "resultados": [
            {
                "series": [
                    {
                        "localidade": {"id": "3550308", "nome": "São Paulo - SP"},
                        "serie": {"2026": "11895578"},
                    },
                    {
                        "localidade": {"id": "9999999", "nome": "Município Sem Dado"},
                        "serie": {"2026": "-"},
                    },
                ]
            }
        ],
    }
]


def test_parse_ibge_population_skips_non_numeric_values() -> None:
    rows = parse_ibge_population(SAMPLE_IBGE_PAYLOAD)

    assert rows == [
        {
            "codigo_ibge": "3550308",
            "municipio_nome": "São Paulo - SP",
            "ano": 2026,
            "populacao": 11895578,
        }
    ]


@respx.mock
def test_ingest_ibge_population_writes_atomic_bronze_json(tmp_path: Path) -> None:
    respx.get("https://ibge.test/v3/agregados/6579/periodos/2026/variaveis/9324").mock(
        return_value=httpx.Response(200, json=SAMPLE_IBGE_PAYLOAD)
    )
    settings = Settings(raw_data_dir=tmp_path, ibge_base_url="https://ibge.test/v3")

    path, count = ingest_ibge_population(settings, period="2026")

    assert count == 1
    assert path.exists()
    saved = json.loads(path.read_text(encoding="utf-8"))
    assert saved["metadata"]["source"] == "ibge"
    assert saved["metadata"]["record_count"] == 1
