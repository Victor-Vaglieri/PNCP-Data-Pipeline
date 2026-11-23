from __future__ import annotations

import httpx
import pytest
import respx

from pncp_pipeline.config import Settings
from pncp_pipeline.ingestion.client import (
    PncpClient,
    PncpRequestError,
    PncpResponseError,
    PncpTransientError,
)


def _envelope(page: int, total_pages: int = 1, items: list[dict] | None = None) -> dict:
    records = items if items is not None else [{"numeroControlePNCP": f"id-{page}"}]
    return {
        "totalRegistros": total_pages * len(records),
        "totalPaginas": total_pages,
        "numeroPagina": page,
        "paginasRestantes": max(total_pages - page, 0),
        "empty": len(records) == 0,
        "data": records,
    }


def _settings(max_retries: int = 3) -> Settings:
    return Settings(
        base_url="https://pncp.test/v1",
        page_size=50,
        request_timeout_s=5.0,
        max_retries=max_retries,
    )


@respx.mock
def test_fetch_page_success() -> None:
    respx.get("https://pncp.test/v1/contratacoes/publicacao").mock(
        return_value=httpx.Response(200, json=_envelope(page=1, total_pages=2))
    )

    with PncpClient(_settings(), sleep=lambda _: None) as client:
        page = client.fetch_page("contratacoes/publicacao", {"pagina": 1})

    assert page is not None
    assert page.status_code == 200
    assert page.page == 1
    assert page.payload["totalPaginas"] == 2


@respx.mock
def test_fetch_page_204_returns_none() -> None:
    respx.get("https://pncp.test/v1/contratacoes/publicacao").mock(
        return_value=httpx.Response(204)
    )

    with PncpClient(_settings(), sleep=lambda _: None) as client:
        page = client.fetch_page("contratacoes/publicacao", {"pagina": 1})

    assert page is None


@respx.mock
def test_fetch_page_retries_on_429_and_503_then_succeeds() -> None:
    route = respx.get("https://pncp.test/v1/contratacoes/publicacao").mock(
        side_effect=[
            httpx.Response(429),
            httpx.Response(503),
            httpx.Response(200, json=_envelope(page=1)),
        ]
    )

    with PncpClient(_settings(max_retries=4), sleep=lambda _: None) as client:
        page = client.fetch_page("contratacoes/publicacao", {"pagina": 1})

    assert page is not None
    assert route.call_count == 3


@respx.mock
def test_fetch_page_retries_on_timeout() -> None:
    route = respx.get("https://pncp.test/v1/contratacoes/publicacao").mock(
        side_effect=[
            httpx.ReadTimeout("timeout"),
            httpx.Response(200, json=_envelope(page=1)),
        ]
    )

    with PncpClient(_settings(max_retries=3), sleep=lambda _: None) as client:
        page = client.fetch_page("contratacoes/publicacao", {"pagina": 1})

    assert page is not None
    assert route.call_count == 2


@respx.mock
def test_fetch_page_400_does_not_retry() -> None:
    route = respx.get("https://pncp.test/v1/contratacoes/publicacao").mock(
        return_value=httpx.Response(400, json={"message": "Tamanho de página inválido"})
    )

    with PncpClient(_settings(max_retries=4), sleep=lambda _: None) as client:
        with pytest.raises(PncpRequestError) as exc:
            client.fetch_page("contratacoes/publicacao", {"pagina": 1})

    assert exc.value.status_code == 400
    assert route.call_count == 1


@respx.mock
def test_fetch_page_exhausts_retries_on_500() -> None:
    route = respx.get("https://pncp.test/v1/contratacoes/publicacao").mock(
        return_value=httpx.Response(500)
    )

    with PncpClient(_settings(max_retries=3), sleep=lambda _: None) as client:
        with pytest.raises(PncpTransientError):
            client.fetch_page("contratacoes/publicacao", {"pagina": 1})

    assert route.call_count == 3


@respx.mock
def test_fetch_page_rejects_malformed_payload() -> None:
    respx.get("https://pncp.test/v1/contratacoes/publicacao").mock(
        return_value=httpx.Response(200, json={"unexpected": True})
    )

    with PncpClient(_settings(max_retries=2), sleep=lambda _: None) as client:
        with pytest.raises(PncpResponseError, match="data"):
            client.fetch_page("contratacoes/publicacao", {"pagina": 1})


@respx.mock
def test_iter_pages_paginates_until_total_pages() -> None:
    route = respx.get("https://pncp.test/v1/contratacoes/publicacao").mock(
        side_effect=[
            httpx.Response(200, json=_envelope(page=1, total_pages=3)),
            httpx.Response(200, json=_envelope(page=2, total_pages=3)),
            httpx.Response(200, json=_envelope(page=3, total_pages=3)),
        ]
    )

    params = {"dataInicial": "20240102", "dataFinal": "20240102", "codigoModalidadeContratacao": 6}
    with PncpClient(_settings(), sleep=lambda _: None) as client:
        pages = list(client.iter_pages("contratacoes/publicacao", params))

    assert [p.page for p in pages] == [1, 2, 3]
    assert route.call_count == 3


@respx.mock
def test_iter_pages_respects_max_pages() -> None:
    route = respx.get("https://pncp.test/v1/contratacoes/publicacao").mock(
        side_effect=[
            httpx.Response(200, json=_envelope(page=1, total_pages=5)),
            httpx.Response(200, json=_envelope(page=2, total_pages=5)),
        ]
    )

    params = {"dataInicial": "20240102", "dataFinal": "20240102", "codigoModalidadeContratacao": 6}
    with PncpClient(_settings(), sleep=lambda _: None) as client:
        pages = list(client.iter_pages("contratacoes/publicacao", params, max_pages=2))

    assert [p.page for p in pages] == [1, 2]
    assert route.call_count == 2
