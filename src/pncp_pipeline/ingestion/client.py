from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

import httpx
from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from pncp_pipeline.config import Settings

log = logging.getLogger(__name__)

REQUIRED_ENVELOPE_INT_FIELDS = ("totalRegistros", "totalPaginas", "numeroPagina")


class PncpError(Exception):
    """Base dos erros do cliente."""


class PncpRequestError(PncpError):
    """Resposta 4xx (exceto 429): a requisição está errada, repetir não adianta."""

    def __init__(self, status_code: int, url: str, body: str) -> None:
        super().__init__(f"HTTP {status_code} em {url}: {body[:300]}")
        self.status_code = status_code


class PncpTransientError(PncpError):
    """Erro temporário (429, 5xx) que vale a pena tentar de novo."""


class PncpResponseError(PncpError):
    """Resposta 200 com corpo que não segue o contrato esperado."""


@dataclass(frozen=True)
class FetchedPage:
    url: str
    status_code: int
    page: int
    payload: dict[str, Any]


class PncpClient:
    def __init__(
        self,
        settings: Settings,
        http: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._settings = settings
        self._owns_http = http is None
        self._http = http or httpx.Client(
            timeout=settings.request_timeout_s,
            headers={"User-Agent": settings.user_agent, "Accept": "application/json"},
        )
        self._sleep = sleep

    def close(self) -> None:
        if self._owns_http:
            self._http.close()

    def __enter__(self) -> PncpClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def fetch_page(self, endpoint: str, params: dict[str, Any]) -> FetchedPage | None:
        """Busca uma página. Retorna None em HTTP 204 (sem registros)."""
        url = f"{self._settings.base_url}/{endpoint.lstrip('/')}"
        retrying = Retrying(
            retry=retry_if_exception_type(
                (httpx.TransportError, PncpTransientError, PncpResponseError)
            ),
            stop=stop_after_attempt(self._settings.max_retries),
            wait=wait_exponential(multiplier=self._settings.retry_wait_initial_s, max=30),
            sleep=self._sleep,
            before_sleep=self._log_retry,
            reraise=True,
        )
        return retrying(self._request_once, url, params)

    def iter_pages(
        self,
        endpoint: str,
        params: dict[str, Any],
        *,
        start_page: int = 1,
        max_pages: int | None = None,
    ) -> Iterator[FetchedPage]:
        """Itera páginas sequencialmente respeitando totalPaginas e HTTP 204."""
        if start_page < 1:
            raise ValueError("start_page deve ser >= 1")
        if max_pages is not None and max_pages < 1:
            raise ValueError("max_pages deve ser >= 1")

        page = start_page
        fetched_count = 0
        while max_pages is None or fetched_count < max_pages:
            page_params = {
                **params,
                "pagina": page,
                "tamanhoPagina": params.get("tamanhoPagina", self._settings.page_size),
            }
            fetched = self.fetch_page(endpoint, page_params)
            if fetched is None:
                break

            yield fetched
            fetched_count += 1

            total_pages = fetched.payload["totalPaginas"]
            if page >= total_pages or fetched.payload.get("empty") is True:
                break
            page += 1

    def _request_once(self, url: str, params: dict[str, Any]) -> FetchedPage | None:
        response = self._http.get(url, params=params)
        status = response.status_code
        if status == 204:
            return None
        if status == 429 or status >= 500:
            raise PncpTransientError(f"HTTP {status} em {url}")
        if status != 200:
            raise PncpRequestError(status, url, response.text)
        payload = _parse_envelope(response)
        return FetchedPage(
            url=str(response.url),
            status_code=status,
            page=payload["numeroPagina"],
            payload=payload,
        )

    @staticmethod
    def _log_retry(state: RetryCallState) -> None:
        error = state.outcome.exception() if state.outcome else None
        log.warning(
            "retry attempt=%s error=%s: %s", state.attempt_number, type(error).__name__, error
        )


def _parse_envelope(response: httpx.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError as exc:
        raise PncpResponseError(f"corpo não é JSON válido: {exc}") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        raise PncpResponseError("envelope sem a lista 'data'")
    for field in REQUIRED_ENVELOPE_INT_FIELDS:
        if not isinstance(payload.get(field), int):
            raise PncpResponseError(f"campo '{field}' ausente ou não numérico no envelope")
    return payload
