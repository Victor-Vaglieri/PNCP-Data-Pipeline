from __future__ import annotations

import json
import logging

from pncp_pipeline.common.logging import JsonLogFormatter, configure_logging


def test_json_log_formatter_emits_structured_fields() -> None:
    formatter = JsonLogFormatter()
    record = logging.LogRecord(
        name="pncp.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="carga concluida",
        args=(),
        exc_info=None,
    )
    record.run_id = "abc123"
    record.rows_out = 50

    parsed = json.loads(formatter.format(record))

    assert parsed["level"] == "INFO"
    assert parsed["logger"] == "pncp.test"
    assert parsed["message"] == "carga concluida"
    assert parsed["run_id"] == "abc123"
    assert parsed["rows_out"] == 50
    assert "timestamp" in parsed


def test_json_log_formatter_includes_exception() -> None:
    formatter = JsonLogFormatter()
    try:
        raise RuntimeError("boom")
    except RuntimeError as exc:
        record = logging.LogRecord(
            name="pncp.test",
            level=logging.ERROR,
            pathname=__file__,
            lineno=30,
            msg="falhou",
            args=(),
            exc_info=(type(exc), exc, exc.__traceback__),
        )

    parsed = json.loads(formatter.format(record))

    assert parsed["level"] == "ERROR"
    assert "RuntimeError: boom" in parsed["exception"]


def test_configure_logging_supports_text_and_json() -> None:
    configure_logging(level="WARNING", log_format="json")
    root = logging.getLogger()
    assert root.level == logging.WARNING
    assert isinstance(root.handlers[0].formatter, JsonLogFormatter)

    configure_logging(level="INFO", log_format="text")
    assert not isinstance(root.handlers[0].formatter, JsonLogFormatter)
