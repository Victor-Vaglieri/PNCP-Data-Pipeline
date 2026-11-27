from __future__ import annotations

import importlib.util
import sys
import types
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from pncp_pipeline.common.quality import QualityReport
from pncp_pipeline.database.loader import LoadSummary
from pncp_pipeline.ingestion.bronze_writer import IngestionResult
from pncp_pipeline.transformations.pandas_baseline import TransformSummary

DAG_FILE = Path(__file__).resolve().parents[2] / "dags" / "pncp_contratacoes_pipeline.py"


@dataclass
class _CapturedDag:
    kwargs: dict[str, Any]
    tasks: dict[str, Any]
    edges: list[tuple[str, str]]
    expanded: list[str]


def _load_dag_module(monkeypatch: pytest.MonkeyPatch, context_box: dict[str, Any]):
    """Importa a DAG com stub leve de airflow.sdk para testar tarefas e grafo sem Airflow."""
    captured = _CapturedDag(kwargs={}, tasks={}, edges=[], expanded=[])

    class _TaskProxy:
        def __init__(self, fn: Any) -> None:
            self.fn = fn
            self.name = fn.__name__

        def __call__(self, *args: Any, **kwargs: Any) -> _TaskRef:
            for arg in list(args) + list(kwargs.values()):
                if isinstance(arg, _TaskRef):
                    captured.edges.append((arg.name, self.name))
            return _TaskRef(self.name)

        def expand(self, **kwargs: Any) -> _TaskRef:
            captured.expanded.append(self.name)
            for arg in kwargs.values():
                if isinstance(arg, _TaskRef):
                    captured.edges.append((arg.name, self.name))
            return _TaskRef(self.name)

    @dataclass
    class _TaskRef:
        name: str

    def fake_dag(**kwargs: Any):
        captured.kwargs.update(kwargs)

        def decorator(fn: Any):
            def wrapper(*args: Any, **kw: Any):
                return fn(*args, **kw)

            return wrapper

        return decorator

    def fake_task(fn: Any):
        captured.tasks[fn.__name__] = fn
        return _TaskProxy(fn)

    def fake_get_current_context() -> dict[str, Any]:
        return context_box["ctx"]

    airflow_mod = types.ModuleType("airflow")
    sdk_mod = types.ModuleType("airflow.sdk")
    sdk_mod.dag = fake_dag  # type: ignore[attr-defined]
    sdk_mod.task = fake_task  # type: ignore[attr-defined]
    sdk_mod.get_current_context = fake_get_current_context  # type: ignore[attr-defined]
    airflow_mod.sdk = sdk_mod  # type: ignore[attr-defined]

    # Stub spark_jobs import so DAG imports cleanly even when pyspark is absent
    fake_spark_mod = types.ModuleType("pncp_pipeline.transformations.spark_jobs")
    fake_spark_mod.create_spark_session = lambda name="test": object()  # type: ignore[attr-defined]
    fake_spark_mod.run_spark_pipeline = lambda *a, **kw: None  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "airflow", airflow_mod)
    monkeypatch.setitem(sys.modules, "airflow.sdk", sdk_mod)
    if "pyspark" not in sys.modules:
        monkeypatch.setitem(sys.modules, "pncp_pipeline.transformations.spark_jobs", fake_spark_mod)

    spec = importlib.util.spec_from_file_location("pncp_dag_under_test", DAG_FILE)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, captured


def test_dag_metadata_and_task_wiring(monkeypatch: pytest.MonkeyPatch) -> None:
    _, captured = _load_dag_module(monkeypatch, {"ctx": {}})

    assert captured.kwargs["dag_id"] == "pncp_contratacoes_pipeline"
    assert captured.kwargs["schedule"] == "@daily"
    assert captured.kwargs["catchup"] is False
    assert captured.kwargs["max_active_runs"] == 1
    assert captured.kwargs["default_args"]["retries"] >= 1
    assert captured.kwargs["default_args"]["retry_exponential_backoff"] is True

    assert "ingest_modality" in captured.expanded
    assert ("resolve_window", "list_ingestion_targets") in captured.edges
    assert ("list_ingestion_targets", "ingest_modality") in captured.edges
    assert ("ingest_modality", "transform_silver_gold") in captured.edges
    assert ("transform_silver_gold", "load_postgres") in captured.edges
    assert ("load_postgres", "data_quality_checks") in captured.edges


def test_dag_resolve_window_supports_conf_scheduled_and_full_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    box: dict[str, Any] = {"ctx": {}}
    _, captured = _load_dag_module(monkeypatch, box)
    resolve_window = captured.tasks["resolve_window"]
    list_targets = captured.tasks["list_ingestion_targets"]

    # Manual trigger via conf with target_date and full_refresh
    dag_run = types.SimpleNamespace(
        conf={
            "target_date": "2024-05-24",
            "modalities": [6],
            "max_pages": 2,
            "full_refresh": True,
        }
    )
    box["ctx"] = {"dag_run": dag_run}
    window = resolve_window()
    assert window == {
        "start_date": "20240524",
        "end_date": "20240524",
        "max_pages": 2,
        "modalities": [6],
        "full_refresh": True,
    }
    targets = list_targets(window)
    assert targets == [
        {
            "start_date": "20240524",
            "end_date": "20240524",
            "modality": 6,
            "max_pages": 2,
            "full_refresh": True,
        }
    ]

    # Scheduled run leaves full_refresh=None (so PNCP_FULL_REFRESH env can apply)
    box["ctx"] = {
        "dag_run": types.SimpleNamespace(conf={}),
        "data_interval_start": datetime(2024, 1, 3, tzinfo=UTC),
    }
    sched_window = resolve_window()
    assert sched_window["start_date"] == "20240103"
    assert sched_window["end_date"] == "20240103"
    assert sched_window["modalities"] == [6, 8]
    assert sched_window["full_refresh"] is None

    # Fallback to logical_date when data_interval_start is absent and dag_run is None
    box["ctx"] = {"dag_run": None, "logical_date": datetime(2024, 2, 10, tzinfo=UTC)}
    fallback_window = resolve_window()
    assert fallback_window["start_date"] == "20240210"
    assert fallback_window["full_refresh"] is None

    # Safe boolean parsing for string variants in conf (must NOT treat "false"/"0" as True)
    for falsy in ("false", "False", "0", "no", "off", ""):
        box["ctx"] = {
            "dag_run": types.SimpleNamespace(conf={"full_refresh": falsy}),
            "data_interval_start": datetime(2024, 1, 3, tzinfo=UTC),
        }
        assert resolve_window()["full_refresh"] is False

    for truthy in ("true", "True", "1", "yes", "on"):
        box["ctx"] = {
            "dag_run": types.SimpleNamespace(conf={"full_refresh": truthy}),
            "data_interval_start": datetime(2024, 1, 3, tzinfo=UTC),
        }
        assert resolve_window()["full_refresh"] is True

    box["ctx"] = {
        "dag_run": types.SimpleNamespace(conf={"full_refresh": "not-a-bool"}),
        "data_interval_start": datetime(2024, 1, 3, tzinfo=UTC),
    }
    with pytest.raises(ValueError, match="full_refresh"):
        resolve_window()


def test_dag_tasks_execute_normal_empty_and_propagate_full_refresh(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    box: dict[str, Any] = {"ctx": {}}
    mod, captured = _load_dag_module(monkeypatch, box)

    class _DummyClient:
        def __init__(self, _settings: Any) -> None:
            pass

        def __enter__(self) -> _DummyClient:
            return self

        def __exit__(self, *exc: object) -> None:
            return None

    class _DummySpark:
        def __init__(self) -> None:
            self.stopped = False

        def stop(self) -> None:
            self.stopped = True

    dummy_spark = _DummySpark()
    spark_calls: list[dict[str, Any]] = []
    pg_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(mod, "PncpClient", _DummyClient)
    monkeypatch.setattr(
        mod,
        "ingest_contratacoes",
        lambda **kw: IngestionResult(
            run_id="r-empty",
            endpoint="contratacoes/publicacao",
            pages_saved=0,
            records_fetched=0,
            files=(),
        ),
    )
    monkeypatch.setattr(mod, "create_spark_session", lambda _name: dummy_spark)

    def fake_spark_pipeline(spark, raw_dir, silver_dir, gold_dir, *, full_refresh=False):
        spark_calls.append({"full_refresh": full_refresh})
        return TransformSummary(
            engine="pyspark",
            files_read=0,
            rows_in=0,
            rows_valid_dedup=0,
            rows_rejected=0,
            gold_uf_rows=0,
            silver_path=tmp_path / "silver",
            gold_path=tmp_path / "gold",
        )

    def fake_load_postgres(raw_dir, db_url, *, full_refresh=False):
        pg_calls.append({"full_refresh": full_refresh})
        return LoadSummary(
            run_id="pg-1",
            files_read=0,
            rows_in=0,
            rows_deduplicated=0,
            duration_s=0.1,
        )

    monkeypatch.setattr(mod, "run_spark_pipeline", fake_spark_pipeline)
    monkeypatch.setattr(mod, "load_bronze_to_postgres", fake_load_postgres)
    monkeypatch.setattr(
        mod,
        "run_quality_checks",
        lambda *a, **kw: QualityReport(
            silver_rows=5,
            gold_rows=5,
            fact_rows=5,
            duplicate_keys=0,
            null_required_fields=0,
            negative_values=0,
        ),
    )

    ing = captured.tasks["ingest_modality"](
        {
            "start_date": "20240101",
            "end_date": "20240101",
            "modality": 6,
            "max_pages": None,
            "full_refresh": True,
        }
    )
    assert ing["pages_saved"] == 0
    assert ing["records_fetched"] == 0
    assert ing["full_refresh"] is True

    tr = captured.tasks["transform_silver_gold"]([ing])
    assert dummy_spark.stopped is True
    assert spark_calls == [{"full_refresh": True}]
    assert tr["full_refresh"] is True

    ld = captured.tasks["load_postgres"](tr)
    assert pg_calls == [{"full_refresh": True}]
    assert ld["rows_deduplicated"] == 0

    qa = captured.tasks["data_quality_checks"](ld)
    assert qa["fact_rows"] == 5
    assert qa["duplicate_keys"] == 0


def test_dag_honors_pncp_full_refresh_env_and_explicit_conf_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    box: dict[str, Any] = {"ctx": {}}
    mod, captured = _load_dag_module(monkeypatch, box)

    class _DummySpark:
        def __init__(self) -> None:
            self.stopped = False

        def stop(self) -> None:
            self.stopped = True

    spark_calls: list[bool] = []
    pg_calls: list[bool] = []
    monkeypatch.setattr(mod, "create_spark_session", lambda _name: _DummySpark())

    def fake_spark(_spark, _raw, _silver, _gold, *, full_refresh=False):
        spark_calls.append(full_refresh)
        return TransformSummary(
            engine="pyspark",
            files_read=1,
            rows_in=2,
            rows_valid_dedup=2,
            rows_rejected=0,
            gold_uf_rows=1,
            silver_path=tmp_path / "silver",
            gold_path=tmp_path / "gold",
        )

    def fake_pg(_raw, _db, *, full_refresh=False):
        pg_calls.append(full_refresh)
        return LoadSummary(
            run_id="pg-env",
            files_read=1,
            rows_in=2,
            rows_deduplicated=2,
            duration_s=0.1,
        )

    monkeypatch.setattr(mod, "run_spark_pipeline", fake_spark)
    monkeypatch.setattr(mod, "load_bronze_to_postgres", fake_pg)

    # 1. Default incremental (no conf, no env) -> False
    monkeypatch.delenv("PNCP_FULL_REFRESH", raising=False)
    tr_inc = captured.tasks["transform_silver_gold"]([{"modality": 6, "full_refresh": None}])
    ld_inc = captured.tasks["load_postgres"](tr_inc)
    assert spark_calls[-1] is False
    assert pg_calls[-1] is False
    assert tr_inc["full_refresh"] is None
    assert tr_inc["effective_full_refresh"] is False
    assert ld_inc["effective_full_refresh"] is False

    # 2. PNCP_FULL_REFRESH=1 honored when conf omitted full_refresh (full_refresh=None)
    monkeypatch.setenv("PNCP_FULL_REFRESH", "1")
    tr_env = captured.tasks["transform_silver_gold"]([{"modality": 6, "full_refresh": None}])
    ld_env = captured.tasks["load_postgres"](tr_env)
    assert spark_calls[-1] is True
    assert pg_calls[-1] is True
    assert tr_env["effective_full_refresh"] is True
    assert ld_env["effective_full_refresh"] is True

    # 3. Explicit conf={"full_refresh": "false"} overrides PNCP_FULL_REFRESH=1
    tr_override = captured.tasks["transform_silver_gold"](
        [{"modality": 6, "full_refresh": "false"}]
    )
    ld_override = captured.tasks["load_postgres"](tr_override)
    assert spark_calls[-1] is False
    assert pg_calls[-1] is False
    assert tr_override["full_refresh"] is False
    assert ld_override["effective_full_refresh"] is False


def test_dag_stops_spark_on_error_and_propagates_task_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    box: dict[str, Any] = {"ctx": {}}
    mod, captured = _load_dag_module(monkeypatch, box)

    class _DummySpark:
        def __init__(self) -> None:
            self.stopped = False

        def stop(self) -> None:
            self.stopped = True

    spark = _DummySpark()
    monkeypatch.setattr(mod, "create_spark_session", lambda _name: spark)
    monkeypatch.setattr(
        mod,
        "run_spark_pipeline",
        lambda *_a, **_kw: (_ for _ in ()).throw(RuntimeError("spark boom")),
    )
    with pytest.raises(RuntimeError, match="spark boom"):
        captured.tasks["transform_silver_gold"]([{"modality": 6, "full_refresh": None}])
    assert spark.stopped is True

    monkeypatch.setattr(
        mod,
        "load_bronze_to_postgres",
        lambda *_a, **_kw: (_ for _ in ()).throw(RuntimeError("pg boom")),
    )
    with pytest.raises(RuntimeError, match="pg boom"):
        captured.tasks["load_postgres"]({"full_refresh": None})

    monkeypatch.setattr(
        mod,
        "run_quality_checks",
        lambda *_a, **_kw: (_ for _ in ()).throw(RuntimeError("qa boom")),
    )
    with pytest.raises(RuntimeError, match="qa boom"):
        captured.tasks["data_quality_checks"]({})
