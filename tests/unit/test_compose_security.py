from __future__ import annotations

import os
import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
ENV_EXAMPLE = REPO_ROOT / ".env.example"


def _run_compose_config(
    env: dict[str, str], *, env_file: Path | None = None
) -> subprocess.CompletedProcess[str]:
    if shutil.which("docker") is None:
        pytest.skip("docker CLI indisponível para validar docker-compose.yml")
    cmd = ["docker", "compose", "-f", str(COMPOSE_FILE)]
    if env_file is not None:
        cmd.extend(["--env-file", str(env_file)])
    cmd.append("config")
    return subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=REPO_ROOT)


def _clean_env() -> dict[str, str]:
    drop_prefixes = ("POSTGRES_", "AIRFLOW_", "PNCP_DEPLOY_MODE", "DATABASE_URL")
    return {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(drop_prefixes)
    }


def test_compose_file_has_no_hardcoded_weak_secrets_and_defaults_secure() -> None:
    text = COMPOSE_FILE.read_text(encoding="utf-8")

    # No hardcoded default password/JWT fallback literals
    assert ":-pncp}" not in text.split("POSTGRES_PASSWORD")[1].splitlines()[0]
    assert "dev-local-jwt-secret" not in text

    # Loopback bind by default for published ports
    assert "127.0.0.1" in text
    assert "${POSTGRES_BIND_HOST:-127.0.0.1}:${POSTGRES_PORT:-5432}:5432" in text
    assert "${AIRFLOW_BIND_HOST:-127.0.0.1}:${AIRFLOW_PORT:-8080}:8080" in text

    # Auth manager must NOT grant all-admins by default
    assert 'AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS: "True"' not in text
    assert "${AIRFLOW_SIMPLE_AUTH_ALL_ADMINS:-False}" in text

    # Source/DAG volumes mounted read-only
    assert "./dags:/opt/airflow/dags:ro" in text
    assert "./src:/opt/airflow/src:ro" in text

    # No raw POSTGRES_PASSWORD interpolation inside postgresql URI strings
    assert "postgresql://${POSTGRES_USER" not in text
    assert "postgresql+psycopg2://${POSTGRES_USER" not in text
    assert ":${POSTGRES_PASSWORD" not in text

    # Pre-flight config-check runs BEFORE postgres starts/exposes ports
    assert "config-check:" in text
    pg_section = text.split("  postgres:", 1)[1].split("  airflow-init:", 1)[0]
    assert "config-check:" in pg_section
    assert "service_completed_successfully" in pg_section


def test_compose_requires_secrets_without_env_and_works_with_env_example(tmp_path: Path) -> None:
    empty_env_file = tmp_path / ".env.empty"
    empty_env_file.write_text("", encoding="utf-8")

    missing = _run_compose_config(_clean_env(), env_file=empty_env_file)
    assert missing.returncode != 0
    combined = (missing.stdout + missing.stderr).lower()
    assert "postgres_password" in combined or "airflow_jwt_secret" in combined

    local_ok = _run_compose_config(_clean_env(), env_file=ENV_EXAMPLE)
    assert local_ok.returncode == 0, local_ok.stderr
    assert "127.0.0.1" in local_ok.stdout
    assert "SIMPLE_AUTH_MANAGER_ALL_ADMINS" in local_ok.stdout
    assert any(tok in local_ok.stdout for tok in ('"False"', "'False'", ": False"))
    assert "config-check:" in local_ok.stdout
    rendered_pg = local_ok.stdout.split("  postgres:", 1)[1].split("  spark:", 1)[0]
    assert "config-check:" in rendered_pg
    assert "service_completed_successfully" in rendered_pg


def test_env_example_documents_deploy_mode_bind_hosts_and_secrets() -> None:
    env_text = ENV_EXAMPLE.read_text(encoding="utf-8")
    for key in (
        "PNCP_DEPLOY_MODE=local",
        "POSTGRES_BIND_HOST=127.0.0.1",
        "AIRFLOW_BIND_HOST=127.0.0.1",
        "AIRFLOW_SIMPLE_AUTH_ALL_ADMINS=False",
        "POSTGRES_PASSWORD=",
        "AIRFLOW_JWT_SECRET=",
        "AIRFLOW_FERNET_KEY=",
    ):
        assert key in env_text


def _extract_heredoc_script(marker: str) -> str:
    text = COMPOSE_FILE.read_text(encoding="utf-8")
    match = re.search(rf"python - <<'{marker}'\n(.*?)\n\s*{marker}", text, flags=re.DOTALL)
    assert (
        match is not None
    ), f"Script {marker} não encontrado no docker-compose.yml"
    lines = match.group(1).splitlines()
    indent = min(len(line) - len(line.lstrip()) for line in lines if line.strip())
    return "\n".join(line[indent:] if len(line) >= indent else "" for line in lines)


def _run_validator(extra_env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    script = _extract_heredoc_script("PY_SECURITY")
    env = {**_clean_env(), **extra_env}
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env=env,
    )


def test_compose_secret_validator_enforces_shared_mode_and_allows_reproducible_local() -> None:
    # 1. Local mode with .env.example values on 127.0.0.1 passes
    example_env: dict[str, str] = {}
    for line in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        k, v = stripped.split("=", 1)
        example_env[k.strip()] = v.strip()

    local_env = {
        "PNCP_DEPLOY_MODE": example_env.get("PNCP_DEPLOY_MODE", "local"),
        "POSTGRES_BIND_HOST": example_env.get("POSTGRES_BIND_HOST", "127.0.0.1"),
        "AIRFLOW_BIND_HOST": example_env.get("AIRFLOW_BIND_HOST", "127.0.0.1"),
        "POSTGRES_PASSWORD": example_env["POSTGRES_PASSWORD"],
        "AIRFLOW__API_AUTH__JWT_SECRET": example_env["AIRFLOW_JWT_SECRET"],
        "AIRFLOW__CORE__FERNET_KEY": example_env.get("AIRFLOW_FERNET_KEY", ""),
        "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS": example_env.get(
            "AIRFLOW_SIMPLE_AUTH_ALL_ADMINS", "False"
        ),
    }
    res_local = _run_validator(local_env)
    assert res_local.returncode == 0, res_local.stderr

    # 2. Shared mode with example/weak secrets MUST FAIL
    res_shared_weak = _run_validator({**local_env, "PNCP_DEPLOY_MODE": "shared"})
    assert res_shared_weak.returncode != 0

    # 3. Exposing on 0.0.0.0 with local/example secrets MUST FAIL even if mode=local
    res_exposed = _run_validator({**local_env, "AIRFLOW_BIND_HOST": "0.0.0.0"})
    assert res_exposed.returncode != 0

    # 4. Shared mode with ALL_ADMINS=True MUST FAIL even with strong secrets
    strong_shared = {
        "PNCP_DEPLOY_MODE": "shared",
        "POSTGRES_BIND_HOST": "0.0.0.0",
        "AIRFLOW_BIND_HOST": "0.0.0.0",
        "POSTGRES_PASSWORD": "S3cure!PgPass#2026_Prod99",
        "AIRFLOW__API_AUTH__JWT_SECRET": "x9K2mP7vQ4wR8tY1uN6bC3zL5jH0fD2gA9sE4qW7",
        "AIRFLOW__CORE__FERNET_KEY": "ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg=",
        "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS": "True",
    }
    res_all_admins = _run_validator(strong_shared)
    assert res_all_admins.returncode != 0

    # 5. Truthy/ambiguous ALL_ADMINS variants (t, y, on, 1, yes, invalid, empty) MUST FAIL
    for variant in ("t", "T", "y", "yes", "on", "1", "maybe", ""):
        res_variant = _run_validator(
            {**strong_shared, "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS": variant}
        )
        assert res_variant.returncode != 0, f"ALL_ADMINS={variant!r} deveria ter falhado"
        res_local_variant = _run_validator(
            {**local_env, "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS": variant}
        )
        assert res_local_variant.returncode != 0, f"ALL_ADMINS={variant!r} inválido em local"

    # 6. Invalid or all-zero Fernet key MUST FAIL in shared mode
    for bad_fernet in ("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", "not*valid!base64"):
        res_bad_fernet = _run_validator(
            {
                **strong_shared,
                "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS": "False",
                "AIRFLOW__CORE__FERNET_KEY": bad_fernet,
            }
        )
        assert res_bad_fernet.returncode != 0

    # 7. Shared mode with strong secrets and ALL_ADMINS=False PASSES
    res_shared_ok = _run_validator(
        {**strong_shared, "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS": "False"}
    )
    assert res_shared_ok.returncode == 0, res_shared_ok.stderr


@pytest.mark.parametrize(
    "raw_password",
    [
        "S3cure!PgPass#2026_Prod99",
        "p@ss:w/rd?a=1#frag%20 ok",
    ],
)
def test_compose_uri_builder_url_encodes_postgres_password(raw_password: str) -> None:
    script = _extract_heredoc_script("PY_DB_URI")
    env = {
        **_clean_env(),
        "POSTGRES_USER": "pncp",
        "POSTGRES_PASSWORD": raw_password,
        "POSTGRES_DB": "pncp",
    }
    res = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, env=env)
    assert res.returncode == 0, res.stderr

    exports: dict[str, str] = {}
    for line in res.stdout.strip().splitlines():
        tokens = shlex.split(line)
        assert tokens[0] == "export"
        key, val = tokens[1].split("=", 1)
        exports[key] = val

    for key, expected_db in (
        ("DATABASE_URL", "/pncp"),
        ("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "/airflow"),
    ):
        parsed = urlsplit(exports[key])
        assert parsed.hostname == "postgres"
        assert parsed.port == 5432
        assert parsed.path == expected_db
        assert parsed.fragment == ""
        assert parsed.query == ""
        assert parsed.password is not None
        assert unquote(parsed.password) == raw_password
