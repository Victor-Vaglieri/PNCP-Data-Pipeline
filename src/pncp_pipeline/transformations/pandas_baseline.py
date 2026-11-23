from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

CNPJ_RE = re.compile(r"^\d{14}$")


@dataclass(frozen=True)
class TransformSummary:
    engine: str
    files_read: int
    rows_in: int
    rows_valid_dedup: int
    rows_rejected: int
    gold_uf_rows: int
    silver_path: Path
    gold_path: Path


def _clean_text(value: Any, *, upper: bool = False) -> str | None:
    if value is None:
        return None
    collapsed = " ".join(str(value).split())
    if not collapsed:
        return None
    return collapsed.upper() if upper else collapsed


def _validate_row(row: pd.Series) -> str | None:
    if not row["numero_controle_pncp"]:
        return "missing_numero_controle_pncp"
    cnpj = row["orgao_cnpj"]
    if not cnpj or not CNPJ_RE.match(cnpj):
        return "invalid_cnpj"
    if pd.isna(row["modalidade_id"]):
        return "missing_modalidade_id"
    ano = row["ano_compra"]
    if pd.isna(ano) or not (2000 <= int(ano) <= 2100):
        return "invalid_ano_compra"
    if pd.isna(row["data_publicacao_pncp"]):
        return "missing_data_publicacao"
    est = row["valor_total_estimado"]
    if pd.notna(est) and float(est) < 0:
        return "negative_valor_estimado"
    hom = row["valor_total_homologado"]
    if pd.notna(hom) and float(hom) < 0:
        return "negative_valor_homologado"
    return None


def _flatten_bronze_item(
    item: dict[str, Any], meta: dict[str, Any], rel_path: str
) -> dict[str, Any]:
    orgao = item.get("orgaoEntidade") or {}
    unidade = item.get("unidadeOrgao") or {}
    uf = _clean_text(unidade.get("ufSigla"), upper=True) or "ND"
    return {
        "numero_controle_pncp": _clean_text(item.get("numeroControlePNCP")),
        "orgao_cnpj": _clean_text(orgao.get("cnpj")),
        "orgao_razao_social": _clean_text(orgao.get("razaoSocial"), upper=True) or "",
        "orgao_poder_id": _clean_text(orgao.get("poderId")),
        "orgao_esfera_id": _clean_text(orgao.get("esferaId")),
        "codigo_ibge": _clean_text(unidade.get("codigoIbge")),
        "municipio_nome": _clean_text(unidade.get("municipioNome")),
        "uf_sigla": uf,
        "modalidade_id": item.get("modalidadeId"),
        "modalidade_nome": _clean_text(item.get("modalidadeNome")) or "",
        "ano_compra": item.get("anoCompra"),
        "sequencial_compra": item.get("sequencialCompra"),
        "numero_compra": _clean_text(item.get("numeroCompra")),
        "objeto_compra": _clean_text(item.get("objetoCompra")),
        "srp": item.get("srp"),
        "situacao_compra_id": item.get("situacaoCompraId"),
        "situacao_compra_nome": _clean_text(item.get("situacaoCompraNome")),
        "valor_total_estimado": item.get("valorTotalEstimado"),
        "valor_total_homologado": item.get("valorTotalHomologado"),
        "data_inclusao": item.get("dataInclusao"),
        "data_publicacao_pncp": item.get("dataPublicacaoPncp"),
        "data_atualizacao": item.get("dataAtualizacao"),
        "ingested_at": meta.get("ingested_at"),
        "run_id": str(meta.get("run_id") or ""),
        "source_file": rel_path,
    }


def read_bronze_dataframe(raw_dir: Path) -> tuple[int, pd.DataFrame]:
    files = sorted(raw_dir.rglob("*.json"))
    rows: list[dict[str, Any]] = []
    for path in files:
        content = json.loads(path.read_text(encoding="utf-8"))
        meta = content["metadata"]
        rel_path = path.relative_to(raw_dir).as_posix()
        for item in content["payload"]["data"]:
            rows.append(_flatten_bronze_item(item, meta, rel_path))
    return len(files), pd.DataFrame(rows)


def transform_silver_pandas(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Valida, normaliza, deduplica e separa rejeitados em Pandas."""
    if df.empty:
        return df.copy(), df.copy()

    work = df.copy()
    for col in ("data_inclusao", "data_publicacao_pncp", "data_atualizacao"):
        work[col] = pd.to_datetime(work[col], errors="coerce")
    work["ingested_at"] = pd.to_datetime(work["ingested_at"], errors="coerce", utc=True)

    for col in ("modalidade_id", "ano_compra", "sequencial_compra", "situacao_compra_id"):
        work[col] = pd.to_numeric(work[col], errors="coerce").astype("Int64")
    for col in ("valor_total_estimado", "valor_total_homologado"):
        work[col] = pd.to_numeric(work[col], errors="coerce").astype("float64")

    work["rejection_reason"] = work.apply(_validate_row, axis=1)
    rejected = work[work["rejection_reason"].notna()].copy()
    valid = work[work["rejection_reason"].isna()].drop(columns=["rejection_reason"]).copy()

    if valid.empty:
        return valid, rejected

    valid = valid.sort_values(
        by=["numero_controle_pncp", "data_atualizacao", "ingested_at"],
        ascending=[True, False, False],
        na_position="last",
    )
    valid = valid.drop_duplicates(subset=["numero_controle_pncp"], keep="first").copy()

    valid["pub_year"] = valid["data_publicacao_pncp"].dt.year.astype("int32")
    valid["pub_month"] = valid["data_publicacao_pncp"].dt.month.astype("int32")
    valid["tem_homologacao"] = valid["valor_total_homologado"].notna()

    has_ratio = valid["tem_homologacao"] & (valid["valor_total_estimado"] > 0)
    valid["variacao_homologado_pct"] = pd.Series(float("nan"), index=valid.index, dtype="float64")
    valid.loc[has_ratio, "variacao_homologado_pct"] = (
        (
            valid.loc[has_ratio, "valor_total_homologado"]
            - valid.loc[has_ratio, "valor_total_estimado"]
        )
        / valid.loc[has_ratio, "valor_total_estimado"]
        * 100.0
    ).round(2)

    return valid.reset_index(drop=True), rejected.reset_index(drop=True)


def build_gold_uf_mes_pandas(silver_df: pd.DataFrame) -> pd.DataFrame:
    """Gera agregação mensal por UF e modalidade."""
    if silver_df.empty:
        return pd.DataFrame()

    work = silver_df.copy()
    work["estimado_zero"] = work["valor_total_estimado"].fillna(0.0)
    work["homologado_zero"] = work["valor_total_homologado"].fillna(0.0)
    work["economia_item"] = 0.0
    mask = work["valor_total_homologado"].notna() & work["valor_total_estimado"].notna()
    work.loc[mask, "economia_item"] = (
        work.loc[mask, "valor_total_estimado"] - work.loc[mask, "valor_total_homologado"]
    )

    grouped = (
        work.groupby(
            ["pub_year", "pub_month", "uf_sigla", "modalidade_id", "modalidade_nome"],
            as_index=False,
        )
        .agg(
            qtd_contratacoes=("numero_controle_pncp", "count"),
            qtd_homologadas=("valor_total_homologado", "count"),
            total_estimado=("estimado_zero", "sum"),
            total_homologado=("homologado_zero", "sum"),
            economia_homologada=("economia_item", "sum"),
        )
    )
    for col in ("total_estimado", "total_homologado", "economia_homologada"):
        grouped[col] = grouped[col].round(2)
    return grouped.sort_values(
        by=["pub_year", "pub_month", "total_estimado"], ascending=[True, True, False]
    ).reset_index(drop=True)


def _write_partitioned_parquet(df: pd.DataFrame, target: Path, partition_cols: list[str]) -> None:
    if df.empty:
        return
    target.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=str(target),
        partition_cols=partition_cols,
        existing_data_behavior="delete_matching",
    )


def run_pandas_pipeline(
    raw_dir: Path, silver_dir: Path, gold_dir: Path
) -> TransformSummary:
    files_read, raw_df = read_bronze_dataframe(raw_dir)
    valid_df, rejected_df = transform_silver_pandas(raw_df)
    gold_uf_df = build_gold_uf_mes_pandas(valid_df)

    silver_contratacoes = silver_dir / "contratacoes"
    silver_rejected = silver_dir / "rejected" / "contratacoes"
    gold_uf_path = gold_dir / "resumo_uf_mes"

    _write_partitioned_parquet(
        valid_df, silver_contratacoes, ["pub_year", "pub_month", "uf_sigla"]
    )
    if not rejected_df.empty:
        silver_rejected.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.Table.from_pandas(rejected_df, preserve_index=False),
            silver_rejected / "rejected.parquet",
        )
    if not gold_uf_df.empty:
        gold_uf_path.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.Table.from_pandas(gold_uf_df, preserve_index=False),
            gold_uf_path / "resumo_uf_mes.parquet",
        )

    return TransformSummary(
        engine="pandas",
        files_read=files_read,
        rows_in=len(raw_df),
        rows_valid_dedup=len(valid_df),
        rows_rejected=len(rejected_df),
        gold_uf_rows=len(gold_uf_df),
        silver_path=silver_contratacoes,
        gold_path=gold_uf_path,
    )
