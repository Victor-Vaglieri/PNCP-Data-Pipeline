# Guia Operacional de Reprocessamento e Backfill

Este projeto foi desenhado para permitir reexecutar qualquer período sem duplicar registros no Data Warehouse nem perder histórico de auditoria.

## 1. Como a idempotência funciona em cada camada

1. **Bronze (`data/raw/`):** nunca sobrescreve execuções anteriores. Cada arquivo inclui timestamp UTC e `run_id` no nome (`20261123T123303Z_<run_id>_page=0001.json`). Re-ingerir uma data adiciona uma nova coleta para auditoria.
2. **Silver e Gold (`data/processed/`, `data/gold/`):**
   - PySpark usa `spark.sql.sources.partitionOverwriteMode = dynamic` sobre `pub_year/pub_month/uf_sigla` e deduplica por `numero_controle_pncp` mantendo a maior `(data_atualizacao, ingested_at)`.
   - Pandas/PyArrow usa `existing_data_behavior="delete_matching"` com o mesmo efeito por partição.
3. **PostgreSQL (`fact_contratacao`):**
   - `INSERT ... ON CONFLICT (numero_controle_pncp) DO UPDATE` usa guarda monotônica: só atualiza se `EXCLUDED.data_atualizacao >= fact_contratacao.data_atualizacao` (ou se a coluna atual for nula).
   - `stg_contratacao_snapshot` ignora duplicatas da mesma `(numero_controle_pncp, run_id)` via `ON CONFLICT DO NOTHING`.

---

## 2. Cenários comuns de reprocessamento

### Cenário A — Capturar retificações de compras antigas (`contratacoes/atualizacao`)
Licitações publicadas antes podem ser homologadas ou retificadas semanas depois:

```powershell
python -m pncp_pipeline.ingestion --endpoint contratacoes/atualizacao --start 2024-05-24 --end 2024-05-24 --modality 6
docker-compose --profile spark run --rm spark python -m pncp_pipeline.transformations spark
python -m pncp_pipeline.database load
python -m pncp_pipeline.common.quality
```

### Cenário B — Reprocessar uma data específica pelo Airflow 3
Para reexecutar uma data sem esperar o agendamento diário:

```powershell
docker exec pncp-airflow-scheduler python -c "import subprocess, json; subprocess.run(['airflow','dags','trigger','pncp_contratacoes_pipeline','--conf', json.dumps({'target_date':'2024-01-03','modalities':[6,8]})], check=True)"
```

Para reprocessar uma janela histórica de várias datas via **Backfill** do Airflow 3:

```powershell
docker exec pncp-airflow-scheduler airflow dags backfill `
  --dag-id pncp_contratacoes_pipeline `
  --from-date 2024-01-02 `
  --to-date 2024-01-05
```

### Cenário C — Reexecutar apenas uma tarefa que falhou no Airflow
Se apenas a carga ou o Spark falhou por indisponibilidade temporária, limpe somente aquela tarefa (e suas dependentes) em vez de re-ingerir da API:

```powershell
docker exec pncp-airflow-scheduler airflow tasks clear pncp_contratacoes_pipeline -t transform_silver_gold -y
```

---

## 3. Verificação pós-reprocessamento

Após qualquer backfill, valide integridade entre Parquet e PostgreSQL:

```powershell
python -m pncp_pipeline.common.quality
```

E confira os registros de execução em `pipeline_runs`:

```sql
SELECT run_id, stage, status, rows_in, rows_out, duration_s, started_at
FROM pipeline_runs
ORDER BY started_at DESC
LIMIT 10;
```
