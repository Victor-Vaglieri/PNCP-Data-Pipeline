# Guia Operacional de Reprocessamento e Backfill

Este projeto foi desenhado para permitir reexecutar qualquer período sem duplicar registros no Data Warehouse nem perder histórico de auditoria.

## 1. Como a idempotência e o processamento incremental funcionam em cada camada

1. **Bronze (`data/raw/`):** nunca sobrescreve execuções anteriores. Cada arquivo inclui timestamp UTC e `run_id` no nome (`20261123T123303Z_<run_id>_page=0001.json`). Arquivos temporários (`.tmp_*.json`) são ignorados pelos consumidores.
2. **Checkpoint incremental explícito (fluxo orquestrado Spark/Parquet e PostgreSQL):**
   - **Silver/Gold PySpark (`spark_jobs.py`, invocado pela DAG):** mantém manifesto atômico em `<SILVER_DATA_DIR>/_checkpoints/bronze_to_silver.json` protegido por lock exclusivo (`.lock`).
   - **PostgreSQL (`loader.py`, invocado pela DAG):** persiste o checkpoint na própria transação em `pipeline_runs` (`run_id='__bronze_checkpoint__'`, `stage='bronze_to_postgres_checkpoint'`, `status='checkpoint'`) serializado com `pg_advisory_xact_lock`.
   - O manifesto rastreia `(caminho relativo, sha256, tamanho)`: arquivos de backfill de datas antigas são detectados normalmente; falhas antes do commit não avançam o checkpoint; estados legados sem checkpoint prévio são absorvidos idempotentemente na primeira execução.
   - Os modos standalone `pandas` e `delta` permanecem inalterados.
3. **Silver e Gold (`data/processed/`, `data/gold/`):**
   - No caminho PySpark/Parquet, novos lotes Bronze são mesclados à Silver existente mantendo a versão mais recente por `(data_atualizacao, ingested_at)` (sem regressão em backfills antigos e sem duplicatas se uma retificação alterar partição), gravando em diretórios de staging e aplicando troca com rollback coletivo antes de gravar o checkpoint.
   - Para ignorar o checkpoint e reprocessar toda a Bronze do zero (incluindo limpeza de saídas obsoletas e reset do checkpoint quando a Bronze estiver vazia), use `PNCP_FULL_REFRESH=1` no ambiente ou dispare a DAG no Airflow com `conf: {"full_refresh": true}`.
4. **PostgreSQL (`fact_contratacao`):**
   - `INSERT ... ON CONFLICT (numero_controle_pncp) DO UPDATE` atualiza **todos os campos mutáveis** do fato (incluindo `objeto_compra`, `processo`, `numero_compra`, `srp`, `modo_disputa_*`, `amparo_legal_*`, datas de proposta/publicação e valores) com guarda monotônica em `(data_atualizacao, data_atualizacao_global, ingested_at)`, impedindo regressão de versão.
   - `stg_contratacao_snapshot` ignora duplicatas da mesma `(numero_controle_pncp, run_id)` via `ON CONFLICT DO NOTHING`.

---

## 2. Cenários comuns de reprocessamento

### Cenário A — Capturar retificações de compras antigas (`contratacoes/atualizacao`)
Licitações publicadas antes podem ser homologadas ou retificadas semanas depois:

```powershell
python -m pncp_pipeline.ingestion --endpoint contratacoes/atualizacao --start 2024-05-24 --end 2024-05-24 --modality 6
docker compose --profile spark run --rm spark python -m pncp_pipeline.transformations spark
python -m pncp_pipeline.database load
python -m pncp_pipeline.common.quality
```

Para forçar reprocessamento completo (ignorando o checkpoint incremental):

```powershell
docker compose --profile spark run --rm -e PNCP_FULL_REFRESH=1 spark python -m pncp_pipeline.transformations spark
$env:PNCP_FULL_REFRESH="1"; python -m pncp_pipeline.database load; Remove-Item Env:PNCP_FULL_REFRESH
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
