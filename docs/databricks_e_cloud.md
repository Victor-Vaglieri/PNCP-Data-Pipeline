# Delta Lake, Databricks Free Edition e Arquitetura em Nuvem (Etapa 6)

## 1. Por que Delta Lake sobre Parquet?

O Parquet puro é imutável: se uma licitação publicada em janeiro for homologada em maio (`dataAtualizacao` mais recente), precisamos reescrever a partição inteira.
O **Delta Lake** adiciona o diretório `_delta_log/` em cima dos mesmos arquivos Parquet, trazendo:
- **Transações ACID:** leitores nunca enxergam escrita parcial.
- **`MERGE INTO` (Upsert):** atualiza apenas os registros modificados pela chave natural (`numero_controle_pncp`) com guarda monotônica em `data_atualizacao`.
- **Time Travel:** cada operação gera uma versão (`version 0`, `1`, ...), permitindo auditar estados anteriores via `option("versionAsOf", 0)` e `DESCRIBE HISTORY`.

### Execução local no container Spark
```powershell
docker-compose --profile spark build spark
docker-compose --profile spark run --rm spark python -m pncp_pipeline.transformations delta
docker-compose --profile spark run --rm spark python -m pncp_pipeline.transformations delta-history
```

---

## 2. Como rodar no Databricks Free Edition

O **Databricks Free Edition** usa **Serverless Compute** gerenciado e Unity Catalog, com duas restrições importantes:
1. **Saída de internet restrita:** notebooks serverless não acessam APIs arbitrárias como `pncp.gov.br`. Por isso a ingestão continua rodando localmente ou no Airflow.
2. **Sem `.cache()` / `.persist()` / env vars:** o ambiente multi-tenant serverless bloqueia comandos de cache e variáveis de ambiente, usando `dbutils.widgets` para parâmetros.

### Passo a passo no Databricks Free Edition
1. Crie uma conta gratuita no Databricks Free Edition.
2. No menu **Catalog**, crie um Volume em `workspace.default`, por exemplo `pncp` (`/Volumes/workspace/default/pncp/`).
3. Faça upload da pasta local `data/processed/contratacoes/` (gerada pelo Airflow ou pelo container `spark`) para `/Volumes/workspace/default/pncp/silver/contratacoes/`.
4. Importe o notebook [pncp_gold_delta.ipynb](../databricks/pncp_gold_delta.ipynb) no Workspace.
5. Conecte ao compute **Serverless** e execute todas as células: ele fará o `MERGE` na tabela Delta `silver_pncp_contratacoes`, gerará `gold_pncp_resumo_uf_mes` e exibirá o `DESCRIBE HISTORY`.

---

## 3. Como esta arquitetura iria para Azure ou GCP

O código de ingestão, qualidade e PySpark do projeto não depende de nuvem, mas em produção os mesmos papéis seriam mapeados assim:

| Papel no pipeline | Local (este repo) | Azure | GCP |
|---|---|---|---|
| **Orquestração** | Airflow 3 (`LocalExecutor`) | Azure Data Factory ou Airflow gerenciado | Cloud Composer (Airflow gerenciado) |
| **Camadas Bronze/Silver/Gold** | `data/` (JSON / Parquet / Delta) | ADLS Gen2 (Delta Lake) | Google Cloud Storage (Parquet / Iceberg / Delta) |
| **Compute distribuído** | PySpark + Delta em container | Azure Databricks (Jobs) | Dataproc Serverless ou BigQuery |
| **Banco analítico (Gold/Fato)** | PostgreSQL 16 (Docker) | Azure Database for PostgreSQL | Cloud SQL for PostgreSQL ou BigQuery |
| **Segredos e observabilidade** | `.env` + `pipeline_runs` + logs JSON | Key Vault + Azure Monitor | Secret Manager + Cloud Logging |
