# PNCP Data Pipeline

[![CI](https://github.com/Victor-Vaglieri/PNCP-Data-Pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Victor-Vaglieri/PNCP-Data-Pipeline/actions/workflows/ci.yml)

**PNCP Data Pipeline** é um pipeline de Engenharia de Dados ponta a ponta voltado para a coleta, padronização, deduplicação e análise de contratações públicas brasileiras a partir da API oficial do **Portal Nacional de Contratações Públicas (PNCP)**. O projeto evolui de forma incremental até a arquitetura Medallion completa (Bronze, Silver e Gold), banco analítico em PostgreSQL, orquestração no Apache Airflow e processamento em Parquet / Delta Lake.

A solução prioriza código testável, idempotência nas reexecuções, preservação integral dos dados brutos para auditoria e execução local reproduzível via Docker.

## 1. Visão Geral (O Problema)

Acompanhar gastos e licitações públicas exige lidar com JSONs aninhados, paginação limitada pela API, registros que são retificados dias ou meses depois (`dataAtualizacao`) e instabilidades pontuais de rede.
* **Para Auditoria e Reprocessamento:** A camada Bronze preserva exatamente a resposta recebida da API junto com metadados de execução (`run_id`, `ingested_at`, URL e parâmetros), evitando reconsultas desnecessárias e permitindo reprocessar regras de limpeza sem perda de histórico.
* **Para Análise e Consumo (BI / SQL):** Em vez de consultar APIs lentas ou manipular arquivos JSON brutos, os dados serão achatados, validados e deduplicados em Parquet (Silver/Gold) e carregados de forma idempotente em um modelo relacional PostgreSQL.

## 2. Roteiro e Fases de Desenvolvimento

1. **Etapa 1 (Ingestão Bronze e Resiliência) - CONCLUÍDA:** Cliente HTTP com `httpx` e `tenacity` (timeout, retry com backoff em `429`/`5xx`/rede e falha imediata em `4xx`), paginação completa respeitando o contrato OpenAPI do PNCP (`tamanhoPagina` 10–50 e `HTTP 204`), escrita atômica em JSON com envelope de metadados, CLI e suíte unitária com `respx`.
2. **Etapa 2 (SQL e Modelagem Relacional) - CONCLUÍDA:** PostgreSQL 16 via Docker Compose, modelagem dimensional (`dim_orgao`, `dim_municipio`, `dim_modalidade`, `fact_contratacao`, `stg_contratacao_snapshot`, `pipeline_runs`), loader idempotente com `ON CONFLICT DO UPDATE` e guarda monotônica por `data_atualizacao`, documentação em `docs/modelagem.md` e consultas analíticas com CTEs, `JOIN` e funções de janela (`ROW_NUMBER`, `DENSE_RANK`, `LAG`, `SUM() OVER`).
3. **Etapa 3 (Transformação Pandas e PySpark / Parquet) - CONCLUÍDA:** Baseline em Pandas (`pandas_baseline.py`) e pipeline PySpark em container Linux (`spark_jobs.py` com `StructType` explícito), quarentena de registros inválidos (`rejected/`), deduplicação por janela, paridade validada entre Pandas e Spark, e gravação em Parquet particionado por `pub_year`/`pub_month`/`uf_sigla` com demonstração de *partition pruning* (`inspect`).
4. **Etapa 4 (Orquestração com Apache Airflow) - CONCLUÍDA:** Apache Airflow 3.1.6 em Docker (`LocalExecutor`, `api-server`, `scheduler`, `dag-processor`), DAG `pncp_contratacoes_pipeline` usando `airflow.sdk`, Dynamic Task Mapping por modalidade (`[6, 8]`), retries com backoff exponencial, `execution_timeout`, passagem de janela por `data_interval_start`/`conf` e validações de qualidade em `common/quality.py`.
5. **Etapa 5 (Qualidade, Observabilidade e Operação) - CONCLUÍDA:** Logging estruturado em JSON (`common/logging.py`), checks de qualidade entre Silver, Gold e PostgreSQL (`common/quality.py` com CLI), testes de integração contra PostgreSQL real e PySpark, guia operacional de reprocessamento (`docs/reprocessamento.md`) e CI no GitHub Actions (`.github/workflows/ci.yml`).
6. **Etapa 6 (Evolução — Delta Lake, IBGE e Databricks) - CONCLUÍDA:** Camadas Silver e Gold em Delta Lake (`delta_jobs.py` com `MERGE` monotônico e *time travel* via `delta`/`delta-history`), enriquecimento municipal via API do IBGE (`ibge_client.py` + `dim_municipio_populacao` + `sql/04_gasto_per_capita_municipio.sql`), notebook pronto para Databricks Free Edition Serverless (`databricks/pncp_gold_delta.ipynb`) e arquitetura de referência em nuvem (`docs/databricks_e_cloud.md`).

## 3. Tecnologias e Ferramentas (Stack)

* **Linguagem Core (Python 3.12):** Cliente HTTP (`httpx`), resiliência (`tenacity`), configuração por ambiente (`python-dotenv`), driver PostgreSQL (`psycopg`), testes (`pytest` + `respx`) e lint (`ruff`).
* **Processamento Distribuído (PySpark 3.5 + Pandas + Delta Lake 3.3 — Etapas 3 e 6):** Baseline em Pandas e pipeline PySpark rodando em container Linux (`Dockerfile`) com schema explícito (`StructType`) e transações ACID via `delta-spark`.
* **Armazenamento Analítico (Parquet / Delta Lake — Etapas 3 e 6):** Formato colunar particionado (`Snappy`) com *partition pruning* na leitura e `_delta_log/` para `MERGE` e *time travel*.
* **Banco Relacional (PostgreSQL 16 — Etapa 2):** Persistência do modelo dimensional, snapshots de staging e tabela de controle `pipeline_runs`.
* **Orquestração e Infraestrutura (Apache Airflow 3.1.6 + Docker Compose — Etapas 2 e 4):** `LocalExecutor`, Task SDK (`airflow.sdk`), Dynamic Task Mapping e execution API em containers locais sem nuvem paga.

## 4. Estrutura do Projeto

```text
proj/
├── databricks/
│   └── pncp_gold_delta.ipynb        # Notebook Delta Silver/Gold para Databricks Free Edition
├── dags/
│   └── pncp_contratacoes_pipeline.py # DAG Airflow 3 (TaskFlow + Dynamic Task Mapping)
├── src/
│   └── pncp_pipeline/
│       ├── config.py                # Leitura e validação de variáveis de ambiente
│       ├── common/
│       │   ├── logging.py           # Logging estruturado em JSON (ou texto via LOG_FORMAT)
│       │   └── quality.py           # Validações de contagem, nulidade, unicidade e consistência
│       ├── ingestion/
│       │   ├── client.py            # Cliente HTTP, retry, paginação e validação do envelope
│       │   ├── ibge_client.py       # Coleta de população municipal (IBGE agregado 6579)
│       │   ├── bronze_writer.py     # Escrita atômica e envelope de metadados Bronze
│       │   └── __main__.py          # CLI da ingestão (python -m pncp_pipeline.ingestion)
│       ├── database/
│       │   ├── schema.sql           # DDL do modelo dimensional, staging e pipeline_runs
│       │   ├── loader.py            # Carga idempotente Bronze -> PostgreSQL
│       │   └── __main__.py          # CLI do banco (init-db, load, load-ibge, query)
│       └── transformations/
│           ├── pandas_baseline.py   # Baseline Silver/Gold em Pandas + PyArrow Parquet
│           ├── spark_jobs.py        # Pipeline PySpark com StructType, quarentena e pruning
│           ├── delta_jobs.py        # Silver/Gold em Delta Lake (MERGE e Time Travel)
│           └── __main__.py          # CLI (pandas, spark, delta, delta-history, inspect)
├── scripts/
│   └── postgres_init.sql            # Cria o banco 'airflow' na inicialização do Postgres
├── sql/                             # Consultas analíticas (JOIN, CTE, janelas, gasto per capita)
├── docs/
│   ├── modelagem.md                 # Decisões de granularidade, tipos e idempotência
│   ├── reprocessamento.md           # Guia de backfill e reexecução idempotente
│   └── databricks_e_cloud.md        # Guia Databricks Free Edition e arquitetura Azure/GCP
├── tests/
│   ├── unit/
│   │   ├── test_bronze_writer.py    # Gravação Bronze, HTTP 204 e atomicidade em falha
│   │   ├── test_cli.py              # Validação de datas e flags da CLI
│   │   ├── test_client.py           # Mocks HTTP (200, 204, 400, 429, 5xx, timeout, paginação)
│   │   ├── test_config.py           # Overrides de ambiente e limites do PNCP
│   │   ├── test_database_loader.py  # Mapeamento Bronze->relacional e deduplicação
│   │   ├── test_ibge_client.py      # Parsing de população IBGE e escrita Bronze atômica
│   │   ├── test_logging.py          # Formato JSON estruturado e captura de exceções
│   │   ├── test_pandas_baseline.py  # Regras de qualidade, quarentena e Parquet em Pandas
│   │   └── test_quality.py          # Testes unitários dos checks de qualidade Silver/Gold/DB
│   └── integration/
│       ├── test_delta_pipeline.py   # Delta MERGE monotônico e Time Travel (versionAsOf)
│       ├── test_postgres_loader.py  # Carga idempotente, guarda monotônica e QA no Postgres real
│       └── test_spark_pipeline.py   # Paridade Pandas vs PySpark e PartitionFilters Parquet
├── .github/workflows/ci.yml         # CI: ruff + unit tests + integration (Postgres + Spark)
├── Dockerfile                       # Imagem Linux com OpenJDK 17 + PySpark para o container spark
├── Dockerfile.airflow               # Imagem Airflow 3.1.6 com OpenJDK 17 + PySpark + pncp-pipeline
├── docker-compose.yml               # PostgreSQL 16, Airflow 3 (LocalExecutor) e runner Spark
├── data/                            # Dados locais gerados (ignorado pelo Git)
│   └── raw/                         # Camada Bronze particionada por endpoint/modalidade/período
├── .env.example                     # Modelo de variáveis de ambiente sem segredos
├── .gitignore
├── inicio.md                        # Especificação inicial e diretrizes do projeto
└── pyproject.toml                   # Dependências e configuração de pytest / ruff
```

## 5. Arquitetura e Fluxos

O pipeline opera de ponta a ponta da coleta na API do PNCP até a validação final no PostgreSQL e nas camadas Silver/Gold em Parquet, orquestrado pelo Airflow 3.

### Fluxo do Pipeline (Etapas 1 a 6 ativas)
```mermaid
flowchart TD
    API[API PNCP v1<br/>publicacao / atualizacao] -->|Etapa 1: httpx + tenacity| B[(Camada Bronze<br/>data/raw JSON + metadata)]
    B -->|Etapa 3: PySpark| S[(Camada Silver<br/>Parquet limpo e deduplicado)]
    S -->|Etapa 3: PySpark| G[(Camada Gold<br/>Agregações UF / Órgão / Mês)]
    S -->|Etapa 2: Upsert| PG[(PostgreSQL<br/>fact_contratacao & dimensões)]
    G -->|Etapa 2: Upsert| PG
    AF{{Etapa 4: Apache Airflow}} -.orquestra.-> API
    AF -.-> S
    AF -.-> PG
```

## 6. Execução e Variáveis de Ambiente

Todas as configurações da ingestão são controladas por variáveis de ambiente (com valores padrão seguros no `config.py`).

### Variáveis de Ambiente Necessárias (`.env.example`)
* `PNCP_BASE_URL`: URL base da API de consulta (padrão: `https://pncp.gov.br/api/consulta/v1`).
* `PNCP_PAGE_SIZE`: Tamanho da página por requisição, entre `10` e `50` conforme OpenAPI do PNCP (padrão: `50`).
* `PNCP_REQUEST_TIMEOUT_S`: Timeout HTTP em segundos (padrão: `60`).
* `PNCP_MAX_RETRIES`: Máximo de tentativas em erros transitórios (padrão: `5`).
* `PNCP_USER_AGENT`: Identificação enviada no header `User-Agent`.
* `RAW_DATA_DIR`, `SILVER_DATA_DIR`, `GOLD_DATA_DIR`: Diretórios locais das camadas Bronze, Silver e Gold (`data/raw`, `data/processed`, `data/gold`).
* `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_PORT`: Credenciais e porta do container PostgreSQL no `docker-compose.yml`.
* `DATABASE_URL`: String de conexão usada pelo loader e consultas (padrão: `postgresql://pncp:pncp@localhost:5432/pncp`).
* `AIRFLOW_PORT`, `AIRFLOW_JWT_SECRET`: Porta da UI do Airflow (`8080`) e segredo JWT compartilhado entre `api-server`, `scheduler` e `dag-processor`.
* `LOG_LEVEL`, `LOG_FORMAT`: Nível (`INFO`) e formato de logs (`json` ou `text`).

### Passo a Passo

1. **Preparar ambiente Python e subir PostgreSQL + Airflow 3:**
    ```powershell
    python -m venv .venv
    .\.venv\Scripts\Activate.ps1
    pip install -e ".[dev]"
    Copy-Item .env.example .env
    docker-compose up -d --build --wait
    ```
    Acesse a UI do Airflow em `http://localhost:8080` (`SimpleAuthManager` em modo local admin) e dispare a DAG `pncp_contratacoes_pipeline`, ou use a CLI no container:
    ```powershell
    docker exec pncp-airflow-scheduler airflow dags unpause pncp_contratacoes_pipeline
    docker exec pncp-airflow-scheduler airflow dags list-runs pncp_contratacoes_pipeline
    ```

2. **Executar a ingestão Bronze (exemplo com limite de páginas):**
    ```powershell
    python -m pncp_pipeline.ingestion --start 2024-01-02 --end 2024-01-02 --modality 6 --max-pages 2
    ```
    *Opções:* `--endpoint` (`contratacoes/publicacao` ou `contratacoes/atualizacao`), `--modality` (`6` Pregão Eletrônico, `8` Dispensa, `4` Concorrência, `9` Inexigibilidade), `--uf` e `--max-pages`.

3. **Carregar dados (PNCP + IBGE) no PostgreSQL e executar consultas analíticas:**
    ```powershell
    python -m pncp_pipeline.ingestion.ibge_client
    python -m pncp_pipeline.database load
    python -m pncp_pipeline.database load-ibge
    python -m pncp_pipeline.database query sql/01_gastos_por_uf_e_modalidade.sql
    python -m pncp_pipeline.database query sql/02_deduplicacao_snapshot.sql
    python -m pncp_pipeline.database query sql/03_ranking_orgaos_e_variacao.sql
    python -m pncp_pipeline.database query sql/04_gasto_per_capita_municipio.sql
    ```

4. **Executar transformações Bronze → Silver → Gold (Parquet e Delta Lake):**
    ```powershell
    # Baseline Pandas local (Windows/Linux)
    python -m pncp_pipeline.transformations pandas

    # Pipeline PySpark + Delta Lake em container Linux
    docker-compose --profile spark build spark
    docker-compose --profile spark run --rm spark python -m pncp_pipeline.transformations spark
    docker-compose --profile spark run --rm spark python -m pncp_pipeline.transformations inspect --uf SP
    docker-compose --profile spark run --rm spark python -m pncp_pipeline.transformations delta
    docker-compose --profile spark run --rm spark python -m pncp_pipeline.transformations delta-history
    ```

5. **Rodar checks de qualidade, lint e testes:**
    ```powershell
    python -m pncp_pipeline.common.quality
    ruff check src dags tests
    pytest -v
    docker-compose --profile spark run --rm spark pytest -v
    ```

*Validação real executada até a Etapa 6: Airflow 3 (`2` DAG runs com `success`), `616` contratações únicas em `fact_contratacao`, `5.571` municípios do IBGE em `dim_municipio_populacao`, tabela Silver/Gold em Delta Lake (`delta_jobs.py` com `MERGE` e `versionAsOf`) e `42` testes passando no container `spark` + integração PostgreSQL real.*

## 7. Motivação e Escolhas Arquiteturais (Trade-offs)

* **Tratamento Estrito de Contrato da API PNCP:** A especificação OpenAPI (`/api/consulta/v3/api-docs`) e testes reais mostraram que `tamanhoPagina > 50` retorna `HTTP 400` e páginas vazias/além do fim retornam `HTTP 204` sem corpo. Por isso, `Settings` valida `10 <= page_size <= 50` na inicialização, erros `4xx` falham rápido sem desperdício de retries (`PncpRequestError`), e apenas rede/`429`/`5xx`/payload corrompido acionam backoff exponencial.
* **Escrita Atômica e Envelope Bronze Imutável:** Cada página é gravada em arquivo temporário no mesmo diretório e promovida via `os.replace` (`write_json_atomic`), garantindo que quedas no meio da execução não deixem JSONs truncados em disco. O nome do arquivo inclui timestamp UTC e `run_id` para nunca sobrescrever coletas anteriores.
* **PySpark em Container Linux (em vez de nativo no Windows):** O Spark no Windows exige binários nativos do Hadoop (`winutils.exe`) para gravar Parquet sem falhas. Para manter o ambiente previsível e próximo de produção, o Windows executa a ingestão leve e o baseline Pandas, enquanto o Spark rodará em container Linux via Docker Compose.
* **Escolha do Domínio (PNCP em vez de dados sintéticos):** Contratações públicas reais sofrem retificações após a publicação (`dataAtualizacao > dataInclusao`), oferecendo um caso de uso concreto para deduplicação por janela na Silver e `MERGE` no Delta Lake, além de cruzamentos naturais por `codigoIbge` e `cnpj`.

## 8. Próximos Passos (Backlog Futuro)

* **Execução no Databricks Free Edition:** Subir o Parquet Silver para um Unity Catalog Volume e executar [databricks/pncp_gold_delta.ipynb](databricks/pncp_gold_delta.ipynb) seguindo [docs/databricks_e_cloud.md](docs/databricks_e_cloud.md).
* **Evoluções Opcionais:** Enriquecimento temporal com PTAX/Selic (BCB) e painel analítico (Metabase / Superset) sobre o PostgreSQL.

## 9. Evidências de Execução (Saídas Reais)

Registros reais capturados após a execução completa do pipeline em ambiente local (Docker + PostgreSQL 16 + Airflow 3.1.6 + PySpark 3.5 + Delta Lake 3.3):

<details>
<summary><strong>1. Airflow 3 — Duas DAG runs concluídas com <code>success</code> em todas as tarefas</strong></summary>

```text
dag_id                     | run_id                                   | state   | start_date                       | end_date
===========================+==========================================+=========+==================================+==================================
pncp_contratacoes_pipeline | manual__2026-11-23T13:19:37.864438+00:00 | success | 2026-11-23T13:23:41.753898+00:00 | 2026-11-23T13:24:31.796248+00:00
pncp_contratacoes_pipeline | scheduled__2026-11-23T00:00:00+00:00     | success | 2026-11-23T13:18:47.586115+00:00 | 2026-11-23T13:23:40.716058+00:00
```

Estados das tarefas na execução `manual__2026-11-23T13:19:37.864438+00:00` (Dynamic Task Mapping nas modalidades `6` e `8`):

```text
task_id                | state   | map_index
=======================+=========+==========
resolve_window         | success |
list_ingestion_targets | success |
ingest_modality        | success | 0
ingest_modality        | success | 1
transform_silver_gold  | success |
load_postgres          | success |
data_quality_checks    | success |
```

</details>

<details>
<summary><strong>2. Parquet — Consolidação de partições e <em>Partition Pruning</em> no plano físico do Spark</strong></summary>

```text
Arquivos Parquet em /app/data/processed/contratacoes: 19
Total registros para uf_sigla='SP': 17
== Physical Plan ==
* ColumnarToRow (2)
+- Scan parquet  (1)

(1) Scan parquet
Location: InMemoryFileIndex [file:/app/data/processed/contratacoes]
PartitionFilters: [isnotnull(uf_sigla#28), (uf_sigla#28 = SP)]
```

</details>

<details>
<summary><strong>3. Delta Lake — <code>WRITE</code> inicial (v0) e <code>MERGE</code> idempotente (v1)</strong></summary>

```text
{"timestamp": "2026-11-24T13:29:16.434015+00:00", "level": "INFO", "logger": "__main__", "message": "delta_pipeline_concluido", "files": 13, "rows_in": 616, "rows_silver_batch": 616, "total_silver_delta": 616, "gold_rows": 78, "delta_version": 1}

v1 | 2026-11-24 13:29:09.718000 | MERGE | {'numOutputRows': '616', 'numTargetRowsInserted': '0', 'numTargetRowsUpdated': '616', ...}
v0 | 2026-11-24 13:26:57.029000 | WRITE | {'numOutputRows': '616', 'numOutputBytes': '620251', 'numFiles': '51'}
```

</details>

<details>
<summary><strong>4. PostgreSQL + IBGE — Cruzamento per capita (<code>5.571</code> municípios) e Quality Gate JSON</strong></summary>

```text
codigo_ibge | municipio_nome     | uf_sigla | ano_populacao | populacao | qtd_contratacoes | total_estimado | total_homologado | gasto_estimado_per_capita | gasto_homologado_per_capita
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
4200903     | Angelina           | SC       | 2026          | 5489      | 1                | 4197821.00     | 0.00             | 764.77                    | 0.00
4101853     | Ariranha do Ivaí   | PR       | 2026          | 2346      | 1                | 1740000.00     | 0.00             | 741.69                    | 0.00
3127370     | Goiabeira          | MG       | 2026          | 2849      | 1                | 1991379.20     | 0.00             | 698.97                    | 0.00
3153707     | Quartel Geral      | MG       | 2026          | 3227      | 1                | 1553461.80     | 1503700.00       | 481.40                    | 465.97
2313005     | Solonópole         | CE       | 2026          | 18831     | 1                | 9015254.48     | 0.00             | 478.75                    | 0.00
```

Check automatizado (`python -m pncp_pipeline.common.quality`):

```json
{"timestamp": "2026-11-24T12:57:49.396953+00:00", "level": "INFO", "logger": "__main__", "message": "quality_checks_passed", "silver_rows": 616, "gold_rows": 616, "fact_rows": 616, "duplicate_keys": 0, "null_required_fields": 0, "negative_values": 0}
```

</details>

<details>
<summary><strong>5. Suíte de Testes (Lint, Unitários e Integração)</strong></summary>

```text
ruff check --no-cache src dags tests                                -> All checks passed!
pytest -q -p no:cacheprovider tests/unit                            -> 40 passed in 4.22s
pytest -q -p no:cacheprovider tests/integration/test_postgres_loader.py -> 1 passed in 1.99s
docker-compose --profile spark run --rm spark pytest ... (42 testes) -> 42 passed in 31.77s
```

</details>
