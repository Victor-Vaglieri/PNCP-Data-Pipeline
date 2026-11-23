# PNCP Data Pipeline

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
5. **Etapa 5 (Qualidade, Observabilidade e Operação) - PLANEJADA:** Testes de integração automatizados contra PostgreSQL/Spark no CI, logging estruturado em JSON, guia de reprocessamento idempotente e workflow no GitHub Actions.
6. **Etapa 6 (Evolução — Delta Lake e Databricks) - PLANEJADA:** Migração do Silver para Delta Lake local (`MERGE` e *time travel*), processamento Gold em notebook no Databricks Free Edition via Volumes e enriquecimento opcional com dados do IBGE (população por `codigoIbge`) e BCB (PTAX/SGS).

## 3. Tecnologias e Ferramentas (Stack)

* **Linguagem Core (Python 3.12):** Cliente HTTP (`httpx`), resiliência (`tenacity`), configuração por ambiente (`python-dotenv`), driver PostgreSQL (`psycopg`), testes (`pytest` + `respx`) e lint (`ruff`).
* **Processamento Distribuído (PySpark 3.5 + Pandas — Etapa 3):** Baseline em Pandas e pipeline PySpark rodando em container Linux (`Dockerfile`) com schema explícito (`StructType`), sem depender de `winutils.exe` no Windows.
* **Armazenamento Analítico (Parquet / Delta Lake — Etapas 3 e 6):** Formato colunar particionado (`Snappy`) com *partition pruning* na leitura e evolução futura para transações ACID via `_delta_log/`.
* **Banco Relacional (PostgreSQL 16 — Etapa 2):** Persistência do modelo dimensional, snapshots de staging e tabela de controle `pipeline_runs`.
* **Orquestração e Infraestrutura (Apache Airflow 3.1.6 + Docker Compose — Etapas 2 e 4):** `LocalExecutor`, Task SDK (`airflow.sdk`), Dynamic Task Mapping e execution API em containers locais sem nuvem paga.

## 4. Estrutura do Projeto

```text
proj/
├── dags/
│   └── pncp_contratacoes_pipeline.py # DAG Airflow 3 (TaskFlow + Dynamic Task Mapping)
├── src/
│   └── pncp_pipeline/
│       ├── config.py                # Leitura e validação de variáveis de ambiente
│       ├── common/
│       │   └── quality.py           # Validações de contagem, duplicidade e valores negativos
│       ├── ingestion/
│       │   ├── client.py            # Cliente HTTP, retry, paginação e validação do envelope
│       │   ├── bronze_writer.py     # Escrita atômica e envelope de metadados Bronze
│       │   └── __main__.py          # CLI da ingestão (python -m pncp_pipeline.ingestion)
│       ├── database/
│       │   ├── schema.sql           # DDL do modelo dimensional, staging e pipeline_runs
│       │   ├── loader.py            # Carga idempotente Bronze -> PostgreSQL
│       │   └── __main__.py          # CLI do banco (init-db, load, query)
│       └── transformations/
│           ├── pandas_baseline.py   # Baseline Silver/Gold em Pandas + PyArrow Parquet
│           ├── spark_jobs.py        # Pipeline PySpark com StructType, quarentena e pruning
│           └── __main__.py          # CLI de transformações (pandas, spark, inspect)
├── scripts/
│   └── postgres_init.sql            # Cria o banco 'airflow' na inicialização do Postgres
├── sql/                             # Consultas analíticas (JOIN, CTE, ROW_NUMBER, DENSE_RANK, LAG)
├── docs/
│   └── modelagem.md                 # Decisões de granularidade, tipos e idempotência
├── tests/
│   ├── unit/
│   │   ├── test_bronze_writer.py    # Gravação Bronze, HTTP 204 e atomicidade em falha
│   │   ├── test_cli.py              # Validação de datas e flags da CLI
│   │   ├── test_client.py           # Mocks HTTP (200, 204, 400, 429, 5xx, timeout, paginação)
│   │   ├── test_config.py           # Overrides de ambiente e limites do PNCP
│   │   ├── test_database_loader.py  # Mapeamento Bronze->relacional e deduplicação
│   │   ├── test_pandas_baseline.py  # Regras de qualidade, quarentena e Parquet em Pandas
│   │   └── test_quality.py          # Testes unitários dos checks de qualidade Silver/DB
│   └── integration/
│       └── test_spark_pipeline.py   # Paridade Pandas vs PySpark e PartitionFilters Parquet
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

Na **Etapa 1** já opera o fluxo de ingestão até a camada Bronze; as camadas Silver, Gold, PostgreSQL e Airflow compõem a arquitetura-alvo das próximas etapas.

### Fluxo do Pipeline (Etapa 1 ativa + Evolução Alvo)
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

3. **Carregar dados no PostgreSQL e executar consultas analíticas:**
    ```powershell
    python -m pncp_pipeline.database load
    python -m pncp_pipeline.database query sql/01_gastos_por_uf_e_modalidade.sql
    python -m pncp_pipeline.database query sql/02_deduplicacao_snapshot.sql
    python -m pncp_pipeline.database query sql/03_ranking_orgaos_e_variacao.sql
    ```

4. **Executar transformações Bronze → Silver → Gold (Parquet) e inspecionar *partition pruning*:**
    ```powershell
    # Baseline Pandas local (Windows/Linux)
    python -m pncp_pipeline.transformations pandas

    # Pipeline PySpark em container Linux (recomendado no Windows)
    docker-compose --profile spark build
    docker-compose --profile spark run --rm spark python -m pncp_pipeline.transformations spark
    docker-compose --profile spark run --rm spark python -m pncp_pipeline.transformations inspect --uf SP
    ```

5. **Rodar lint e testes (unitários locais e integração PySpark no container):**
    ```powershell
    ruff check src tests
    pytest -v
    docker-compose --profile spark run --rm spark pytest -v
    ```

*Validação real executada até a Etapa 4: duas execuções completas da DAG `pncp_contratacoes_pipeline` no Airflow 3 (`scheduled__2026-11-23T00:00:00+00:00` e `manual__2026-11-23T13:19:37.864438+00:00`) concluídas com `success` em todas as tarefas (`ingest_modality`, `transform_silver_gold`, `load_postgres`, `data_quality_checks`), totalizando `616` contratações únicas em `fact_contratacao`.*

## 7. Motivação e Escolhas Arquiteturais (Trade-offs)

* **Tratamento Estrito de Contrato da API PNCP:** A especificação OpenAPI (`/api/consulta/v3/api-docs`) e testes reais mostraram que `tamanhoPagina > 50` retorna `HTTP 400` e páginas vazias/além do fim retornam `HTTP 204` sem corpo. Por isso, `Settings` valida `10 <= page_size <= 50` na inicialização, erros `4xx` falham rápido sem desperdício de retries (`PncpRequestError`), e apenas rede/`429`/`5xx`/payload corrompido acionam backoff exponencial.
* **Escrita Atômica e Envelope Bronze Imutável:** Cada página é gravada em arquivo temporário no mesmo diretório e promovida via `os.replace` (`write_json_atomic`), garantindo que quedas no meio da execução não deixem JSONs truncados em disco. O nome do arquivo inclui timestamp UTC e `run_id` para nunca sobrescrever coletas anteriores.
* **PySpark em Container Linux (em vez de nativo no Windows):** O Spark no Windows exige binários nativos do Hadoop (`winutils.exe`) para gravar Parquet sem falhas. Para manter o ambiente previsível e próximo de produção, o Windows executa a ingestão leve e o baseline Pandas, enquanto o Spark rodará em container Linux via Docker Compose.
* **Escolha do Domínio (PNCP em vez de dados sintéticos):** Contratações públicas reais sofrem retificações após a publicação (`dataAtualizacao > dataInclusao`), oferecendo um caso de uso concreto para deduplicação por janela na Silver e `MERGE` no Delta Lake, além de cruzamentos naturais por `codigoIbge` e `cnpj`.

## 8. Próximos Passos (Backlog Futuro)

* **Etapa 5 (Qualidade, Observabilidade e CI):** Testes de integração automatizados no GitHub Actions, logging estruturado em JSON e guia operacional de reprocessamento/backfill.
* **Etapa 6 (Delta Lake e Databricks):** Adotar `MERGE` e *time travel* no Delta Lake local, publicar notebook Gold no Databricks Free Edition e enriquecer municípios com IBGE/BCB.
