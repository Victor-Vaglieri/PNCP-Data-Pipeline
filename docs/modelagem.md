# Modelagem Relacional — PostgreSQL (Etapa 2)

## 1. Granularidade e Entidades

A tabela fato **`fact_contratacao`** representa a menor granularidade entregue pelo endpoint `/v1/contratacoes/publicacao`: **1 linha = 1 contratação pública**, identificada pela chave natural **`numero_controle_pncp`** (ex.: `87613071000148-1-000330/2023`).

### Dimensões
- **`dim_orgao`** (`cnpj` PK): órgão/entidade contratante (`razao_social`, `poder_id`, `esfera_id`). Um mesmo órgão pode publicar centenas de contratações ao longo do ano.
- **`dim_municipio`** (`codigo_ibge` PK): município da unidade executora (`municipio_nome`, `uf_sigla`, `uf_nome`). Mantida separada para permitir cruzamento futuro com indicadores demográficos do IBGE (população por `codigo_ibge`).
- **`dim_modalidade`** (`modalidade_id` PK): domínio de modalidades do PNCP (ex.: `6` Pregão Eletrônico, `8` Dispensa).

### Staging e Observabilidade
- **`stg_contratacao_snapshot`** (`(numero_controle_pncp, run_id)` PK): armazena cada snapshot ingerido sem sobrescrever execuções anteriores. Permite auditar quantas versões uma contratação recebeu e exercitar deduplicação via SQL (`ROW_NUMBER()`).
- **`pipeline_runs`** (`(run_id, stage)` PK): registra início, fim, duração, volume de entrada/saída e erro em JSONB por execução.

## 2. Decisões de Modelagem

1. **`codigo_ibge` anulável na fato:** Nem toda contratação tem município IBGE preenchido em `unidadeOrgao` (ex.: unidades federais no exterior). A FK para `dim_municipio` aceita `NULL`, evitando descartar registros válidos na carga.
2. **`uf_sigla` também na fato:** Mantida desnormalizada em `fact_contratacao` para acelerar filtros/agregações por UF sem obrigar `JOIN` quando `codigo_ibge` for nulo, além de servir ao índice composto `(data_publicacao_pncp, uf_sigla)`.
3. **Valores financeiros em `NUMERIC(18, 2)`:** Evita erro de arredondamento de ponto flutuante (`float`/`DOUBLE PRECISION`). `valor_total_homologado` é anulável porque licitações recém-publicadas ainda não foram homologadas.
4. **Timestamps sem fuso na fato (`TIMESTAMP`) vs. metadados com fuso (`TIMESTAMPTZ`):** A API do PNCP retorna datas locais sem offset (`2024-01-02T00:01:21`); já `ingested_at` e `loaded_at` usam `TIMESTAMPTZ` em UTC para rastreabilidade precisa.

## 3. Idempotência e Tratamento de Duplicidades

Reexecutar o loader sobre os mesmos arquivos Bronze (ou após nova coleta do mesmo período) não gera linhas duplicadas nem regressão de versão:
1. O loader mantém um checkpoint transacional em `pipeline_runs` (`stage = 'bronze_to_postgres_checkpoint'`) serializado via `pg_advisory_xact_lock`, processando apenas arquivos Bronze novos, modificados ou de backfill (com opção `full_refresh=True` / `PNCP_FULL_REFRESH=1` para reprocessamento total).
2. Dentro do lote lido do Bronze, `deduplicate_contratacoes` mantém apenas o registro de maior `(data_atualizacao, data_atualizacao_global, ingested_at)` para cada `numero_controle_pncp`.
3. No PostgreSQL, `INSERT ... ON CONFLICT (numero_controle_pncp) DO UPDATE` atualiza todos os campos mutáveis de `fact_contratacao` sob guarda monotônica por `(data_atualizacao, data_atualizacao_global, ingested_at)`:
   - Atualiza se `EXCLUDED.data_atualizacao` for mais recente (ou preencher um `NULL` existente);
   - Em empate de `data_atualizacao`, só atualiza se `data_atualizacao_global` for mais recente ou, empatando ambos, se `EXCLUDED.ingested_at >= fact_contratacao.ingested_at`.
   Se uma reexecução antiga tentar sobrescrever uma versão mais nova já gravada, o `UPDATE` é ignorado.
