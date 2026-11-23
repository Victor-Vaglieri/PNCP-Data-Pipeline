CREATE TABLE IF NOT EXISTS dim_orgao (
    cnpj TEXT PRIMARY KEY,
    razao_social TEXT NOT NULL,
    poder_id TEXT,
    esfera_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_municipio (
    codigo_ibge TEXT PRIMARY KEY,
    municipio_nome TEXT NOT NULL,
    uf_sigla CHAR(2) NOT NULL,
    uf_nome TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_modalidade (
    modalidade_id INTEGER PRIMARY KEY,
    modalidade_nome TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fact_contratacao (
    numero_controle_pncp TEXT PRIMARY KEY,
    orgao_cnpj TEXT NOT NULL REFERENCES dim_orgao(cnpj),
    codigo_ibge TEXT REFERENCES dim_municipio(codigo_ibge),
    modalidade_id INTEGER NOT NULL REFERENCES dim_modalidade(modalidade_id),
    codigo_unidade TEXT,
    nome_unidade TEXT,
    uf_sigla CHAR(2),
    ano_compra INTEGER NOT NULL,
    sequencial_compra INTEGER NOT NULL,
    numero_compra TEXT,
    processo TEXT,
    objeto_compra TEXT,
    srp BOOLEAN,
    modo_disputa_id INTEGER,
    modo_disputa_nome TEXT,
    situacao_compra_id INTEGER,
    situacao_compra_nome TEXT,
    amparo_legal_codigo INTEGER,
    amparo_legal_nome TEXT,
    valor_total_estimado NUMERIC(18, 2),
    valor_total_homologado NUMERIC(18, 2),
    data_inclusao TIMESTAMP,
    data_publicacao_pncp TIMESTAMP,
    data_abertura_proposta TIMESTAMP,
    data_encerramento_proposta TIMESTAMP,
    data_atualizacao TIMESTAMP,
    data_atualizacao_global TIMESTAMP,
    source_file TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stg_contratacao_snapshot (
    numero_controle_pncp TEXT NOT NULL,
    run_id TEXT NOT NULL,
    orgao_cnpj TEXT NOT NULL,
    codigo_ibge TEXT,
    uf_sigla CHAR(2),
    modalidade_id INTEGER NOT NULL,
    valor_total_estimado NUMERIC(18, 2),
    valor_total_homologado NUMERIC(18, 2),
    data_publicacao_pncp TIMESTAMP,
    data_atualizacao TIMESTAMP,
    ingested_at TIMESTAMPTZ NOT NULL,
    source_file TEXT NOT NULL,
    PRIMARY KEY (numero_controle_pncp, run_id)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    duration_s DOUBLE PRECISION,
    rows_in INTEGER NOT NULL DEFAULT 0,
    rows_out INTEGER NOT NULL DEFAULT 0,
    details JSONB,
    PRIMARY KEY (run_id, stage)
);

CREATE INDEX IF NOT EXISTS idx_fact_contratacao_pub_uf
    ON fact_contratacao (data_publicacao_pncp, uf_sigla);

CREATE INDEX IF NOT EXISTS idx_fact_contratacao_orgao
    ON fact_contratacao (orgao_cnpj);

CREATE INDEX IF NOT EXISTS idx_fact_contratacao_modalidade
    ON fact_contratacao (modalidade_id);

CREATE INDEX IF NOT EXISTS idx_stg_snapshot_dedup
    ON stg_contratacao_snapshot (numero_controle_pncp, data_atualizacao DESC, ingested_at DESC);
