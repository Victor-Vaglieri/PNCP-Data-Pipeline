-- Tratamento de duplicidades sobre snapshots ingeridos usando CTE + ROW_NUMBER().
-- Mantém apenas a versão mais recente por numero_controle_pncp (data_atualizacao DESC, ingested_at DESC).
WITH ranked_snapshots AS (
    SELECT
        s.numero_controle_pncp,
        s.run_id,
        s.orgao_cnpj,
        s.uf_sigla,
        s.valor_total_estimado,
        s.valor_total_homologado,
        s.data_atualizacao,
        s.ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY s.numero_controle_pncp
            ORDER BY s.data_atualizacao DESC NULLS LAST, s.ingested_at DESC
        ) AS rn,
        COUNT(*) OVER (PARTITION BY s.numero_controle_pncp) AS total_versoes
    FROM stg_contratacao_snapshot AS s
)
SELECT
    numero_controle_pncp,
    run_id AS run_vencedor,
    orgao_cnpj,
    uf_sigla,
    valor_total_estimado,
    valor_total_homologado,
    data_atualizacao,
    total_versoes
FROM ranked_snapshots
WHERE rn = 1
ORDER BY total_versoes DESC, data_atualizacao DESC NULLS LAST
LIMIT 20;
