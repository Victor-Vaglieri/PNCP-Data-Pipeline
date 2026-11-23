-- Ranking de órgãos por UF (DENSE_RANK) e variação em relação ao líder/anterior via LAG().
WITH totais_orgao AS (
    SELECT
        COALESCE(f.uf_sigla, 'ND') AS uf_sigla,
        o.cnpj,
        o.razao_social,
        COUNT(*) AS qtd_compras,
        COALESCE(SUM(f.valor_total_estimado), 0) AS total_estimado,
        COALESCE(SUM(f.valor_total_homologado), 0) AS total_homologado
    FROM fact_contratacao AS f
    JOIN dim_orgao AS o
        ON o.cnpj = f.orgao_cnpj
    GROUP BY
        COALESCE(f.uf_sigla, 'ND'),
        o.cnpj,
        o.razao_social
),
ranqueado AS (
    SELECT
        uf_sigla,
        cnpj,
        razao_social,
        qtd_compras,
        ROUND(total_estimado, 2) AS total_estimado,
        ROUND(total_homologado, 2) AS total_homologado,
        DENSE_RANK() OVER (
            PARTITION BY uf_sigla
            ORDER BY total_estimado DESC
        ) AS rank_na_uf,
        ROUND(
            LAG(total_estimado) OVER (
                PARTITION BY uf_sigla
                ORDER BY total_estimado DESC
            ) - total_estimado,
            2
        ) AS diferenca_para_acima
    FROM totais_orgao
)
SELECT
    uf_sigla,
    rank_na_uf,
    razao_social,
    qtd_compras,
    total_estimado,
    total_homologado,
    diferenca_para_acima
FROM ranqueado
WHERE rank_na_uf <= 3
ORDER BY uf_sigla, rank_na_uf;
