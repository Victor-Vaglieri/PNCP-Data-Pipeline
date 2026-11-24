-- Gasto público estimado e homologado per capita cruzando PNCP com população municipal (IBGE).
SELECT
    m.codigo_ibge,
    m.municipio_nome,
    m.uf_sigla,
    p.ano AS ano_populacao,
    p.populacao,
    COUNT(*) AS qtd_contratacoes,
    ROUND(COALESCE(SUM(f.valor_total_estimado), 0), 2) AS total_estimado,
    ROUND(COALESCE(SUM(f.valor_total_homologado), 0), 2) AS total_homologado,
    ROUND(COALESCE(SUM(f.valor_total_estimado), 0) / NULLIF(p.populacao, 0), 2)
        AS gasto_estimado_per_capita,
    ROUND(COALESCE(SUM(f.valor_total_homologado), 0) / NULLIF(p.populacao, 0), 2)
        AS gasto_homologado_per_capita
FROM fact_contratacao AS f
JOIN dim_municipio AS m
    ON m.codigo_ibge = f.codigo_ibge
JOIN dim_municipio_populacao AS p
    ON p.codigo_ibge = f.codigo_ibge
GROUP BY
    m.codigo_ibge,
    m.municipio_nome,
    m.uf_sigla,
    p.ano,
    p.populacao
ORDER BY gasto_estimado_per_capita DESC NULLS LAST
LIMIT 20;
