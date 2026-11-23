-- Agregação por UF e modalidade com JOIN nas dimensões e participação (%) via função de janela.
SELECT
    COALESCE(m.uf_sigla, f.uf_sigla, 'ND') AS uf_sigla,
    mod.modalidade_nome,
    COUNT(*) AS total_contratacoes,
    COUNT(f.valor_total_homologado) AS qtd_homologadas,
    ROUND(COALESCE(SUM(f.valor_total_estimado), 0), 2) AS total_estimado,
    ROUND(COALESCE(SUM(f.valor_total_homologado), 0), 2) AS total_homologado,
    ROUND(
        100.0 * COALESCE(SUM(f.valor_total_estimado), 0)
        / NULLIF(SUM(COALESCE(SUM(f.valor_total_estimado), 0)) OVER (), 0),
        2
    ) AS pct_estimado_nacional
FROM fact_contratacao AS f
JOIN dim_modalidade AS mod
    ON mod.modalidade_id = f.modalidade_id
LEFT JOIN dim_municipio AS m
    ON m.codigo_ibge = f.codigo_ibge
GROUP BY
    COALESCE(m.uf_sigla, f.uf_sigla, 'ND'),
    mod.modalidade_nome
ORDER BY total_estimado DESC
LIMIT 20;
