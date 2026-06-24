-- ======================================================================
-- Consultas analiticas de exemplo sobre o Star Schema (camada Gold).
--
-- Dialeto: Spark SQL. As tabelas Delta da Gold ficam em s3a://gold/<obj>/.
-- Antes de rodar, registre cada Delta como view temporaria, por exemplo:
--
--   spark.read.format("delta").load("s3a://gold/dim_streamer") \
--        .createOrReplaceTempView("dim_streamer")
--   ... (idem para as demais dimensoes e fatos) ...
--
-- Depois, execute qualquer consulta abaixo com spark.sql(...).
-- ======================================================================


-- 1) Top 10 streamers por valor total arrecadado em doacoes.
SELECT
    s.nome_streamer,
    s.nome_plataforma,
    ROUND(SUM(f.valor), 2) AS total_doacoes,
    COUNT(*)               AS qtd_doacoes
FROM fato_doacoes f
JOIN dim_streamer s ON s.sk_streamer = f.sk_streamer
GROUP BY s.nome_streamer, s.nome_plataforma
ORDER BY total_doacoes DESC
LIMIT 10;


-- 2) Receita de doacoes por mes (serie temporal).
SELECT
    t.ano,
    t.mes,
    t.nome_mes,
    ROUND(SUM(f.valor), 2) AS receita_doacoes
FROM fato_doacoes f
JOIN dim_tempo t ON t.sk_tempo = f.sk_tempo
GROUP BY t.ano, t.mes, t.nome_mes
ORDER BY t.ano, t.mes;


-- 3) Jogos mais transmitidos e seu pico medio de audiencia.
SELECT
    j.nome_jogo,
    COUNT(*)                       AS qtd_transmissoes,
    ROUND(AVG(f.pico_viewers), 1)  AS pico_medio,
    ROUND(AVG(f.duracao_minutos), 1) AS duracao_media_min
FROM fato_transmissoes f
JOIN dim_jogo j ON j.sk_jogo = f.sk_jogo
GROUP BY j.nome_jogo
ORDER BY qtd_transmissoes DESC
LIMIT 15;


-- 4) Minutos assistidos por pais do espectador.
SELECT
    v.pais,
    SUM(f.minutos_assistidos) AS minutos_totais,
    COUNT(DISTINCT f.sk_viewer) AS viewers_distintos
FROM fato_visualizacoes f
JOIN dim_viewer v ON v.sk_viewer = f.sk_viewer
GROUP BY v.pais
ORDER BY minutos_totais DESC;


-- 5) Assinaturas ativas por tier (tipo) e receita mensal recorrente (MRR).
SELECT
    f.tipo,
    COUNT(*)                      AS assinaturas_ativas,
    ROUND(SUM(f.valor_mensal), 2) AS mrr
FROM fato_assinaturas f
WHERE f.ativa = TRUE
GROUP BY f.tipo
ORDER BY mrr DESC;


-- 6) Engajamento por streamer: cruza audiencia (visualizacoes) com monetizacao
--    (doacoes) usando as dimensoes compartilhadas (conformed dimensions).
SELECT
    s.nome_streamer,
    SUM(vis.minutos_assistidos)              AS minutos_assistidos,
    ROUND(COALESCE(SUM(doa.valor), 0), 2)    AS total_doacoes
FROM dim_streamer s
LEFT JOIN fato_visualizacoes vis ON vis.sk_streamer = s.sk_streamer
LEFT JOIN fato_doacoes       doa ON doa.sk_streamer = s.sk_streamer
WHERE s.sk_streamer <> -1
GROUP BY s.nome_streamer
ORDER BY total_doacoes DESC
LIMIT 20;


-- 7) Auditoria de integridade referencial: quantas linhas de fato cairam no
--    membro desconhecido (-1) de cada dimensao. Idealmente, tudo zero.
SELECT 'fato_doacoes'  AS fato, 'sk_viewer'   AS dim, COUNT(*) AS orfas FROM fato_doacoes      WHERE sk_viewer   = -1
UNION ALL SELECT 'fato_doacoes',       'sk_streamer', COUNT(*) FROM fato_doacoes      WHERE sk_streamer = -1
UNION ALL SELECT 'fato_transmissoes',  'sk_streamer', COUNT(*) FROM fato_transmissoes WHERE sk_streamer = -1
UNION ALL SELECT 'fato_transmissoes',  'sk_jogo',     COUNT(*) FROM fato_transmissoes WHERE sk_jogo     = -1
UNION ALL SELECT 'fato_visualizacoes', 'sk_viewer',   COUNT(*) FROM fato_visualizacoes WHERE sk_viewer  = -1
UNION ALL SELECT 'fato_assinaturas',   'sk_streamer', COUNT(*) FROM fato_assinaturas   WHERE sk_streamer = -1;
