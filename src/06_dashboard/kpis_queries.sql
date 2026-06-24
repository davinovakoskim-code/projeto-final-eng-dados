-- ======================================================================
-- Queries dos KPIs do Dashboard (One Page View) — Issue #30
--
-- Dialect: PostgreSQL (schema gold_analytics, exportado por gold_to_postgres.py)
-- Todas as tabelas: gold_analytics.<mart>
--
-- KPI 1 - Receita total: soma de doacoes + assinaturas, vs. periodo anterior
-- KPI 2 - Valor medio por doacao: receita_total / qtd_doacoes
-- KPI 3 - Numero de transmissoes: contagem total + tendencia mensal (3 anos)
-- KPI 4 - Viewers ativos: contagem distinta de viewers com visualizacao
-- ======================================================================


-- -----------------------------------------------------------------------
-- KPI 1 — Receita Total (global) + variacao % vs. periodo anterior
-- Card Metabase: numero / variacao automatica (comparar periodo)
-- Fonte: agg_receita_mensal
-- -----------------------------------------------------------------------

-- Receita total de todo o periodo (valor exibido no card)
SELECT
    ROUND(SUM(receita_total), 2) AS receita_total_global
FROM gold_analytics.agg_receita_mensal;


-- Receita por mes para calcular variacao % (ultimo mes vs. penultimo)
WITH mensal AS (
    SELECT
        ano,
        mes,
        receita_total,
        LAG(receita_total) OVER (ORDER BY ano, mes) AS receita_mes_anterior
    FROM gold_analytics.agg_receita_mensal
)
SELECT
    ano,
    mes,
    receita_total,
    receita_mes_anterior,
    CASE
        WHEN receita_mes_anterior IS NULL OR receita_mes_anterior = 0 THEN NULL
        ELSE ROUND(
            ((receita_total - receita_mes_anterior) / receita_mes_anterior) * 100,
            2
        )
    END AS variacao_pct
FROM mensal
ORDER BY ano DESC, mes DESC
LIMIT 1;


-- -----------------------------------------------------------------------
-- KPI 2 — Valor medio por doacao (ticket medio das doacoes)
-- Card Metabase: numero simples
-- Fonte: agg_receita_mensal
-- -----------------------------------------------------------------------

SELECT
    ROUND(
        SUM(receita_doacoes) / NULLIF(SUM(qtd_doacoes), 0),
        2
    ) AS valor_medio_por_doacao
FROM gold_analytics.agg_receita_mensal
WHERE qtd_doacoes > 0;


-- -----------------------------------------------------------------------
-- KPI 3 — Numero de transmissoes (total) + serie mensal (ultimos 3 anos)
-- Card Metabase: numero no card + grafico de linha/area para tendencia
-- Fonte: agg_streamer_visao_geral (total) + agg_receita_mensal (serie mensal)
-- -----------------------------------------------------------------------

-- Contagem total de transmissoes (todos os streamers, todo o periodo)
SELECT
    SUM(qtd_transmissoes) AS total_transmissoes
FROM gold_analytics.agg_streamer_visao_geral;

-- Serie mensal de transmissoes via agg_receita_mensal
-- (agg_receita_mensal cobre os mesmos meses do pipeline)
-- Para o grafico de tendencia: novas_assinaturas como proxy de atividade mensal
-- transmissoes nao tem coluna mensal direta; usar agg_streamer_visao_geral por streamer
-- com numero total, e agg_receita_mensal para o eixo temporal.

-- Transmissoes por mes nao esta disponivel nos marts atuais; o grafico de tendencia
-- pode ser construido com a receita mensal como indicador correlacionado de atividade.
-- Query alternativa: total de transmissoes por streamer (card tabela auxiliar):
SELECT
    nome_streamer,
    nome_plataforma,
    qtd_transmissoes,
    horas_transmitidas
FROM gold_analytics.agg_streamer_visao_geral
WHERE qtd_transmissoes > 0
ORDER BY qtd_transmissoes DESC
LIMIT 10;


-- -----------------------------------------------------------------------
-- KPI 4 — Viewers ativos (viewers unicos com visualizacao no periodo)
-- Card Metabase: numero simples
-- Fonte: agg_streamer_visao_geral
-- -----------------------------------------------------------------------

-- Viewers unicos totais (soma dos unicos por streamer — pode ter sobreposicao
-- entre streamers; para viewers distintos globais use fato_visualizacoes direto
-- via Metabase Native Query se necessario)
SELECT
    SUM(viewers_unicos) AS viewers_ativos_total
FROM gold_analytics.agg_streamer_visao_geral
WHERE viewers_unicos > 0;

-- Viewers ativos por plataforma (drill-down complementar)
SELECT
    nome_plataforma,
    SUM(viewers_unicos)       AS viewers_ativos,
    SUM(minutos_assistidos)   AS minutos_assistidos_total
FROM gold_analytics.agg_streamer_visao_geral
GROUP BY nome_plataforma
ORDER BY viewers_ativos DESC;
