# Modelagem Gold — Star Schema (Kimball)

Script PySpark da camada Gold do data lake medalhao. Le as tabelas **Silver**
(limpas e tipadas) e materializa um **modelo estrela** em Delta Lake.

| Script | Etapa | O que faz |
|---|---|---|
| `silver_to_gold.py` | Silver → Gold | 5 dimensoes + 4 fatos (Kimball) em Delta |
| `gold_agregados.py` | Gold → Gold | Data marts agregados (joins + agregacoes do modelo) |
| `consultas_analiticas.sql` | — | Consultas Spark SQL de exemplo sobre o modelo |

A Gold tem **dois estagios**: primeiro o star schema (`silver_to_gold.py`), depois os
agregados (`gold_agregados.py`), que consomem o star schema e materializam as tabelas
que o dashboard le direto — com os joins fato × dimensao e as agregacoes ja resolvidos,
sem refazer `group by` a cada consulta.

---

## Modelo dimensional

```
                +----------------+
                |   dim_tempo    |
                +----------------+
                        |
   dim_streamer --+-----+-----+-- dim_jogo
                  |     |     |
            +-----v-----v-----v-----+
            |   fato_transmissoes   |
            |   fato_visualizacoes  |
            |   fato_doacoes        |
            |   fato_assinaturas    |
            +-----^-----^-----^-----+
                  |     |     |
   dim_viewer ----+     |     +---- dim_plataforma (via dim_streamer)
```

### Dimensoes

| Dimensao | Chave substituta | Chave natural | Atributos principais |
|---|---|---|---|
| `dim_tempo` | `sk_tempo` (`yyyyMMdd`) | `data` | ano, trimestre, mes, dia, dia_semana, fim_de_semana |
| `dim_plataforma` | `sk_plataforma` | `id_plataforma` | nome_plataforma |
| `dim_streamer` | `sk_streamer` | `id_streamer` | nome_streamer, pais, data_cadastro, **plataforma desnormalizada** |
| `dim_viewer` | `sk_viewer` | `id_viewer` | nome_viewer, pais, data_cadastro |
| `dim_jogo` | `sk_jogo` | `id_jogo` | nome_jogo, desenvolvedor, ano_lancamento, ativo |

### Fatos

| Fato | Grao (1 linha por...) | Metricas | Chaves substitutas |
|---|---|---|---|
| `fato_transmissoes` | transmissao | pico_viewers, duracao_minutos | sk_tempo, sk_streamer, sk_jogo |
| `fato_visualizacoes` | visualizacao | minutos_assistidos | sk_tempo, sk_viewer, sk_streamer, sk_jogo |
| `fato_doacoes` | doacao | valor | sk_tempo, sk_viewer, sk_streamer |
| `fato_assinaturas` | assinatura | valor_mensal, duracao_dias | sk_tempo, sk_viewer, sk_streamer |

Os fatos guardam tambem **dimensoes degeneradas** (os `id_*` da propria transacao,
ex.: `id_transmissao`, `id_doacao`) para rastreabilidade.

---

## Decisoes de modelagem

- **Chaves substitutas (surrogate keys).** Cada dimensao recebe um `sk_*` sequencial
  gerado na Gold (independente das chaves de negocio). `dim_tempo` usa uma *smart key*
  `yyyyMMdd`, o que dispensa lookup: os fatos derivam `sk_tempo` direto do timestamp.
- **Integridade referencial materializada aqui.** A Silver garante qualidade *por
  tabela*; e na Gold que os joins fato × dimensao sao validados. Cada dimensao tem um
  **membro desconhecido** (`sk = -1`); um fato cuja chave natural nao casa com nenhuma
  dimensao aponta para esse membro (em vez de virar NULL ou ser descartado). O script
  reporta quantas FKs cairam em `-1`.
- **Desnormalizacao (snowflake → estrela).** A plataforma do streamer e desnormalizada
  para dentro de `dim_streamer`, mantendo o modelo em estrela pura.
- **Reprodutibilidade.** Gravacao em modo `overwrite`; reexecutar reconstroi a Gold de
  forma deterministica a partir da Silver.

---

## Data marts agregados (`gold_agregados.py`)

Segundo estagio: le o star schema e grava tabelas agregadas em `s3a://gold/agg_<mart>/`.
Os joins partem da dimensao filtrando `sk != -1`, entao linhas de fato orfas (membro
desconhecido) nao poluem os agregados.

| Mart | Grao (1 linha por...) | Conteudo |
|---|---|---|
| `agg_streamer_visao_geral` | streamer | qtd_transmissoes, pico_viewers_medio, horas_transmitidas, minutos_assistidos, viewers_unicos, total_doacoes, qtd_doacoes, assinaturas_ativas, mrr |
| `agg_receita_mensal` | mes (ano, mes) | receita_doacoes, qtd_doacoes, novas_assinaturas, receita_assinaturas, receita_total |
| `agg_jogo_popularidade` | jogo | qtd_transmissoes, streamers_distintos, pico_viewers_medio, duracao_media_min, minutos_assistidos |
| `agg_plataforma_resumo` | plataforma | qtd_streamers, total_doacoes, minutos_assistidos |

## Uso

Por padrao constroi todos os objetos (dimensoes antes dos fatos):

```bash
# 1) Star schema (dimensoes + fatos)
python src/04_modelagem_gold/silver_to_gold.py

# 2) Agregados (le o star schema e grava os marts)
python src/04_modelagem_gold/gold_agregados.py
```

Dentro do container Jupyter (tem Spark + jars + acesso a rede `datalake`):

```bash
docker exec jupyter_spark python \
  /home/jovyan/work/src/04_modelagem_gold/silver_to_gold.py
```

Restringir a objetos especificos (as dimensoes necessarias aos fatos sao
construidas automaticamente em memoria para o lookup das chaves):

```bash
python src/04_modelagem_gold/silver_to_gold.py --objects fato_doacoes dim_tempo
python src/04_modelagem_gold/gold_agregados.py --marts agg_receita_mensal
```

Alternativa exploratoria: o notebook `notebooks/silver_to_gold.ipynb`.

## Variaveis de ambiente

| Variavel | Obrigatoria | Padrao |
|---|---|---|
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | sim | — |
| `MINIO_SILVER_BUCKET` | nao | `silver` |
| `MINIO_GOLD_BUCKET` | nao | `gold` |
