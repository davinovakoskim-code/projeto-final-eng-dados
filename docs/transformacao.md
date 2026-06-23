# Transformação — Silver (Data Quality)

Lê as tabelas Delta da **Bronze** (tudo `string`, fiel à origem), aplica regras de
**Data Quality** por tabela e grava versões **refinadas** em **Silver**.

!!! note "Etapa anterior"
    A persistência Landing → **Bronze** (Delta Lake, fiel à origem) está documentada em
    [Ingestão (Landing → Bronze)](ingestao.md#bronze-persistencia-em-delta-lake).

O script é [`src/03_transformacao/bronze_to_silver.py`](https://github.com/davinovakoskim-code/projeto-final-eng-dados/blob/main/src/03_transformacao/bronze_to_silver.py).
O motor de transformação é o **Apache Spark (PySpark)**.

## Spec declarativa por tabela

As regras de cada uma das 13 tabelas (chave primária, tipo-alvo de cada coluna, domínios e
checagens temporais) são descritas de forma **declarativa** no dicionário `TABLE_SPECS`,
derivado 1:1 do DDL de origem (`src/01_origem/schema.sql`). Um motor genérico aplica a spec —
adicionar uma nova regra é editar a spec, não o motor.

## Etapas (por tabela)

1. **Ler a Bronze**: `spark.read.format("delta").load("s3a://bronze/<tabela>")`. As colunas de
   auditoria próprias da Bronze (`_source_file`, `_ingestion_timestamp`) são descartadas;
   `_extraction_date` é propagada.
2. **Tipagem correta**: cast por coluna conforme a spec (`int`, `decimal(8,2)`, `date`,
   `timestamp`, `boolean`, `string`). Cast inválido vira `NULL` (capturado pela validação de
   nulos obrigatórios).
3. **Padronização**: `trim` em strings, string vazia → `NULL`, domínios normalizados
   (minúsculo/trim), datas/timestamps em tipo nativo Delta (formato canônico).
4. **Avaliação de qualidade**: cada linha recebe `_motivo_rejeicao` com as regras violadas.
5. **Separação válidos × quarentena**: linhas sem motivo seguem para a Silver; as demais vão
   para a quarentena (sem perder o dado).
6. **Deduplicação**: remoção de duplicados pela chave primária (`id_*`) nos registros válidos.
7. **Auditoria da Silver**: adiciona `_silver_processed_at`; grava em Delta `overwrite`
   (idempotente).

## Regras de Data Quality aplicadas

- **Nulos obrigatórios** — colunas `NOT NULL` do DDL (PK, `nome`, `data_cadastro`,
  `data_inicio`, `data_hora`, `data_criacao`, `valor`, `data_follow`, `data_fim` de
  transmissões) não podem ser nulas.
- **Duplicados** — deduplicação pela chave primária; uma linha por chave.
- **Domínios** (`CHECK` do DDL) — `assinaturas.tipo ∈ {gratis, tier1, tier2, tier3}`;
  `emotes.disponivel_para ∈ {gratis, assinante}`.
- **Coerência temporal** — `data_fim ≥ data_inicio` em `transmissoes`, `assinaturas`,
  `follows` (`data_unfollow ≥ data_follow`) e `moderadores`.

!!! info "Integridade referencial fica para a Gold"
    As FKs entre tabelas (ex.: `transmissoes.id_streamer` apontando para `streamers`) **não**
    são cruzadas aqui. Numa arquitetura medalhão, a verificação dimensão × fato pertence aos
    joins da camada **Gold** (modelo Kimball). A Silver foca em qualidade *por tabela*.

## Quarentena

Registros que violam regras **não são descartados silenciosamente**: vão para uma área de
**quarentena** em Delta, preservando o dado original e o motivo da rejeição.

| Destino | Caminho |
|---|---|
| Silver "limpa" | `s3a://silver/<tabela>/` |
| Quarentena | `s3a://silver/_quarentena/<tabela>/` (+ coluna `_motivo_rejeicao`) |

Assim, `lidos = válidos + quarentena + duplicados removidos` — nenhuma linha é perdida sem rastro.

## Uso

```bash
python src/03_transformacao/bronze_to_silver.py
```

Dentro do container Jupyter (Spark + jars + acesso à rede `datalake`):

```bash
docker exec jupyter_spark python \
  /home/jovyan/work/src/03_transformacao/bronze_to_silver.py
```

Por padrão processa todas as 13 tabelas; use `--tables` para restringir.

## Variáveis de ambiente

| Variável | Obrigatória | Padrão |
|---|---|---|
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | sim | — |
| `MINIO_BRONZE_BUCKET` | não | `bronze` |
| `MINIO_SILVER_BUCKET` | não | `silver` |
