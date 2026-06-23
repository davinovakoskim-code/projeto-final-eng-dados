# Transformacao — Bronze e Silver

Scripts PySpark da camada de transformacao do data lake medalhao:

| Script | Etapa | O que faz |
|---|---|---|
| `landing_to_bronze.py` | Landing → Bronze | CSV bruto → Delta fiel a origem (tudo string) |
| `bronze_to_silver.py` | Bronze → Silver | Tipagem, nulos, duplicados, dominios, quarentena |

---

## Landing → Bronze

Le os CSVs brutos da camada **Landing** no MinIO e os grava como tabelas **Delta Lake**
na camada **Bronze**. Os dados sao mantidos **fieis a origem**: nenhuma regra de negocio
e aplicada nesta etapa. Todas as colunas sao lidas como `string` (sem inferencia de tipo)
e apenas colunas de auditoria (proveniencia) sao adicionadas. A tipagem e a limpeza ficam
para a camada Silver.

## Etapas

1. Listar os CSVs da particao `extraction_date=YYYY-MM-DD/` no bucket **landing**.
2. Para cada CSV (`public__<tabela>.csv`), derivar o nome da tabela (`<tabela>`).
3. Ler o CSV com `header=true` e todas as colunas como `string`.
4. Adicionar colunas de auditoria: `_extraction_date`, `_source_file`, `_ingestion_timestamp`.
5. Gravar como Delta em `s3a://bronze/<tabela>/` no modo `overwrite`.
6. Idempotencia: reexecutar a mesma data sobrescreve a tabela sem duplicar dados.

## Uso

```bash
python src/03_transformacao/landing_to_bronze.py --extraction-date 2026-06-22
```

Dentro do container Jupyter (tem Spark + jars + acesso a rede `datalake`):

```bash
docker exec jupyter_spark python \
  /home/jovyan/work/src/03_transformacao/landing_to_bronze.py \
  --extraction-date 2026-06-22
```

Por padrao processa todos os CSVs da particao. Use `--tables` para restringir:

```bash
python src/03_transformacao/landing_to_bronze.py \
  --extraction-date 2026-06-22 --tables streamers transmissoes
```

## Variaveis de ambiente (Landing → Bronze)

| Variavel | Obrigatoria | Padrao |
|---|---|---|
| `MINIO_ENDPOINT` | sim | — |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | sim | — |
| `MINIO_LANDING_BUCKET` | nao | `landing` |
| `MINIO_BRONZE_BUCKET` | nao | `bronze` |
| `MINIO_SECURE` | nao | `false` |

---

## Bronze → Silver

Le as tabelas Delta da **Bronze** (tudo string) e grava versoes **refinadas** na **Silver**,
aplicando regras de **Data Quality** por tabela:

- **Tipagem correta** — cast por coluna espelhando o DDL (`src/01_origem/schema.sql`):
  `int`, `decimal(8,2)`, `date`, `timestamp`, `boolean`, `string`.
- **Padronizacao** — `trim` em strings, string vazia → `NULL`, datas/timestamps em tipo
  nativo (formato canonico), dominios normalizados (minusculo/trim).
- **Nulos obrigatorios** — colunas `NOT NULL` do DDL nao podem ser nulas.
- **Duplicados** — deduplicacao pela chave primaria (`id_*`), 1 linha por chave.
- **Dominios** — `CHECK` do DDL (`assinaturas.tipo`, `emotes.disponivel_para`).
- **Coerencia temporal** — `data_fim >= data_inicio` (transmissoes, assinaturas, follows,
  moderadores).

A spec de cada tabela (PK, tipos, dominios, regras temporais) e **declarativa**, no dict
`TABLE_SPECS` do script; um motor generico a aplica.

### Quarentena (registros invalidos)

Registros que violam regras **nao sao descartados silenciosamente**: vao para uma area de
**quarentena** em Delta, com o motivo da rejeicao em `_motivo_rejeicao`.

- Silver "limpa": `s3a://silver/<tabela>/`
- Quarentena: `s3a://silver/_quarentena/<tabela>/`

> A integridade referencial **entre tabelas** (FK dimensao × fato) **nao** e validada aqui:
> numa arquitetura medalhao isso pertence aos joins da **Gold** (Kimball). A Silver foca em
> qualidade *por tabela*.

### Uso

```bash
python src/03_transformacao/bronze_to_silver.py
```

Dentro do container Jupyter:

```bash
docker exec jupyter_spark python \
  /home/jovyan/work/src/03_transformacao/bronze_to_silver.py
```

Por padrao processa todas as 13 tabelas. Use `--tables` para restringir:

```bash
python src/03_transformacao/bronze_to_silver.py --tables streamers assinaturas
```

### Variaveis de ambiente (Bronze → Silver)

| Variavel | Obrigatoria | Padrao |
|---|---|---|
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | sim | — |
| `MINIO_BRONZE_BUCKET` | nao | `bronze` |
| `MINIO_SILVER_BUCKET` | nao | `silver` |
