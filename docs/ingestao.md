# Ingestão — Landing → Bronze

Extrai as tabelas do PostgreSQL de origem para a camada **Landing** (CSV bruto) no MinIO
e, em seguida, persiste esses dados em **Bronze** no formato **Delta Lake**.

## Landing — extração para CSV

### Etapas

1. Conectar ao PostgreSQL via variáveis de ambiente (`.env`).
2. Extrair cada tabela de forma parametrizada (`--tables` / `--tables-file`).
3. Gerar CSV bruto por tabela em `data/landing/extraction_date=YYYY-MM-DD/`.
4. Subir os CSVs para o bucket **landing** no MinIO (`--upload-minio`).
5. Idempotência: a escrita é atômica (arquivo temporário + rename) e o upload
   sobrescreve o objeto, então reexecutar a mesma data não duplica dados.

### Uso

```bash
uv run python src/02_ingestao/ingestao_postgres.py \
  --tables plataformas jogos streamers viewers emotes transmissoes \
           visualizacoes follows assinaturas doacoes clips raids moderadores \
  --output-dir data/landing \
  --extraction-date 2026-06-22 \
  --upload-minio
```

As tabelas também podem vir de um arquivo com `--tables-file`. Use `--check-connection`
para validar o acesso ao PostgreSQL antes de extrair.

### Variáveis de ambiente — Landing

| Variável | Obrigatória | Padrão |
|---|---|---|
| `POSTGRES_HOST` / `POSTGRES_PORT` | não | `localhost` / `5432` |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | sim | — |
| `MINIO_ENDPOINT` | só com `--upload-minio` | — |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | só com `--upload-minio` | — |
| `MINIO_BUCKET` | não | `landing` |
| `MINIO_SECURE` | não | `false` |

## Bronze — persistência em Delta Lake

Lê os CSVs da Landing e os grava como tabelas **Delta Lake** na camada **Bronze**, mantendo
os dados **fiéis à origem** — nenhuma regra de negócio é aplicada aqui (isso fica para a
[Silver](transformacao.md)). O motor de transformação é o **Apache Spark (PySpark)**.

### Etapas

1. Listar os CSVs da partição `extraction_date=YYYY-MM-DD/` no bucket **landing**.
2. Para cada `public__<tabela>.csv`, derivar o nome da tabela (`<tabela>`).
3. Ler o CSV com `header=true` e **todas as colunas como `string`** (sem inferência de
   tipo), preservando o dado bruto.
4. Adicionar colunas de auditoria (proveniência, não regra de negócio): `_extraction_date`,
   `_source_file`, `_ingestion_timestamp`.
5. Gravar como Delta em `s3a://bronze/<tabela>/` no modo `overwrite` — uma tabela Delta por
   tabela de origem.
6. Idempotência: reexecutar a mesma data sobrescreve a tabela sem duplicar dados.

A `SparkSession` (Delta Lake + acesso s3a ao MinIO) é construída por
`src/utils/spark_config.py`.

### Uso

```bash
python src/03_transformacao/landing_to_bronze.py --extraction-date 2026-06-22
```

Dentro do container Jupyter (Spark + jars + acesso à rede `datalake`):

```bash
docker exec jupyter_spark python \
  /home/jovyan/work/src/03_transformacao/landing_to_bronze.py \
  --extraction-date 2026-06-22
```

Por padrão processa todos os CSVs da partição; use `--tables` para restringir.

### Variáveis de ambiente — Bronze

| Variável | Obrigatória | Padrão |
|---|---|---|
| `MINIO_ENDPOINT` | sim | — |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | sim | — |
| `MINIO_LANDING_BUCKET` | não | `landing` |
| `MINIO_BRONZE_BUCKET` | não | `bronze` |
| `MINIO_SECURE` | não | `false` |
