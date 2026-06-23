# Transformacao — Landing → Bronze

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

## Variaveis de ambiente

| Variavel | Obrigatoria | Padrao |
|---|---|---|
| `MINIO_ENDPOINT` | sim | — |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | sim | — |
| `MINIO_LANDING_BUCKET` | nao | `landing` |
| `MINIO_BRONZE_BUCKET` | nao | `bronze` |
| `MINIO_SECURE` | nao | `false` |
