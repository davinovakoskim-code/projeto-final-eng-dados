# Ingestão — Landing → Bronze

Extrai as tabelas do PostgreSQL de origem para a camada **Landing** (CSV bruto) no MinIO.
A persistência em **Bronze** (Delta) é a etapa seguinte do pipeline.

## Etapas

1. Conectar ao PostgreSQL via variáveis de ambiente (`.env`).
2. Extrair cada tabela de forma parametrizada (`--tables` / `--tables-file`).
3. Gerar CSV bruto por tabela em `data/landing/extraction_date=YYYY-MM-DD/`.
4. Subir os CSVs para o bucket **landing** no MinIO (`--upload-minio`).
5. Idempotência: a escrita é atômica (arquivo temporário + rename) e o upload
   sobrescreve o objeto, então reexecutar a mesma data não duplica dados.

## Uso

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

### Variáveis de ambiente

| Variável | Obrigatória | Padrão |
|---|---|---|
| `POSTGRES_HOST` / `POSTGRES_PORT` | não | `localhost` / `5432` |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | sim | — |
| `MINIO_ENDPOINT` | só com `--upload-minio` | — |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | só com `--upload-minio` | — |
| `MINIO_BUCKET` | não | `landing` |
| `MINIO_SECURE` | não | `false` |
