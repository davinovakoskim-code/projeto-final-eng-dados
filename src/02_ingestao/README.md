# Ingestao PostgreSQL

Esta pasta contem a ingestao de dados brutos do PostgreSQL.
Nesta etapa o script conecta no banco, le as tabelas configuradas e grava um CSV bruto local por tabela.

## Configuracao

As credenciais e parametros de conexao devem ser definidos por variaveis de ambiente:

- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_DB`
- `MINIO_ENDPOINT`
- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`
- `MINIO_BUCKET`
- `MINIO_SECURE`

## Uso

```bash
python src/02_ingestao/ingestao_postgres.py --tables clientes pedidos
```

Tambem e possivel informar as tabelas por arquivo:

```bash
python src/02_ingestao/ingestao_postgres.py --tables-file tabelas.txt
```

Para validar a conexao antes de executar:

```bash
python src/02_ingestao/ingestao_postgres.py --tables clientes --check-connection
```

Exemplo usando o PostgreSQL local do Docker:

```bash
POSTGRES_HOST=localhost \
POSTGRES_PORT=5433 \
POSTGRES_USER=admin \
POSTGRES_PASSWORD=admin \
POSTGRES_DB=origem \
uv run python src/02_ingestao/ingestao_postgres.py --tables plataformas jogos
```

Por padrao os arquivos sao gravados em:

```text
data/landing/{schema}/{tabela}.csv
```

Para alterar o diretorio de saida:

```bash
python src/02_ingestao/ingestao_postgres.py --tables plataformas --output-dir data/teste_landing
```

Se uma tabela informada nao existir, a execucao e interrompida com erro.

## MinIO

O client do MinIO pode ser validado com:

```bash
python src/02_ingestao/minio_client.py --check-connection
```

O bucket padrao de destino e `landing`, mas pode ser alterado com `MINIO_BUCKET`.

### Estrutura da Landing

Os arquivos no bucket `landing` seguem o padrao:

```text
{schema}/{tabela}/data_extracao=YYYY-MM-DD/{tabela}.csv
```

Exemplo:

```text
public/plataformas/data_extracao=2026-06-22/plataformas.csv
```

Esse padrao fica centralizado na funcao `build_landing_object_name`.
