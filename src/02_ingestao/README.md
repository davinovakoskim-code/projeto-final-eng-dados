# Ingestao PostgreSQL

Esta pasta contem a ingestao de dados brutos do PostgreSQL.
Nesta etapa o script conecta no banco e le as tabelas configuradas, sem aplicar transformacoes e sem gravar CSV.

## Configuracao

As credenciais e parametros de conexao devem ser definidos por variaveis de ambiente:

- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_DB`

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

Se uma tabela informada nao existir, a execucao e interrompida com erro.
