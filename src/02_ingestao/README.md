# Ingestao PostgreSQL

Esta pasta contem a base da ingestao de dados brutos do PostgreSQL.

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
