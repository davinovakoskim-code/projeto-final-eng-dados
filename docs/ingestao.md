# Ingestão — Landing → Bronze

Extrai as tabelas do PostgreSQL de origem para a camada **Landing** (CSV bruto) no MinIO
e, em seguida, persiste em **Bronze** (Delta).

!!! warning "Em desenvolvimento"
    Etapa em andamento. PR **#48** (base da ingestão) em revisão; issues **#42–#47**
    (extração parametrizada, CSV bruto, client MinIO, upload e idempotência) pendentes.

## Etapas planejadas

1. Conectar ao PostgreSQL via variáveis de ambiente (`.env`).
2. Extrair cada tabela de forma parametrizada (`--tables` / `--tables-file`).
3. Gerar CSV bruto por tabela.
4. Subir os CSVs para o bucket **landing** no MinIO.
5. Garantir idempotência (reexecução sem duplicar dados).

## Código

!!! note "A preencher após o merge"
    Quando `src/02_ingestao/` tiver o script, embuta o código real nesta página usando
    a convenção de snippets descrita em [Contribuindo](contribuindo.md#convencao-citar-codigo-real-snippets).
