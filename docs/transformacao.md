# Transformação — Silver (Data Quality)

Lê os dados de **Bronze**, aplica regras de **Data Quality** e grava em **Silver**.

!!! warning "A iniciar"
    Etapa ainda não começada (sem PR associado).

## Regras de Data Quality previstas

- Remoção/marcação de registros com chaves nulas ou órfãs (FKs inválidas).
- Padronização de tipos (datas, timestamps, valores monetários).
- Deduplicação por chave primária.
- Validação de domínios (ex.: `assinaturas.tipo ∈ {gratis, tier1, tier2, tier3}`).
- Coerência temporal (ex.: `transmissoes.data_fim ≥ data_inicio`).

## Código

A preencher com snippets de `src/03_transformacao/` após implementação.
