# Contribuindo com a documentação

A documentação é **consolidada por uma pessoa** (responsável por orquestração + docs),
preenchendo cada seção conforme as etapas do pipeline são finalizadas pelos demais.

## Rodar a documentação localmente

Com **uv** (gerenciador do projeto):

```bash
# instala as dependências de documentação
uv add --group docs mkdocs-material pymdown-extensions

# sobe o servidor local com hot-reload em http://127.0.0.1:8000
uv run mkdocs serve
```

## Publicar (GitHub Pages)

```bash
uv run mkdocs gh-deploy
```

> A publicação é a issue **#32**. A URL gerada deve ser registrada na entrega do Portal AVA.

## Convenção: citar código real (Snippets)

Para que o código mostrado na doc seja **sempre o código real do projeto** (e nunca uma
cópia desatualizada), use a extensão **`pymdownx.snippets`**. Em vez de colar o código,
dentro de um bloco ` ```python ` adicione uma linha com o marcador apontando para o arquivo —
por exemplo, a linha `SNIPPET "src/02_ingestao/ingestao_postgres.py"` (onde `SNIPPET` é o
marcador `--8` seguido de `<--`) é substituída, no build, pelo conteúdo real do arquivo.

Também é possível embutir **apenas um intervalo de linhas**, acrescentando `:inicio:fim`
ao caminho (ex.: `...generate_data.py:15:25`).

!!! tip "Exemplo funcionando nesta doc"
    A página [Origem](origem.md#schema-ddl-real-do-projeto) embute o `schema.sql` real
    do projeto por este mecanismo — ao editar o arquivo em `src/`, a doc reflete sozinha.

## Como adicionar uma seção

1. Crie/edite o arquivo em `docs/<etapa>.md`.
2. Garanta que ela esteja no `nav:` do `mkdocs.yml`.
3. Use snippets para o código e Mermaid para diagramas.
4. Atualize a tabela de progresso em `index.md`.
