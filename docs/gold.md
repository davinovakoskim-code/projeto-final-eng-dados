# Gold — Modelagem Dimensional (Kimball)

Lê os dados de **Silver** e alimenta o **modelo estrela** (Ralph Kimball) na camada Gold.

!!! warning "A iniciar"
    Etapa ainda não começada (sem PR associado).

## Esboço do Star Schema

Proposta inicial de dimensões e fatos a partir do domínio de streaming:

```mermaid
flowchart TB
    F_TRANS[Fato: Transmissões<br/>pico_viewers, duração]
    F_DOA[Fato: Doações<br/>valor]
    D_STREAMER[Dim Streamer]
    D_VIEWER[Dim Viewer]
    D_JOGO[Dim Jogo]
    D_TEMPO[Dim Tempo]
    D_PLAT[Dim Plataforma]

    D_STREAMER --> F_TRANS
    D_JOGO --> F_TRANS
    D_TEMPO --> F_TRANS
    D_PLAT --> F_TRANS
    D_STREAMER --> F_DOA
    D_VIEWER --> F_DOA
    D_TEMPO --> F_DOA
```

## Dimensões e fatos previstos

- **Dimensões:** `dim_streamer`, `dim_viewer`, `dim_jogo`, `dim_plataforma`, `dim_tempo`.
- **Fatos:** `fato_transmissoes`, `fato_doacoes`, `fato_assinaturas` (a confirmar).

## Código

A preencher com snippets de `src/04_modelagem_gold/` após implementação.
