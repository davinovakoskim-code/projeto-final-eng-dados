-- ======================================================================
-- Limpa o schema antigo (idempotente) — pode rodar quantas vezes quiser.
-- ======================================================================
DROP TABLE IF EXISTS
    moderadores, raids, clips, doacoes, assinaturas, follows,
    visualizacoes, transmissoes, emotes, viewers, streamers, jogos, plataformas,
    contratos_patrocinio, torneio_participantes, torneios, patrocinadores, categorias
CASCADE;

-- ======================================================================
-- TABELAS DE REFERÊNCIA
-- ======================================================================

CREATE TABLE plataformas (
    id_plataforma SERIAL PRIMARY KEY,
    nome VARCHAR(50) NOT NULL
);

CREATE TABLE jogos (
    id_jogo SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    desenvolvedor VARCHAR(100),
    ano_lancamento INT,
    ativo BOOLEAN DEFAULT TRUE
);

-- ======================================================================
-- ENTIDADES PRINCIPAIS
-- ======================================================================

CREATE TABLE streamers (
    id_streamer SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    pais VARCHAR(50),
    data_cadastro DATE NOT NULL,
    id_plataforma INT REFERENCES plataformas(id_plataforma)
);

CREATE TABLE viewers (
    id_viewer SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    pais VARCHAR(50),
    data_cadastro DATE NOT NULL
);

CREATE TABLE emotes (
    id_emote SERIAL PRIMARY KEY,
    nome VARCHAR(50) NOT NULL,
    id_streamer INT REFERENCES streamers(id_streamer),
    disponivel_para VARCHAR(20) CHECK (disponivel_para IN ('gratis', 'assinante'))
);

CREATE TABLE transmissoes (
    id_transmissao SERIAL PRIMARY KEY,
    id_streamer INT REFERENCES streamers(id_streamer),
    id_jogo INT REFERENCES jogos(id_jogo),
    data_inicio TIMESTAMP NOT NULL,
    data_fim TIMESTAMP NOT NULL,
    pico_viewers INT
);

CREATE TABLE visualizacoes (
    id_visualizacao SERIAL PRIMARY KEY,
    id_viewer INT REFERENCES viewers(id_viewer),
    id_transmissao INT REFERENCES transmissoes(id_transmissao),
    minutos_assistidos INT,
    data_hora TIMESTAMP NOT NULL
);

CREATE TABLE follows (
    id_follow SERIAL PRIMARY KEY,
    id_viewer INT REFERENCES viewers(id_viewer),
    id_streamer INT REFERENCES streamers(id_streamer),
    data_follow DATE NOT NULL,
    data_unfollow DATE
);

CREATE TABLE assinaturas (
    id_assinatura SERIAL PRIMARY KEY,
    id_viewer INT REFERENCES viewers(id_viewer),
    id_streamer INT REFERENCES streamers(id_streamer),
    tipo VARCHAR(20) CHECK (tipo IN ('gratis', 'tier1', 'tier2', 'tier3')),
    data_inicio DATE NOT NULL,
    data_fim DATE,
    valor_mensal NUMERIC(8,2)
);

CREATE TABLE doacoes (
    id_doacao SERIAL PRIMARY KEY,
    id_viewer INT REFERENCES viewers(id_viewer),
    id_streamer INT REFERENCES streamers(id_streamer),
    id_transmissao INT REFERENCES transmissoes(id_transmissao),
    valor NUMERIC(8,2) NOT NULL,
    data_hora TIMESTAMP NOT NULL
);

CREATE TABLE clips (
    id_clip SERIAL PRIMARY KEY,
    id_transmissao INT REFERENCES transmissoes(id_transmissao),
    id_viewer INT REFERENCES viewers(id_viewer),
    visualizacoes INT DEFAULT 0,
    data_criacao TIMESTAMP NOT NULL
);

CREATE TABLE raids (
    id_raid SERIAL PRIMARY KEY,
    id_streamer_origem INT REFERENCES streamers(id_streamer),
    id_streamer_destino INT REFERENCES streamers(id_streamer),
    id_transmissao INT REFERENCES transmissoes(id_transmissao),
    viewers_enviados INT,
    data_hora TIMESTAMP NOT NULL
);

CREATE TABLE moderadores (
    id_moderador SERIAL PRIMARY KEY,
    id_viewer INT REFERENCES viewers(id_viewer),
    id_streamer INT REFERENCES streamers(id_streamer),
    data_inicio DATE NOT NULL,
    data_fim DATE
);