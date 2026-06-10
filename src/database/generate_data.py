import random
from datetime import date, datetime, timedelta
from faker import Faker
import psycopg2

fake = Faker('pt_BR')

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="streaming_db",
    user="admin",
    password="admin123"
)
cur = conn.cursor()

def rand_date(start_year=2023):
    start = date(start_year, 1, 1)
    end = date(2026, 6, 10)
    return start + timedelta(days=random.randint(0, (end - start).days))

def rand_datetime(start_year=2023):
    d = rand_date(start_year)
    return datetime(d.year, d.month, d.day, random.randint(0,23), random.randint(0,59))

print("Inserindo categorias...")
categorias = ['FPS', 'RPG', 'Esporte', 'Aventura', 'Estratégia', 'Luta', 'Corrida', 'Simulação', 'Terror', 'MOBA']
for c in categorias:
    cur.execute("INSERT INTO categorias (nome) VALUES (%s)", (c,))
conn.commit()

print("Inserindo plataformas...")
plataformas = ['PC', 'PlayStation 5', 'Xbox Series X', 'Nintendo Switch', 'Mobile']
for p in plataformas:
    cur.execute("INSERT INTO plataformas (nome) VALUES (%s)", (p,))
conn.commit()

print("Inserindo jogos...")
jogos_lista = [
    ('League of Legends', 'Riot Games'), ('Valorant', 'Riot Games'), ('Fortnite', 'Epic Games'),
    ('CS2', 'Valve'), ('Minecraft', 'Mojang'), ('GTA V', 'Rockstar'), ('FIFA 24', 'EA Sports'),
    ('Apex Legends', 'EA Games'), ('Call of Duty', 'Activision'), ('Dota 2', 'Valve'),
    ('Overwatch 2', 'Blizzard'), ('Rocket League', 'Psyonix'), ('Among Us', 'InnerSloth'),
    ('Fall Guys', 'Mediatonic'), ('Cyberpunk 2077', 'CD Projekt'), ('Elden Ring', 'FromSoftware'),
    ('Warzone', 'Activision'), ('PUBG', 'Krafton'), ('Hearthstone', 'Blizzard'), ('Teamfight Tactics', 'Riot Games')
]
for nome, dev in jogos_lista:
    cur.execute(
        "INSERT INTO jogos (nome, id_categoria, desenvolvedor, ano_lancamento, ativo) VALUES (%s, %s, %s, %s, %s)",
        (nome, random.randint(1, 10), dev, random.randint(2015, 2024), random.random() > 0.1)
    )
conn.commit()

print("Inserindo patrocinadores...")
patrocinadores_lista = [
    ('Red Bull', 'Áustria'), ('Monster Energy', 'EUA'), ('Logitech', 'Suíça'),
    ('Razer', 'Singapura'), ('HyperX', 'EUA'), ('NVIDIA', 'EUA'),
    ('Intel', 'EUA'), ('AMD', 'EUA'), ('Samsung', 'Coreia do Sul'), ('SteelSeries', 'Dinamarca')
]
for nome, pais in patrocinadores_lista:
    cur.execute("INSERT INTO patrocinadores (nome, pais) VALUES (%s, %s)", (nome, pais))
conn.commit()

print("Inserindo streamers (10.000)...")
for _ in range(10000):
    cur.execute(
        "INSERT INTO streamers (nome, pais, data_cadastro, id_plataforma) VALUES (%s, %s, %s, %s)",
        (fake.name(), fake.country(), rand_date(), random.randint(1, 5))
    )
conn.commit()

print("Inserindo viewers (10.000)...")
for _ in range(10000):
    cur.execute(
        "INSERT INTO viewers (nome, pais, data_cadastro) VALUES (%s, %s, %s)",
        (fake.name(), fake.country(), rand_date())
    )
conn.commit()

print("Inserindo emotes (1.000)...")
for _ in range(1000):
    cur.execute(
        "INSERT INTO emotes (nome, id_streamer, disponivel_para) VALUES (%s, %s, %s)",
        (fake.word() + str(random.randint(1,999)), random.randint(1, 10000), random.choice(['gratis', 'assinante']))
    )
conn.commit()

print("Inserindo torneios (500)...")
for _ in range(500):
    inicio = rand_date()
    fim = inicio + timedelta(days=random.randint(1, 30))
    cur.execute(
        "INSERT INTO torneios (nome, id_jogo, data_inicio, data_fim, premio_total) VALUES (%s, %s, %s, %s, %s)",
        (fake.catch_phrase(), random.randint(1, 20), inicio, fim, round(random.uniform(1000, 100000), 2))
    )
conn.commit()

print("Inserindo torneio_participantes (2.000)...")
for _ in range(2000):
    cur.execute(
        "INSERT INTO torneio_participantes (id_torneio, id_streamer, posicao_final) VALUES (%s, %s, %s)",
        (random.randint(1, 500), random.randint(1, 10000), random.randint(1, 32) if random.random() > 0.3 else None)
    )
conn.commit()

print("Inserindo contratos_patrocinio (1.000)...")
for _ in range(1000):
    inicio = rand_date()
    fim = inicio + timedelta(days=random.randint(30, 365)) if random.random() > 0.3 else None
    cur.execute(
        "INSERT INTO contratos_patrocinio (id_streamer, id_patrocinador, valor_mensal, data_inicio, data_fim) VALUES (%s, %s, %s, %s, %s)",
        (random.randint(1, 10000), random.randint(1, 10), round(random.uniform(500, 50000), 2), inicio, fim)
    )
conn.commit()

print("Inserindo transmissoes (10.000)...")
for _ in range(10000):
    inicio = rand_datetime()
    fim = inicio + timedelta(hours=random.randint(1, 12))
    cur.execute(
        "INSERT INTO transmissoes (id_streamer, id_jogo, data_inicio, data_fim, pico_viewers, id_torneio) VALUES (%s, %s, %s, %s, %s, %s)",
        (
            random.randint(1, 10000),
            random.randint(1, 20),
            inicio, fim,
            random.randint(10, 100000),
            random.randint(1, 500) if random.random() > 0.7 else None
        )
    )
conn.commit()

print("Inserindo visualizacoes (10.000)...")
for _ in range(10000):
    cur.execute(
        "INSERT INTO visualizacoes (id_viewer, id_transmissao, minutos_assistidos, data_hora) VALUES (%s, %s, %s, %s)",
        (random.randint(1, 10000), random.randint(1, 10000), random.randint(1, 720), rand_datetime())
    )
conn.commit()

print("Inserindo follows (10.000)...")
for _ in range(10000):
    follow = rand_date()
    unfollow = follow + timedelta(days=random.randint(1, 365)) if random.random() > 0.6 else None
    cur.execute(
        "INSERT INTO follows (id_viewer, id_streamer, data_follow, data_unfollow) VALUES (%s, %s, %s, %s)",
        (random.randint(1, 10000), random.randint(1, 10000), follow, unfollow)
    )
conn.commit()

print("Inserindo assinaturas (10.000)...")
for _ in range(10000):
    inicio = rand_date()
    fim = inicio + timedelta(days=random.randint(30, 730)) if random.random() > 0.4 else None
    tipo = random.choice(['gratis', 'tier1', 'tier2', 'tier3'])
    valor = 0 if tipo == 'gratis' else round(random.uniform(4.99, 24.99), 2)
    cur.execute(
        "INSERT INTO assinaturas (id_viewer, id_streamer, tipo, data_inicio, data_fim, valor_mensal) VALUES (%s, %s, %s, %s, %s, %s)",
        (random.randint(1, 10000), random.randint(1, 10000), tipo, inicio, fim, valor)
    )
conn.commit()

print("Inserindo doacoes (10.000)...")
for _ in range(10000):
    cur.execute(
        "INSERT INTO doacoes (id_viewer, id_streamer, id_transmissao, valor, data_hora) VALUES (%s, %s, %s, %s, %s)",
        (random.randint(1, 10000), random.randint(1, 10000), random.randint(1, 10000), round(random.uniform(1, 500), 2), rand_datetime())
    )
conn.commit()

print("Inserindo clips (10.000)...")
for _ in range(10000):
    cur.execute(
        "INSERT INTO clips (id_transmissao, id_viewer, visualizacoes, data_criacao) VALUES (%s, %s, %s, %s)",
        (random.randint(1, 10000), random.randint(1, 10000), random.randint(0, 500000), rand_datetime())
    )
conn.commit()

print("Inserindo raids (10.000)...")
for _ in range(10000):
    origem = random.randint(1, 10000)
    destino = random.randint(1, 10000)
    while destino == origem:
        destino = random.randint(1, 10000)
    cur.execute(
        "INSERT INTO raids (id_streamer_origem, id_streamer_destino, id_transmissao, viewers_enviados, data_hora) VALUES (%s, %s, %s, %s, %s)",
        (origem, destino, random.randint(1, 10000), random.randint(10, 50000), rand_datetime())
    )
conn.commit()

print("Inserindo moderadores (2.000)...")
for _ in range(2000):
    inicio = rand_date()
    fim = inicio + timedelta(days=random.randint(30, 500)) if random.random() > 0.5 else None
    cur.execute(
        "INSERT INTO moderadores (id_viewer, id_streamer, data_inicio, data_fim) VALUES (%s, %s, %s, %s)",
        (random.randint(1, 10000), random.randint(1, 10000), inicio, fim)
    )
conn.commit()

print("✅ Dados gerados com sucesso!")
cur.close()
conn.close()