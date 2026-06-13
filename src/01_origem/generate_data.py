import random
from datetime import date, datetime, timedelta
from faker import Faker
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

fake = Faker('pt_BR')

# ----------------------------------------------------------------------
# Volumes centralizados.
# ----------------------------------------------------------------------
N_STREAMERS     = 10_000
N_VIEWERS       = 16_975
N_EMOTES        = 1_000
N_TRANSMISSOES  = 10_000
N_VISUALIZACOES = 10_000
N_FOLLOWS       = 10_000
N_ASSINATURAS   = 10_000
N_DOACOES       = 10_000
N_CLIPS         = 10_000
N_RAIDS         = 10_000
N_MODERADORES   = 2_000

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)
cur = conn.cursor()


def rand_date(start_year=2023):
    start = date(start_year, 1, 1)
    end = date(2026, 6, 10)
    return start + timedelta(days=random.randint(0, (end - start).days))


def rand_datetime(start_year=2023):
    d = rand_date(start_year)
    return datetime(d.year, d.month, d.day, random.randint(0, 23), random.randint(0, 59))


try:
    print("Inserindo plataformas...")
    plataformas = ['PC', 'PlayStation 5', 'Xbox Series X', 'Nintendo Switch', 'Mobile']
    for p in plataformas:
        cur.execute("INSERT INTO plataformas (nome) VALUES (%s)", (p,))
    conn.commit()
    n_plataformas = len(plataformas)

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
            "INSERT INTO jogos (nome, desenvolvedor, ano_lancamento, ativo) VALUES (%s, %s, %s, %s)",
            (nome, dev, random.randint(2015, 2024), random.random() > 0.1)
        )
    conn.commit()
    n_jogos = len(jogos_lista)

    print(f"Inserindo streamers ({N_STREAMERS})...")
    for _ in range(N_STREAMERS):
        cur.execute(
            "INSERT INTO streamers (nome, pais, data_cadastro, id_plataforma) VALUES (%s, %s, %s, %s)",
            (fake.name(), fake.country(), rand_date(), random.randint(1, n_plataformas))
        )
    conn.commit()

    print(f"Inserindo viewers ({N_VIEWERS})...")
    for _ in range(N_VIEWERS):
        cur.execute(
            "INSERT INTO viewers (nome, pais, data_cadastro) VALUES (%s, %s, %s)",
            (fake.name(), fake.country(), rand_date())
        )
    conn.commit()

    print(f"Inserindo emotes ({N_EMOTES})...")
    for _ in range(N_EMOTES):
        cur.execute(
            "INSERT INTO emotes (nome, id_streamer, disponivel_para) VALUES (%s, %s, %s)",
            (fake.word() + str(random.randint(1, 999)), random.randint(1, N_STREAMERS), random.choice(['gratis', 'assinante']))
        )
    conn.commit()

    print(f"Inserindo transmissoes ({N_TRANSMISSOES})...")
    for _ in range(N_TRANSMISSOES):
        inicio = rand_datetime()
        fim = inicio + timedelta(hours=random.randint(1, 12))
        cur.execute(
            "INSERT INTO transmissoes (id_streamer, id_jogo, data_inicio, data_fim, pico_viewers) VALUES (%s, %s, %s, %s, %s)",
            (random.randint(1, N_STREAMERS), random.randint(1, n_jogos), inicio, fim, random.randint(10, 100000))
        )
    conn.commit()

    print(f"Inserindo visualizacoes ({N_VISUALIZACOES})...")
    for _ in range(N_VISUALIZACOES):
        cur.execute(
            "INSERT INTO visualizacoes (id_viewer, id_transmissao, minutos_assistidos, data_hora) VALUES (%s, %s, %s, %s)",
            (random.randint(1, N_VIEWERS), random.randint(1, N_TRANSMISSOES), random.randint(1, 720), rand_datetime())
        )
    conn.commit()

    print(f"Inserindo follows ({N_FOLLOWS})...")
    for _ in range(N_FOLLOWS):
        follow = rand_date()
        unfollow = follow + timedelta(days=random.randint(1, 365)) if random.random() > 0.6 else None
        cur.execute(
            "INSERT INTO follows (id_viewer, id_streamer, data_follow, data_unfollow) VALUES (%s, %s, %s, %s)",
            (random.randint(1, N_VIEWERS), random.randint(1, N_STREAMERS), follow, unfollow)
        )
    conn.commit()

    print(f"Inserindo assinaturas ({N_ASSINATURAS})...")
    for _ in range(N_ASSINATURAS):
        inicio = rand_date()
        fim = inicio + timedelta(days=random.randint(30, 730)) if random.random() > 0.4 else None
        tipo = random.choice(['gratis', 'tier1', 'tier2', 'tier3'])
        valor = 0 if tipo == 'gratis' else round(random.uniform(4.99, 24.99), 2)
        cur.execute(
            "INSERT INTO assinaturas (id_viewer, id_streamer, tipo, data_inicio, data_fim, valor_mensal) VALUES (%s, %s, %s, %s, %s, %s)",
            (random.randint(1, N_VIEWERS), random.randint(1, N_STREAMERS), tipo, inicio, fim, valor)
        )
    conn.commit()

    print(f"Inserindo doacoes ({N_DOACOES})...")
    for _ in range(N_DOACOES):
        cur.execute(
            "INSERT INTO doacoes (id_viewer, id_streamer, id_transmissao, valor, data_hora) VALUES (%s, %s, %s, %s, %s)",
            (random.randint(1, N_VIEWERS), random.randint(1, N_STREAMERS), random.randint(1, N_TRANSMISSOES), round(random.uniform(1, 500), 2), rand_datetime())
        )
    conn.commit()

    print(f"Inserindo clips ({N_CLIPS})...")
    for _ in range(N_CLIPS):
        cur.execute(
            "INSERT INTO clips (id_transmissao, id_viewer, visualizacoes, data_criacao) VALUES (%s, %s, %s, %s)",
            (random.randint(1, N_TRANSMISSOES), random.randint(1, N_VIEWERS), random.randint(0, 500000), rand_datetime())
        )
    conn.commit()

    print(f"Inserindo raids ({N_RAIDS})...")
    for _ in range(N_RAIDS):
        origem = random.randint(1, N_STREAMERS)
        destino = random.randint(1, N_STREAMERS)
        while destino == origem:
            destino = random.randint(1, N_STREAMERS)
        cur.execute(
            "INSERT INTO raids (id_streamer_origem, id_streamer_destino, id_transmissao, viewers_enviados, data_hora) VALUES (%s, %s, %s, %s, %s)",
            (origem, destino, random.randint(1, N_TRANSMISSOES), random.randint(10, 50000), rand_datetime())
        )
    conn.commit()

    print(f"Inserindo moderadores ({N_MODERADORES})...")
    for _ in range(N_MODERADORES):
        inicio = rand_date()
        fim = inicio + timedelta(days=random.randint(30, 500)) if random.random() > 0.5 else None
        cur.execute(
            "INSERT INTO moderadores (id_viewer, id_streamer, data_inicio, data_fim) VALUES (%s, %s, %s, %s)",
            (random.randint(1, N_VIEWERS), random.randint(1, N_STREAMERS), inicio, fim)
        )
    conn.commit()

    print("\n✅ Dados gerados com sucesso!")

    print("\n--- Conferência de linhas por tabela ---")
    tabelas = ['plataformas', 'jogos', 'streamers', 'viewers', 'emotes',
               'transmissoes', 'visualizacoes', 'follows', 'assinaturas',
               'doacoes', 'clips', 'raids', 'moderadores']
    total = 0
    for t in tabelas:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        n = cur.fetchone()[0]
        total += n
        print(f"{t:<16} {n:>7}")
    print("-" * 24)
    print(f"{'TOTAL':<16} {total:>7}")

except Exception as e:
    conn.rollback()
    print(f"\n❌ Erro durante a geração: {e}")
    raise
finally:
    cur.close()
    conn.close()