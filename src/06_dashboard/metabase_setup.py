"""
Cria automaticamente a conexao, os cards de KPI/metricas e o dashboard
One Page View no Metabase via API REST.

Idempotente: verifica se cada recurso ja existe antes de criar.
Roda apos o stack Docker estar de pe e a DAG ter populado gold_analytics.

Uso:
    python src/06_dashboard/metabase_setup.py
    python src/06_dashboard/metabase_setup.py --metabase-url http://localhost:3000

Variaveis de ambiente (herda do .env):
    METABASE_URL          URL base do Metabase (default: http://localhost:3000)
    METABASE_USER         E-mail do admin do Metabase (default: admin@datalake.local)
    METABASE_PASSWORD     Senha do admin (default: admin1234)
    POSTGRES_HOST         Host do Postgres (default: localhost)
    POSTGRES_PORT         Porta do Postgres (default: 5433 — mapeada no host)
    POSTGRES_USER         Usuario do Postgres
    POSTGRES_PASSWORD     Senha do Postgres
    POSTGRES_DB           Database do Postgres (default: origem)
"""

import argparse
import os
import sys
import time
from pathlib import Path

_SRC_DIR = Path(__file__).resolve().parent.parent
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

try:
    import requests
except ImportError:
    sys.exit("Dependencia ausente: instale com  pip install requests")

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Configuracao
# ---------------------------------------------------------------------------

METABASE_URL = os.getenv("METABASE_URL", "http://localhost:3000").rstrip("/")
METABASE_USER = os.getenv("METABASE_USER", "admin@datalake.local")
METABASE_PASSWORD = os.getenv("METABASE_PASSWORD", "admin1234")

# Postgres acessivel a partir do HOST (porta mapeada 5433 -> 5432)
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
PG_USER = os.getenv("POSTGRES_USER", "admin")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")
PG_DB = os.getenv("POSTGRES_DB", "origem")

GOLD_SCHEMA = "gold_analytics"
DB_DISPLAY_NAME = "Gold Analytics (Postgres)"
DASHBOARD_NAME = "One Page View — Streaming Analytics"

# ---------------------------------------------------------------------------
# Cards: 4 KPIs + 2 metricas (issue #30 e #34)
# ---------------------------------------------------------------------------

CARDS = [
    # ------ KPIs (#30) ------
    {
        "name": "KPI 1 — Receita Total",
        "description": "Soma de doacoes + assinaturas de todo o periodo.",
        "display": "scalar",
        "visualization_settings": {
            "scalar.field": "receita_total_global",
            "column_settings": {
                '["name","receita_total_global"]': {
                    "number_style": "currency",
                    "currency": "BRL",
                    "currency_style": "symbol",
                }
            },
        },
        "sql": (
            "SELECT ROUND(SUM(receita_total), 2) AS receita_total_global\n"
            f"FROM {GOLD_SCHEMA}.agg_receita_mensal;"
        ),
    },
    {
        "name": "KPI 2 — Valor Medio por Doacao",
        "description": "Receita de doacoes dividida pelo numero de doacoes (ticket medio).",
        "display": "scalar",
        "visualization_settings": {
            "scalar.field": "valor_medio_por_doacao",
            "column_settings": {
                '["name","valor_medio_por_doacao"]': {
                    "number_style": "currency",
                    "currency": "BRL",
                    "currency_style": "symbol",
                }
            },
        },
        "sql": (
            "SELECT ROUND(\n"
            "    SUM(receita_doacoes) / NULLIF(SUM(qtd_doacoes), 0),\n"
            "    2\n"
            ") AS valor_medio_por_doacao\n"
            f"FROM {GOLD_SCHEMA}.agg_receita_mensal\n"
            "WHERE qtd_doacoes > 0;"
        ),
    },
    {
        "name": "KPI 3 — Total de Transmissoes",
        "description": "Contagem total de transmissoes de todos os streamers no periodo.",
        "display": "scalar",
        "visualization_settings": {
            "scalar.field": "total_transmissoes",
        },
        "sql": (
            "SELECT SUM(qtd_transmissoes) AS total_transmissoes\n"
            f"FROM {GOLD_SCHEMA}.agg_streamer_visao_geral;"
        ),
    },
    {
        "name": "KPI 4 — Viewers Ativos",
        "description": "Soma de viewers unicos com visualizacao registrada no periodo.",
        "display": "scalar",
        "visualization_settings": {
            "scalar.field": "viewers_ativos_total",
        },
        "sql": (
            "SELECT SUM(viewers_unicos) AS viewers_ativos_total\n"
            f"FROM {GOLD_SCHEMA}.agg_streamer_visao_geral\n"
            "WHERE viewers_unicos > 0;"
        ),
    },
    # ------ Metricas (#34) ------
    {
        "name": "Metrica 1 — Receita por Plataforma",
        "description": "Faturamento total (doacoes) agrupado por plataforma — barras horizontais.",
        "display": "row",
        "visualization_settings": {
            "graph.dimensions": ["nome_plataforma"],
            "graph.metrics": ["total_doacoes"],
            "graph.x_axis.title_text": "Plataforma",
            "graph.y_axis.title_text": "Receita (R$)",
        },
        "sql": (
            "SELECT nome_plataforma,\n"
            "       ROUND(SUM(total_doacoes), 2) AS total_doacoes\n"
            f"FROM {GOLD_SCHEMA}.agg_plataforma_resumo\n"
            "GROUP BY nome_plataforma\n"
            "ORDER BY total_doacoes DESC;"
        ),
    },
    {
        "name": "Metrica 2 — Top 10 Jogos por Transmissoes",
        "description": "Jogos com mais transmissoes — barras horizontais.",
        "display": "row",
        "visualization_settings": {
            "graph.dimensions": ["nome_jogo"],
            "graph.metrics": ["qtd_transmissoes"],
            "graph.x_axis.title_text": "Jogo",
            "graph.y_axis.title_text": "Transmissoes",
        },
        "sql": (
            "SELECT nome_jogo,\n"
            "       qtd_transmissoes\n"
            f"FROM {GOLD_SCHEMA}.agg_jogo_popularidade\n"
            "WHERE qtd_transmissoes > 0\n"
            "ORDER BY qtd_transmissoes DESC\n"
            "LIMIT 10;"
        ),
    },
]

# Layout do dashboard: (card_index, row, col, size_x, size_y)
# Grid do Metabase: 24 colunas, altura em unidades de ~150px
LAYOUT = [
    # KPIs na primeira linha — 4 cards de largura 6 cada
    (0, 0, 0, 6, 3),
    (1, 0, 6, 6, 3),
    (2, 0, 12, 6, 3),
    (3, 0, 18, 6, 3),
    # Metricas na segunda linha — 2 graficos de largura 12 cada
    (4, 3, 0, 12, 6),
    (5, 3, 12, 12, 6),
]

# ---------------------------------------------------------------------------
# Cliente HTTP
# ---------------------------------------------------------------------------


class MetabaseClient:
    def __init__(self, base_url: str):
        self.base = base_url
        self.session = requests.Session()
        self.session.headers["Content-Type"] = "application/json"

    def _url(self, path: str) -> str:
        return f"{self.base}/api{path}"

    def get(self, path: str) -> dict:
        r = self.session.get(self._url(path))
        r.raise_for_status()
        return r.json()

    def post(self, path: str, body: dict) -> dict:
        r = self.session.post(self._url(path), json=body)
        r.raise_for_status()
        return r.json()

    def put(self, path: str, body: dict) -> dict:
        r = self.session.put(self._url(path), json=body)
        r.raise_for_status()
        return r.json()

    def authenticate(self, email: str, password: str) -> None:
        resp = self.post("/session", {"username": email, "password": password})
        token = resp.get("id")
        if not token:
            raise RuntimeError(f"Login falhou: {resp}")
        self.session.headers["X-Metabase-Session"] = token
        print("  Autenticado no Metabase.")

    def setup_status(self) -> bool:
        """Retorna True se o Metabase ja passou pelo setup inicial."""
        try:
            resp = self.get("/session/properties")
            return resp.get("setup-token") is None
        except Exception:
            return False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def wait_for_metabase(url: str, timeout: int = 180) -> None:
    health = f"{url}/api/health"
    print(f"Aguardando Metabase em {url} ...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(health, timeout=5)
            if r.status_code == 200 and r.json().get("status") == "ok":
                print("  Metabase pronto.\n")
                return
        except Exception:
            pass
        time.sleep(5)
    raise TimeoutError(f"Metabase nao respondeu em {timeout}s. Verifique o container.")


def find_or_create_database(client: MetabaseClient) -> int:
    """Retorna o ID do banco gold_analytics, criando se nao existir."""
    databases = client.get("/database")
    items = databases if isinstance(databases, list) else databases.get("data", [])
    for db in items:
        if db.get("name") == DB_DISPLAY_NAME:
            db_id = db["id"]
            print(f"  Banco '{DB_DISPLAY_NAME}' ja existe (id={db_id}).")
            return db_id

    print(f"  Criando conexao '{DB_DISPLAY_NAME}' ...")
    payload = {
        "name": DB_DISPLAY_NAME,
        "engine": "postgres",
        "details": {
            "host": PG_HOST,
            "port": PG_PORT,
            "dbname": PG_DB,
            "user": PG_USER,
            "password": PG_PASSWORD,
            "schema-filters-type": "inclusion",
            "schema-filters-patterns": GOLD_SCHEMA,
            "ssl": False,
        },
        "auto_run_queries": True,
        "is_full_sync": True,
    }
    resp = client.post("/database", payload)
    db_id = resp["id"]
    print(f"  Banco criado (id={db_id}). Aguardando sync inicial ...")
    time.sleep(10)
    return db_id


def find_or_create_card(client: MetabaseClient, card_def: dict, db_id: int) -> int:
    """Retorna o ID do card, criando se nao existir."""
    cards = client.get("/card")
    for c in cards:
        if c.get("name") == card_def["name"]:
            print(f"    Card '{card_def['name']}' ja existe (id={c['id']}).")
            return c["id"]

    payload = {
        "name": card_def["name"],
        "description": card_def.get("description", ""),
        "display": card_def["display"],
        "visualization_settings": card_def.get("visualization_settings", {}),
        "dataset_query": {
            "type": "native",
            "native": {"query": card_def["sql"]},
            "database": db_id,
        },
    }
    resp = client.post("/card", payload)
    card_id = resp["id"]
    print(f"    Card '{card_def['name']}' criado (id={card_id}).")
    return card_id


def find_or_create_dashboard(client: MetabaseClient) -> int:
    """Retorna o ID do dashboard, criando se nao existir."""
    dashboards = client.get("/dashboard")
    for d in dashboards:
        if d.get("name") == DASHBOARD_NAME:
            print(f"  Dashboard '{DASHBOARD_NAME}' ja existe (id={d['id']}).")
            return d["id"]

    resp = client.post("/dashboard", {"name": DASHBOARD_NAME})
    dash_id = resp["id"]
    print(f"  Dashboard '{DASHBOARD_NAME}' criado (id={dash_id}).")
    return dash_id


def populate_dashboard(
    client: MetabaseClient, dash_id: int, card_ids: list[int]
) -> None:
    """Adiciona os cards ao dashboard com o layout definido em LAYOUT."""
    existing = client.get(f"/dashboard/{dash_id}")
    existing_card_ids = {
        dc.get("card_id") for dc in existing.get("dashcards", [])
    }

    dashcards = []
    for card_idx, row, col, size_x, size_y in LAYOUT:
        card_id = card_ids[card_idx]
        if card_id in existing_card_ids:
            print(f"    Card id={card_id} ja esta no dashboard.")
            continue
        dashcards.append({
            "id": -(card_idx + 1),  # ID temporario negativo (Metabase exige)
            "card_id": card_id,
            "row": row,
            "col": col,
            "size_x": size_x,
            "size_y": size_y,
            "parameter_mappings": [],
            "visualization_settings": {},
        })

    if not dashcards:
        print("  Todos os cards ja estavam no dashboard.")
        return

    client.put(f"/dashboard/{dash_id}", {"dashcards": dashcards})
    print(f"  {len(dashcards)} card(s) adicionado(s) ao dashboard.")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def run(metabase_url: str) -> None:
    global METABASE_URL
    METABASE_URL = metabase_url.rstrip("/")

    wait_for_metabase(METABASE_URL)

    client = MetabaseClient(METABASE_URL)

    if not client.setup_status():
        print(
            "\nO Metabase ainda nao passou pelo setup inicial.\n"
            "Acesse http://localhost:3000, complete o cadastro de admin e\n"
            "execute este script novamente.\n"
        )
        sys.exit(1)

    print("Autenticando ...")
    client.authenticate(METABASE_USER, METABASE_PASSWORD)

    print("\n[1/3] Configurando conexao com o Postgres ...")
    db_id = find_or_create_database(client)

    print("\n[2/3] Criando cards (KPIs + metricas) ...")
    card_ids = []
    for card_def in CARDS:
        card_id = find_or_create_card(client, card_def, db_id)
        card_ids.append(card_id)

    print("\n[3/3] Montando dashboard One Page View ...")
    dash_id = find_or_create_dashboard(client)
    populate_dashboard(client, dash_id, card_ids)

    print(
        f"\nSetup concluido!\n"
        f"Acesse: {METABASE_URL}/dashboard/{dash_id}\n"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Cria automaticamente o dashboard One Page View no Metabase via API."
    )
    parser.add_argument(
        "--metabase-url",
        default=os.getenv("METABASE_URL", "http://localhost:3000"),
        help="URL base do Metabase (default: http://localhost:3000)",
    )
    return parser


if __name__ == "__main__":
    args = build_parser().parse_args()
    run(args.metabase_url)
