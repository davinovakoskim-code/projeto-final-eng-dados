"""
Modelagem Gold -- Star Schema (Kimball).

Le as tabelas Delta da camada Silver (limpas e tipadas por ``bronze_to_silver.py``)
e materializa o modelo dimensional (estrela) na camada Gold, tambem em Delta Lake.

Modelo dimensional (grao de cada fato entre parenteses):

  Dimensoes
    - dim_tempo        : calendario diario derivado das datas dos fatos.
    - dim_plataforma   : plataformas de streaming.
    - dim_streamer     : streamers (com a plataforma desnormalizada).
    - dim_viewer       : espectadores.
    - dim_jogo         : jogos transmitidos.

  Fatos
    - fato_transmissoes  (1 linha por transmissao)   -> pico_viewers, duracao_minutos
    - fato_visualizacoes (1 linha por visualizacao)  -> minutos_assistidos
    - fato_doacoes       (1 linha por doacao)        -> valor
    - fato_assinaturas   (1 linha por assinatura)    -> valor_mensal, duracao_dias

Cada dimensao recebe uma CHAVE SUBSTITUTA (surrogate key, ``sk_*``) gerada aqui,
mantendo tambem a chave natural de negocio (``id_*``). Os fatos guardam apenas as
chaves substitutas das dimensoes (modelo estrela) + dimensoes degeneradas (os
``id_*`` da propria transacao) + as metricas.

Integridade referencial: e AQUI que ela e validada (a Silver garante qualidade por
tabela, nao entre tabelas). Cada dimensao tem um MEMBRO DESCONHECIDO (``sk = -1``);
fatos cuja chave natural nao casa com nenhuma dimensao apontam para esse membro em
vez de virar NULL ou serem descartados.

  Silver (origem):  s3a://silver/<tabela>/
  Gold   (destino): s3a://gold/<dim_ou_fato>/

Uso:
    python src/04_modelagem_gold/silver_to_gold.py
    python src/04_modelagem_gold/silver_to_gold.py --objects dim_streamer fato_doacoes
"""

import argparse
import os
import sys
from dataclasses import dataclass
from functools import reduce
from pathlib import Path

# Garante que src/ esteja no path para importar utils.spark_config, tanto local
# quanto dentro do container Jupyter (/home/jovyan/work/src).
_SRC_DIR = Path(__file__).resolve().parent.parent
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from utils.spark_config import build_spark_session  # noqa: E402


DEFAULT_SILVER_BUCKET = "silver"
DEFAULT_GOLD_BUCKET = "gold"

# Chave substituta do membro "desconhecido" (linhas de fato sem dimensao casada).
UNKNOWN_SK = -1

# Ordem topologica: dimensoes primeiro (os fatos dependem delas para o lookup das
# chaves substitutas). dim_tempo e construida dinamicamente a partir dos fatos.
DIMENSIONS = ("dim_plataforma", "dim_streamer", "dim_viewer", "dim_jogo", "dim_tempo")
FACTS = ("fato_transmissoes", "fato_visualizacoes", "fato_doacoes", "fato_assinaturas")
ALL_OBJECTS = DIMENSIONS + FACTS


@dataclass(frozen=True)
class GoldConfig:
    """Configuracao dos buckets origem (Silver) e destino (Gold)."""

    silver_bucket: str
    gold_bucket: str

    @classmethod
    def from_env(cls) -> "GoldConfig":
        # Apenas valida que as credenciais do MinIO existem; o acesso real ao
        # storage e via s3a (configurado em build_spark_session).
        _required_env("MINIO_ROOT_USER")
        _required_env("MINIO_ROOT_PASSWORD")
        return cls(
            silver_bucket=os.getenv("MINIO_SILVER_BUCKET", DEFAULT_SILVER_BUCKET),
            gold_bucket=os.getenv("MINIO_GOLD_BUCKET", DEFAULT_GOLD_BUCKET),
        )


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variavel de ambiente obrigatoria nao definida: {name}")
    return value


def load_environment() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return

    load_dotenv()


# ----------------------------------------------------------------------
# Leitura / escrita
# ----------------------------------------------------------------------


def read_silver(spark, config: GoldConfig, table: str):
    """Le uma tabela Delta da Silver. Descarta a auditoria propria da Silver."""
    path = f"s3a://{config.silver_bucket}/{table}"
    df = spark.read.format("delta").load(path)
    drop_cols = [c for c in ("_silver_processed_at", "_extraction_date") if c in df.columns]
    return df.drop(*drop_cols) if drop_cols else df


def add_gold_audit(df):
    from pyspark.sql import functions as F

    return df.withColumn("_gold_processed_at", F.current_timestamp())


def write_delta(df, path: str) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )


# ----------------------------------------------------------------------
# Utilitarios dimensionais (Kimball)
# ----------------------------------------------------------------------


def add_surrogate_key(df, sk_name: str, order_col: str):
    """Gera uma chave substituta sequencial (1..N) ordenada pela chave natural.

    row_number sobre uma janela global e deterministico dado o mesmo conjunto de
    dados, o que mantem a Gold reproduzivel em reexecucoes (modo overwrite).
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    window = Window.orderBy(order_col)
    return df.withColumn(sk_name, F.row_number().over(window).cast("long"))


def with_unknown_member(spark, dim, sk_name: str):
    """Prepende a linha do membro desconhecido (sk = -1) a uma dimensao.

    Para cada coluna alem da chave substituta, usa "(desconhecido)" em colunas
    string e NULL nas demais, preservando o tipo de cada coluna.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    selects = []
    for fld in dim.schema.fields:
        if fld.name == sk_name:
            value = F.lit(UNKNOWN_SK)
        elif isinstance(fld.dataType, StringType):
            value = F.lit("(desconhecido)")
        else:
            value = F.lit(None)
        selects.append(value.cast(fld.dataType).alias(fld.name))

    unknown = spark.range(1).select(*selects)
    return unknown.unionByName(dim)


def map_surrogate(fact, dim, fact_nk: str, dim_nk: str, dim_sk: str, out_sk: str):
    """Traduz uma chave natural do fato na chave substituta da dimensao.

    Left join: chaves sem correspondencia recebem o membro desconhecido (-1),
    materializando a validacao de integridade referencial na Gold.
    """
    from pyspark.sql import functions as F

    lookup = dim.select(
        F.col(dim_nk).alias("_nk"), F.col(dim_sk).alias("_sk")
    )
    joined = fact.join(lookup, fact[fact_nk] == F.col("_nk"), "left")
    return (
        joined
        .withColumn(out_sk, F.coalesce(F.col("_sk"), F.lit(UNKNOWN_SK).cast("long")))
        .drop("_nk", "_sk")
    )


def map_tempo(fact, ts_col: str, out_sk: str = "sk_tempo"):
    """Deriva a chave de dim_tempo (yyyyMMdd) a partir de um timestamp/date do fato."""
    from pyspark.sql import functions as F

    sk = F.date_format(F.to_date(F.col(ts_col)), "yyyyMMdd").cast("int")
    return fact.withColumn(out_sk, F.coalesce(sk, F.lit(UNKNOWN_SK)))


# ----------------------------------------------------------------------
# Dimensoes
# ----------------------------------------------------------------------


def build_dim_plataforma(spark, silver):
    from pyspark.sql import functions as F

    dim = (
        silver["plataformas"]
        .select(
            F.col("id_plataforma"),
            F.col("nome").alias("nome_plataforma"),
        )
    )
    dim = add_surrogate_key(dim, "sk_plataforma", "id_plataforma")
    dim = dim.select("sk_plataforma", "id_plataforma", "nome_plataforma")
    return with_unknown_member(spark, dim, "sk_plataforma")


def build_dim_streamer(spark, silver):
    """Streamer com a plataforma desnormalizada (snowflake -> estrela)."""
    from pyspark.sql import functions as F

    plataformas = silver["plataformas"].select(
        F.col("id_plataforma").alias("_id_plat"),
        F.col("nome").alias("nome_plataforma"),
    )

    dim = (
        silver["streamers"]
        .join(plataformas, F.col("id_plataforma") == F.col("_id_plat"), "left")
        .select(
            F.col("id_streamer"),
            F.col("nome").alias("nome_streamer"),
            F.col("pais"),
            F.col("data_cadastro"),
            F.col("id_plataforma"),
            F.col("nome_plataforma"),
        )
    )
    dim = add_surrogate_key(dim, "sk_streamer", "id_streamer")
    dim = dim.select(
        "sk_streamer", "id_streamer", "nome_streamer", "pais",
        "data_cadastro", "id_plataforma", "nome_plataforma",
    )
    return with_unknown_member(spark, dim, "sk_streamer")


def build_dim_viewer(spark, silver):
    from pyspark.sql import functions as F

    dim = (
        silver["viewers"]
        .select(
            F.col("id_viewer"),
            F.col("nome").alias("nome_viewer"),
            F.col("pais"),
            F.col("data_cadastro"),
        )
    )
    dim = add_surrogate_key(dim, "sk_viewer", "id_viewer")
    dim = dim.select("sk_viewer", "id_viewer", "nome_viewer", "pais", "data_cadastro")
    return with_unknown_member(spark, dim, "sk_viewer")


def build_dim_jogo(spark, silver):
    from pyspark.sql import functions as F

    dim = (
        silver["jogos"]
        .select(
            F.col("id_jogo"),
            F.col("nome").alias("nome_jogo"),
            F.col("desenvolvedor"),
            F.col("ano_lancamento"),
            F.col("ativo"),
        )
    )
    dim = add_surrogate_key(dim, "sk_jogo", "id_jogo")
    dim = dim.select(
        "sk_jogo", "id_jogo", "nome_jogo", "desenvolvedor", "ano_lancamento", "ativo"
    )
    return with_unknown_member(spark, dim, "sk_jogo")


def build_dim_tempo(spark, silver):
    """Calendario diario cobrindo todas as datas referenciadas pelos fatos.

    A chave substituta e o proprio dia no formato yyyyMMdd (smart key), o que
    dispensa lookup: os fatos derivam sk_tempo direto do seu timestamp.
    """
    from pyspark.sql import functions as F

    # Coleta as datas de todas as colunas temporais usadas como FK pelos fatos.
    sources = [
        (silver["transmissoes"], "data_inicio"),
        (silver["visualizacoes"], "data_hora"),
        (silver["doacoes"], "data_hora"),
        (silver["assinaturas"], "data_inicio"),
    ]
    date_dfs = [df.select(F.to_date(F.col(col)).alias("data")) for df, col in sources]
    all_dates = reduce(lambda a, b: a.unionByName(b), date_dfs)
    bounds = all_dates.agg(
        F.min("data").alias("min_d"), F.max("data").alias("max_d")
    ).collect()[0]

    min_d, max_d = bounds["min_d"], bounds["max_d"]
    if min_d is None or max_d is None:
        raise RuntimeError(
            "Nao foi possivel determinar o intervalo de datas para dim_tempo "
            "(fatos sem datas validas na Silver)."
        )

    calendario = spark.range(1).select(
        F.explode(
            F.sequence(F.lit(min_d), F.lit(max_d), F.expr("interval 1 day"))
        ).alias("data")
    )

    dim = calendario.select(
        F.date_format(F.col("data"), "yyyyMMdd").cast("int").alias("sk_tempo"),
        F.col("data"),
        F.year("data").alias("ano"),
        F.quarter("data").alias("trimestre"),
        F.month("data").alias("mes"),
        F.date_format("data", "MMMM").alias("nome_mes"),
        F.dayofmonth("data").alias("dia"),
        F.dayofweek("data").alias("dia_semana"),
        F.date_format("data", "EEEE").alias("nome_dia_semana"),
        F.dayofweek("data").isin(1, 7).alias("fim_de_semana"),
    )
    return with_unknown_member(spark, dim, "sk_tempo")


DIMENSION_BUILDERS = {
    "dim_plataforma": build_dim_plataforma,
    "dim_streamer": build_dim_streamer,
    "dim_viewer": build_dim_viewer,
    "dim_jogo": build_dim_jogo,
    "dim_tempo": build_dim_tempo,
}


# ----------------------------------------------------------------------
# Fatos
# ----------------------------------------------------------------------


def build_fato_transmissoes(spark, silver, dims):
    """Grao: 1 linha por transmissao. Metricas: pico_viewers, duracao_minutos."""
    from pyspark.sql import functions as F

    base = silver["transmissoes"]
    base = map_surrogate(base, dims["dim_streamer"], "id_streamer",
                         "id_streamer", "sk_streamer", "sk_streamer")
    base = map_surrogate(base, dims["dim_jogo"], "id_jogo",
                         "id_jogo", "sk_jogo", "sk_jogo")
    base = map_tempo(base, "data_inicio", "sk_tempo")

    duracao = (
        (F.unix_timestamp("data_fim") - F.unix_timestamp("data_inicio")) / 60.0
    ).cast("double")

    return base.select(
        # Chaves substitutas (estrela)
        "sk_tempo", "sk_streamer", "sk_jogo",
        # Dimensao degenerada
        F.col("id_transmissao"),
        # Metricas
        F.col("pico_viewers"),
        F.round(duracao, 2).alias("duracao_minutos"),
    )


def build_fato_visualizacoes(spark, silver, dims):
    """Grao: 1 linha por visualizacao. Metrica: minutos_assistidos.

    O streamer e o jogo vem da transmissao assistida (transmissoes), permitindo
    analisar audiencia por streamer/jogo direto neste fato.
    """
    from pyspark.sql import functions as F

    transm = silver["transmissoes"].select(
        F.col("id_transmissao").alias("_id_tr"),
        F.col("id_streamer").alias("_id_streamer"),
        F.col("id_jogo").alias("_id_jogo"),
    )

    base = (
        silver["visualizacoes"]
        .join(transm, F.col("id_transmissao") == F.col("_id_tr"), "left")
    )
    base = map_surrogate(base, dims["dim_viewer"], "id_viewer",
                         "id_viewer", "sk_viewer", "sk_viewer")
    base = map_surrogate(base, dims["dim_streamer"], "_id_streamer",
                         "id_streamer", "sk_streamer", "sk_streamer")
    base = map_surrogate(base, dims["dim_jogo"], "_id_jogo",
                         "id_jogo", "sk_jogo", "sk_jogo")
    base = map_tempo(base, "data_hora", "sk_tempo")

    return base.select(
        "sk_tempo", "sk_viewer", "sk_streamer", "sk_jogo",
        F.col("id_visualizacao"),
        F.col("id_transmissao"),
        F.col("minutos_assistidos"),
    )


def build_fato_doacoes(spark, silver, dims):
    """Grao: 1 linha por doacao. Metrica: valor."""
    from pyspark.sql import functions as F

    base = silver["doacoes"]
    base = map_surrogate(base, dims["dim_viewer"], "id_viewer",
                         "id_viewer", "sk_viewer", "sk_viewer")
    base = map_surrogate(base, dims["dim_streamer"], "id_streamer",
                         "id_streamer", "sk_streamer", "sk_streamer")
    base = map_tempo(base, "data_hora", "sk_tempo")

    return base.select(
        "sk_tempo", "sk_viewer", "sk_streamer",
        F.col("id_doacao"),
        F.col("id_transmissao"),
        F.col("valor"),
    )


def build_fato_assinaturas(spark, silver, dims):
    """Grao: 1 linha por assinatura. Metricas: valor_mensal, duracao_dias.

    Inclui o flag ``ativa`` (assinatura sem data_fim) e o ``tipo`` (tier) como
    atributo de fato (junk dimension simplificada).
    """
    from pyspark.sql import functions as F

    base = silver["assinaturas"]
    base = map_surrogate(base, dims["dim_viewer"], "id_viewer",
                         "id_viewer", "sk_viewer", "sk_viewer")
    base = map_surrogate(base, dims["dim_streamer"], "id_streamer",
                         "id_streamer", "sk_streamer", "sk_streamer")
    base = map_tempo(base, "data_inicio", "sk_tempo")

    return base.select(
        "sk_tempo", "sk_viewer", "sk_streamer",
        F.col("id_assinatura"),
        F.col("tipo"),
        F.col("valor_mensal"),
        F.datediff(F.col("data_fim"), F.col("data_inicio")).alias("duracao_dias"),
        F.col("data_fim").isNull().alias("ativa"),
    )


FACT_BUILDERS = {
    "fato_transmissoes": build_fato_transmissoes,
    "fato_visualizacoes": build_fato_visualizacoes,
    "fato_doacoes": build_fato_doacoes,
    "fato_assinaturas": build_fato_assinaturas,
}

# Tabelas Silver necessarias para construir cada objeto da Gold.
SILVER_DEPENDENCIES = {
    "dim_plataforma": ("plataformas",),
    "dim_streamer": ("streamers", "plataformas"),
    "dim_viewer": ("viewers",),
    "dim_jogo": ("jogos",),
    "dim_tempo": ("transmissoes", "visualizacoes", "doacoes", "assinaturas"),
    "fato_transmissoes": ("transmissoes",),
    "fato_visualizacoes": ("visualizacoes", "transmissoes"),
    "fato_doacoes": ("doacoes",),
    "fato_assinaturas": ("assinaturas",),
}


# ----------------------------------------------------------------------
# Orquestracao
# ----------------------------------------------------------------------


def parse_object_names(values) -> list[str]:
    names: list[str] = []
    for value in values:
        for name in value.split(","):
            name = name.strip()
            if name:
                names.append(name)
    return names


def _required_silver_tables(objects) -> list[str]:
    needed: set[str] = set()
    for obj in objects:
        needed.update(SILVER_DEPENDENCIES[obj])
    return sorted(needed)


@dataclass(frozen=True)
class ObjectResult:
    name: str
    rows: int
    unknown_fk: int  # linhas de fato apontando para o membro desconhecido (-1)


def _count_unknown_fk(df) -> int:
    from pyspark.sql import functions as F

    sk_cols = [c for c in df.columns if c.startswith("sk_")]
    if not sk_cols:
        return 0
    cond = reduce(
        lambda a, b: a | b, [F.col(c) == F.lit(UNKNOWN_SK) for c in sk_cols]
    )
    return df.filter(cond).count()


def run(spark, config: GoldConfig, objects) -> list[ObjectResult]:
    # Dimensoes pedidas + as dimensoes exigidas pelos fatos pedidos (para o lookup).
    requested = list(objects)
    fact_requested = [o for o in requested if o in FACTS]
    dims_needed = set(o for o in requested if o in DIMENSIONS)
    if fact_requested:
        # Fatos precisam das 5 dimensoes para traduzir as chaves naturais.
        dims_needed.update(DIMENSIONS)

    dims_to_build = [d for d in DIMENSIONS if d in dims_needed]

    silver_tables = _required_silver_tables(dims_to_build + fact_requested)
    print(f"Lendo Silver: {', '.join(silver_tables)}\n")
    silver = {t: read_silver(spark, config, t) for t in silver_tables}

    results: list[ObjectResult] = []
    dims: dict = {}

    # 1) Dimensoes (construidas e persistidas; ficam em memoria para os fatos).
    for name in dims_to_build:
        dim = DIMENSION_BUILDERS[name](spark, silver)
        dim = add_gold_audit(dim)
        dim.cache()
        rows = dim.count()
        dims[name] = dim
        if name in requested:
            write_delta(dim, f"s3a://{config.gold_bucket}/{name}")
            results.append(ObjectResult(name=name, rows=rows, unknown_fk=0))
            print(f"- {name}: {rows} linhas")

    # 2) Fatos.
    for name in fact_requested:
        fato = FACT_BUILDERS[name](spark, silver, dims)
        fato = add_gold_audit(fato)
        fato.cache()
        rows = fato.count()
        unknown = _count_unknown_fk(fato)
        write_delta(fato, f"s3a://{config.gold_bucket}/{name}")
        results.append(ObjectResult(name=name, rows=rows, unknown_fk=unknown))
        suffix = f" | FKs desconhecidas (-1): {unknown}" if unknown else ""
        print(f"- {name}: {rows} linhas{suffix}")

    return results


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Le as tabelas Delta da Silver e materializa o modelo estrela (Kimball) "
            "na Gold: 5 dimensoes e 4 fatos, em Delta Lake."
        ),
    )
    parser.add_argument(
        "--objects",
        nargs="*",
        default=[],
        help=(
            "Objetos da Gold a (re)construir (separados por espaco ou virgula). "
            "Por padrao constroi todos. Conhecidos: " + ", ".join(ALL_OBJECTS) + "."
        ),
    )
    parser.add_argument(
        "--silver-bucket",
        help="Sobrescreve o bucket de origem (padrao: MINIO_SILVER_BUCKET ou 'silver').",
    )
    parser.add_argument(
        "--gold-bucket",
        help="Sobrescreve o bucket de destino (padrao: MINIO_GOLD_BUCKET ou 'gold').",
    )
    return parser


def main() -> None:
    load_environment()

    args = build_parser().parse_args()
    config = GoldConfig.from_env()

    if args.silver_bucket:
        config = GoldConfig(**{**config.__dict__, "silver_bucket": args.silver_bucket})
    if args.gold_bucket:
        config = GoldConfig(**{**config.__dict__, "gold_bucket": args.gold_bucket})

    selected = parse_object_names(args.objects) or list(ALL_OBJECTS)
    unknown = [o for o in selected if o not in ALL_OBJECTS]
    if unknown:
        raise SystemExit(
            f"Objeto(s) desconhecido(s): {', '.join(unknown)}. "
            f"Conhecidos: {', '.join(ALL_OBJECTS)}."
        )

    # Mantem a ordem topologica (dimensoes antes dos fatos).
    selected = [o for o in ALL_OBJECTS if o in selected]

    print(f"Silver: s3a://{config.silver_bucket}/<tabela>/")
    print(f"Gold  : s3a://{config.gold_bucket}/<dim_ou_fato>/")
    print(f"Objetos a construir: {len(selected)}\n")

    spark = build_spark_session(app_name="silver-to-gold")
    try:
        results = run(spark, config, selected)
    finally:
        spark.stop()

    total_unknown = sum(r.unknown_fk for r in results)
    print(
        f"\nModelagem Silver -> Gold concluida. "
        f"Objetos gravados: {len(results)} | FKs desconhecidas totais: {total_unknown}."
    )


if __name__ == "__main__":
    main()
