"""
Arma en local las tablas del esquema estrella de E1 a partir del dataset
limpio, con las llaves sustitutas y las columnas y tipos exactos del DDL
(sql/ddl_dw.sql). No se conecta a BigQuery: deja un Parquet por tabla en
data/data-processed/estrella/, listo para cargarse con un load job.

Tablas:
    dim_fecha, dim_entidad, dim_cancion, dim_tipo_violencia,
    dim_contexto_entidad, fact_violencia y fact_consumo_musical.
El hecho de consumo reparte los streams nacionales de cada canción entre las
32 entidades con los pesos de src/loading/pesos.py, que hay que correr antes.

Este script se ejecuta desde pipeline/ con:
    python -m src.loading.estrella
"""

import sys
from pathlib import Path

import polars as pl

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from config.parametros import (  # noqa: E402
    CATEGORIAS_MISONGYNY,
    MAPEO_LGAMVLV,
    REGIONES_BANXICO,
    TABLAS_TRENDS_ARTISTA,
    VENTANA_FIN,
    VENTANA_INICIO,
)
from config.rutas import (  # noqa: E402
    RUTA_ESTRELLA,
    RUTA_LIMPIO_CANCIONES,
    RUTA_LIMPIO_CONSUMO,
    RUTA_LIMPIO_CONTEXTO,
    RUTA_LIMPIO_ENTIDADES,
    RUTA_LIMPIO_VIOLENCIA,
    RUTA_PESOS_ENTIDAD,
    ruta_raw,
)
from src.ingestion.ensamblado import LLAVE_DELITO, PREVALENCIAS  # noqa: E402
from src.ingestion.generos import comparable  # noqa: E402
from src.loading.pesos import leer_trends  # noqa: E402
from src.registro import registrar_etapa, seccion  # noqa: E402

MESES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto",
         "septiembre", "octubre", "noviembre", "diciembre"]

# Llave del tipo de violencia que usan las canciones no misóginas o sin etiqueta.
NO_APLICA = -1

# Código del catálogo de fuentes de E1 que se guarda en la columna `fuente` de los hechos.
FUENTES = {"SESNSP": "F1", "RNPDNO": "F13"}
FUENTE_CONSUMO = "F4"

# Tolerancia para comprobar que el reparto conserva los streams nacionales.
TOLERANCIA_STREAMS = 1e-6

# Natural Earth y Google Trends usan el código anterior de la Ciudad de México;
# la norma ISO 3166-2 vigente es MX-CMX.
ISO_ANTERIOR = {"MX-DIF": "MX-CMX"}

# Tipos de la LGAMVLV que ninguna fuente mide, pero que P4 necesita nombrar.
SIN_FUENTE = ["económica o patrimonial", "mediática"]

ESQUEMAS = {
    "dim_fecha": {
        "fecha_key": pl.Int64, "fecha": pl.Date, "anio": pl.Int64, "semestre": pl.Int64, "trimestre": pl.Int64,
        "mes": pl.Int64, "nombre_mes": pl.String, "indice_mes": pl.Int64,
    },
    "dim_entidad": {
        "entidad_key": pl.Int64, "cve_ent": pl.Int64, "entidad": pl.String, "nombre_trends": pl.String,
        "iso_3166_2": pl.String, "region": pl.String, "geometry_wkt": pl.String,
    },
    "dim_cancion": {
        "cancion_key": pl.Int64, "spotify_id": pl.String, "clave_cancion": pl.String, "titulo": pl.String,
        "artista_principal": pl.String, "artistas": pl.String, "termino_trends": pl.String,
        "pais_artista": pl.String, "genero": pl.String, "macrogenero": pl.String, "etiquetas_fuente": pl.String,
        "tiene_letra": pl.Boolean, "en_corpus_misongyny": pl.Boolean, "etiqueta_misongyny": pl.String,
        "es_misogina": pl.Boolean, "categoria_misoginia": pl.String, "score_misoginia": pl.Float64,
        "origen_etiqueta": pl.String, "version_modelo": pl.String,
    },
    "dim_tipo_violencia": {
        "tipo_violencia_key": pl.Int64, "tipo_lgamvlv": pl.String, "ambito_lgamvlv": pl.String,
        "origen": pl.String, "categoria_origen": pl.String, "bien_juridico": pl.String, "tipo_delito": pl.String,
        "subtipo_delito": pl.String, "modalidad": pl.String, "medido_en_sesnsp": pl.Boolean,
        "es_control": pl.Boolean,
    },
    "dim_contexto_entidad": {
        "contexto_key": pl.Int64, "cve_ent": pl.Int64, "anio": pl.Int64, "es_nacional": pl.Boolean,
        "poblacion_total": pl.Int64, "poblacion_mujeres": pl.Int64, "poblacion_mujeres_15_mas": pl.Int64,
        "poblacion_hombres": pl.Int64, "pct_usuarios_internet": pl.Float64,
        "usuarios_internet_estimados": pl.Int64, "pct_viviendas_internet": pl.Float64,
        "pct_pobreza": pl.Float64, "indice_impunidad": pl.Float64, "prevalencia_total": pl.Float64,
        "prevalencia_psicologica": pl.Float64, "prevalencia_fisica": pl.Float64,
        "prevalencia_sexual": pl.Float64, "prevalencia_economica_patrimonial": pl.Float64,
    },
    "fact_consumo_musical": {
        "fecha_key": pl.Int64, "entidad_key": pl.Int64, "cancion_key": pl.Int64, "tipo_violencia_key": pl.Int64,
        "contexto_key": pl.Int64, "streams_estimados": pl.Float64, "peso_entidad": pl.Float64,
        "interes_busqueda": pl.Int64, "semanas_en_chart_nacional": pl.Int64, "mejor_posicion_nacional": pl.Int64,
        "origen_peso": pl.String, "fuente": pl.String,
    },
    "fact_violencia": {
        "fecha_key": pl.Int64, "entidad_key": pl.Int64, "tipo_violencia_key": pl.Int64,
        "contexto_key": pl.Int64, "carpetas": pl.Int64, "victimas_mujeres": pl.Int64,
        "victimas_mujeres_menores": pl.Int64, "victimas_hombres": pl.Int64, "fuente": pl.String,
    },
}


def ajustar_esquema(tabla: pl.DataFrame, nombre: str) -> pl.DataFrame:
    """Deja una tabla con las columnas, el orden y los tipos del DDL.

    Args:
        tabla (pl.DataFrame): tabla armada.
        nombre (str): nombre de la tabla en el DDL.

    Returns:
        pl.DataFrame: la tabla con exactamente las columnas del DDL.
    """
    esquema = ESQUEMAS[nombre]
    faltantes = set(esquema) - set(tabla.columns)
    assert not faltantes, f"{nombre}: faltan columnas {sorted(faltantes)}"
    return tabla.select([pl.col(c).cast(t) for c, t in esquema.items()])


def clave_fecha(fecha: pl.Expr) -> pl.Expr:
    """Convierte el primer día del mes en la llave AAAAMM01.

    Args:
        fecha (pl.Expr): fecha del primer día del mes.

    Returns:
        pl.Expr: llave entera, por ejemplo 20210301.
    """
    return fecha.dt.strftime("%Y%m%d").cast(pl.Int64)


def dim_fecha() -> pl.DataFrame:
    """Genera el calendario mensual de la ventana de análisis.

    Returns:
        pl.DataFrame: una fila por mes.
    """
    return pl.DataFrame({"fecha": pl.date_range(VENTANA_INICIO, VENTANA_FIN, "1mo", eager=True)}).with_columns(
        clave_fecha(pl.col("fecha")).alias("fecha_key"),
        pl.col("fecha").dt.year().alias("anio"),
        ((pl.col("fecha").dt.month() - 1) // 6 + 1).alias("semestre"),
        pl.col("fecha").dt.quarter().alias("trimestre"),
        pl.col("fecha").dt.month().alias("mes"),
        pl.col("fecha").dt.month().replace_strict(list(range(1, 13)), MESES).alias("nombre_mes"),
        (pl.col("fecha").dt.year() * 12 + pl.col("fecha").dt.month()).alias("indice_mes"),
    )


def dim_entidad() -> pl.DataFrame:
    """Arma las 32 entidades con su nombre en Trends, clave ISO, región y geometría.

    Returns:
        pl.DataFrame: una fila por entidad; la llave sustituta sigue el orden de la clave INEGI.
    """
    region = pl.DataFrame(
        [(cve, nombre) for nombre, claves in REGIONES_BANXICO.items() for cve in claves],
        schema={"cve_ent": pl.Int64, "region": pl.String}, orient="row",
    )
    trends = pl.read_parquet(ruta_raw("stg_trends_macrogenero_anio"), columns=["geo_code", "region_trends"]).unique()
    return (
        pl.read_parquet(RUTA_LIMPIO_ENTIDADES)
        .join(trends.rename({"geo_code": "iso_3166_2", "region_trends": "nombre_trends"}), on="iso_3166_2", how="left")
        .join(region, on="cve_ent", how="left")
        .with_columns(pl.col("iso_3166_2").replace(ISO_ANTERIOR))
        .sort("cve_ent")
        .with_row_index("entidad_key", offset=1)
    )


def dim_cancion() -> pl.DataFrame:
    """Arma las canciones con su género y su etiqueta de MiSonGyny.

    El género es la etiqueta más votada del artista en MusicBrainz; la letra
    y el clasificador todavía no existen, así que sus columnas quedan nulas.

    Returns:
        pl.DataFrame: una fila por canción del catálogo de Kworb.
    """
    categorias = {codigo: v["categoria"] for codigo, v in CATEGORIAS_MISONGYNY.items()}
    temas = pl.DataFrame(schema={"artista": pl.String, "termino_trends": pl.String})
    if any(ruta_raw(t).exists() for t in TABLAS_TRENDS_ARTISTA):
        temas = (leer_trends(TABLAS_TRENDS_ARTISTA).select("artista", "titulo_tema", "tema_trends")
                 .unique("artista").select("artista", pl.format("{} ({})", "titulo_tema", "tema_trends").alias("termino_trends")))
    return (
        pl.read_parquet(RUTA_LIMPIO_CANCIONES)
        .join(temas, on="artista", how="left")
        .with_columns(
            pl.col("track_id").alias("spotify_id"),
            pl.format("{}|{}", comparable(pl.col("artista")), comparable(pl.col("titulo"))).alias("clave_cancion"),
            pl.col("artista").alias("artista_principal"),
            pl.col("artista").alias("artistas"),
            pl.col("etiquetas_fuente").str.split(",").list.first().str.strip_chars().replace("", None).alias("genero"),
            pl.lit(None, dtype=pl.Boolean).alias("tiene_letra"),
            pl.col("etiqueta_tarea_1").alias("etiqueta_misongyny"),
            pl.col("etiqueta_tarea_2").replace_strict(categorias, default=None).alias("categoria_misoginia"),
            pl.lit(None, dtype=pl.Float64).alias("score_misoginia"),
            pl.lit(None, dtype=pl.String).alias("version_modelo"),
        )
        .sort("track_id")
        .with_row_index("cancion_key", offset=1)
    )


def dim_tipo_violencia(violencia: pl.DataFrame) -> pl.DataFrame:
    """Arma el catálogo de tipos de violencia de las tres fuentes y la fila de no aplica.

    Args:
        violencia (pl.DataFrame): dataset limpio de violencia.

    Returns:
        pl.DataFrame: una fila por delito del SESNSP a nivel modalidad, una
            del RNPDNO, una por categoría de MiSonGyny, los tipos sin fuente y
            la fila -1.
    """
    sesnsp = (
        violencia.filter(pl.col("origen") == "SESNSP").select(*LLAVE_DELITO, "tipo_lgamvlv").unique()
        .with_columns(
            pl.lit("SESNSP").alias("origen"),
            pl.format("{} / {}", "subtipo_de_delito", "modalidad").alias("categoria_origen"),
            pl.when(pl.col("subtipo_de_delito") == "Violencia familiar").then(pl.lit("familiar"))
            .when(pl.col("subtipo_de_delito").str.starts_with("Violencia de género")).then(pl.lit("otro"))
            .alias("ambito_lgamvlv"),
            pl.lit(True).alias("medido_en_sesnsp"),
            (pl.col("tipo_lgamvlv") == "control").alias("es_control"),
        )
        .rename({"bien_juridico_afectado": "bien_juridico", "tipo_de_delito": "tipo_delito",
                 "subtipo_de_delito": "subtipo_delito"})
        .sort("tipo_lgamvlv", "subtipo_delito", "modalidad")
    )
    otros = pl.DataFrame(
        [{"tipo_lgamvlv": "desaparición", "origen": "RNPDNO", "categoria_origen": "Mujer desaparecida (todos los estatus)"}]
        + [{"tipo_lgamvlv": v["tipo_lgamvlv"], "origen": "clasificador", "categoria_origen": v["categoria"]}
           for v in CATEGORIAS_MISONGYNY.values() if v["tipo_lgamvlv"] != "no aplica"]
        + [{"tipo_lgamvlv": t, "origen": "sin fuente", "categoria_origen": None} for t in SIN_FUENTE]
    ).with_columns(pl.lit(False).alias("medido_en_sesnsp"), pl.lit(False).alias("es_control"))
    catalogo = pl.concat([sesnsp, otros], how="diagonal").with_row_index("tipo_violencia_key", offset=1)
    no_aplica = pl.DataFrame({"tipo_violencia_key": [NO_APLICA], "tipo_lgamvlv": ["no aplica"], "origen": ["clasificador"],
                              "categoria_origen": ["canción no misógina o sin etiqueta"],
                              "medido_en_sesnsp": [False], "es_control": [False]})
    tabla = pl.concat([no_aplica, catalogo.with_columns(pl.col("tipo_violencia_key").cast(pl.Int64))], how="diagonal")
    assert set(MAPEO_LGAMVLV.values()) <= set(tabla["tipo_lgamvlv"]), "falta un tipo del mapeo de E1"
    return tabla


def dim_contexto_entidad() -> pl.DataFrame:
    """Arma el contexto por entidad y año, más una fila nacional por año.

    La fila nacional suma las poblaciones, pondera los porcentajes por
    población y toma la prevalencia nacional de la ENDIREH; no tiene hechos.

    Returns:
        pl.DataFrame: una fila por entidad (o nacional) y año.
    """
    estatal = pl.read_parquet(RUTA_LIMPIO_CONTEXTO).with_columns(pl.col("anio").cast(pl.Int64), pl.lit(False).alias("es_nacional"))
    ponderado = lambda c: (pl.col(c) * pl.col("poblacion_total")).sum() / pl.col("poblacion_total").filter(pl.col(c).is_not_null()).sum()  # noqa: E731
    nacional_endireh = (
        pl.read_parquet(ruta_raw("stg_endireh_2021_prevalencias"))
        .filter((pl.col("entidad") == "Estados Unidos Mexicanos") & (pl.col("persona_agresora") == "total")
                & (pl.col("medida") == "estimacion"))
        .select(pl.col("levantamiento").cast(pl.Int64).alias("anio"), pl.col("tipo_violencia").replace_strict(PREVALENCIAS),
                pl.col("valor").cast(pl.Float64))
        .pivot(on="tipo_violencia", index="anio", values="valor")
    )
    nacional = (
        estatal.group_by("anio")
        .agg(
            *[pl.col(c).sum() for c in ["poblacion_total", "poblacion_mujeres", "poblacion_mujeres_15_mas", "poblacion_hombres",
                                        "poblacion_6_mas", "usuarios_internet"]],
            *[ponderado(c).alias(c) for c in ["pct_viviendas_internet", "pct_pobreza"]],
        )
        .join(nacional_endireh, on="anio", how="left")
        .with_columns((pl.col("usuarios_internet") / pl.col("poblacion_6_mas") * 100).alias("pct_usuarios_internet"))
        .with_columns(pl.lit(0, dtype=pl.Int64).alias("cve_ent"), pl.lit(True).alias("es_nacional"))
    )
    return (
        pl.concat([estatal, nacional], how="diagonal_relaxed")
        .with_columns(pl.col("usuarios_internet").round().alias("usuarios_internet_estimados"))
        .sort("anio", "cve_ent")
        .with_row_index("contexto_key", offset=1)
    )


def fact_violencia(violencia: pl.DataFrame, tipos: pl.DataFrame, entidades: pl.DataFrame,
                   contexto: pl.DataFrame) -> pl.DataFrame:
    """Une el dataset limpio de violencia con las llaves de las dimensiones.

    Args:
        violencia (pl.DataFrame): dataset limpio de violencia.
        tipos (pl.DataFrame): resultado de `dim_tipo_violencia`.
        entidades (pl.DataFrame): resultado de `dim_entidad`.
        contexto (pl.DataFrame): resultado de `dim_contexto_entidad`.

    Returns:
        pl.DataFrame: una fila por entidad, delito o registro y mes.
    """
    llave_tipo = (
        tipos.filter(pl.col("origen").is_in(list(FUENTES)))
        .select("tipo_violencia_key", "origen", pl.col("tipo_lgamvlv"),
                pl.col("bien_juridico").alias("bien_juridico_afectado"), pl.col("tipo_delito").alias("tipo_de_delito"),
                pl.col("subtipo_delito").alias("subtipo_de_delito"), "modalidad")
    )
    rnpdno = llave_tipo.filter(pl.col("origen") == "RNPDNO").select("tipo_violencia_key", "origen")
    sesnsp = llave_tipo.filter(pl.col("origen") == "SESNSP").drop("tipo_lgamvlv")
    con_tipo = pl.concat([
        violencia.filter(pl.col("origen") == "SESNSP").join(sesnsp, on=["origen", *LLAVE_DELITO], how="left"),
        violencia.filter(pl.col("origen") == "RNPDNO").join(rnpdno, on="origen", how="left"),
    ])
    hechos = (
        con_tipo.with_columns(clave_fecha(pl.col("fecha")).alias("fecha_key"), pl.col("fecha").dt.year().cast(pl.Int64).alias("anio"),
                              pl.col("origen").replace_strict(FUENTES).alias("fuente"))
        .join(entidades.select("entidad_key", "cve_ent"), on="cve_ent", how="left")
        .join(contexto.filter(~pl.col("es_nacional")).select("contexto_key", "cve_ent", "anio"), on=["cve_ent", "anio"], how="left")
    )
    for llave in ["tipo_violencia_key", "entidad_key", "contexto_key"]:
        assert hechos[llave].null_count() == 0, f"fact_violencia: hay filas sin {llave}"
    assert hechos.height == violencia.height, "la unión con las dimensiones cambió el número de filas"
    return hechos


def fact_consumo_musical(canciones: pl.DataFrame, tipos: pl.DataFrame, entidades: pl.DataFrame,
                         contexto: pl.DataFrame) -> pl.DataFrame:
    """Reparte el consumo nacional de cada canción y mes entre las 32 entidades.

    Cada canción toma el peso de su artista en el año del mes. El tipo de
    violencia es el de la categoría de MiSonGyny cuando la canción es
    misógina; si no lo es, o no tiene etiqueta, es la fila -1.

    Args:
        canciones (pl.DataFrame): resultado de `dim_cancion`.
        tipos (pl.DataFrame): resultado de `dim_tipo_violencia`.
        entidades (pl.DataFrame): resultado de `dim_entidad`.
        contexto (pl.DataFrame): resultado de `dim_contexto_entidad`.

    Returns:
        pl.DataFrame: una fila por canción, entidad y mes.
    """
    consumo = pl.read_parquet(RUTA_LIMPIO_CONSUMO)
    pesos = pl.read_parquet(RUTA_PESOS_ENTIDAD)
    tipo_letra = tipos.filter(pl.col("origen") == "clasificador").select("tipo_violencia_key", pl.col("categoria_origen").alias("categoria_misoginia"))
    llaves_cancion = (
        canciones.join(tipo_letra, on="categoria_misoginia", how="left")
        .with_columns(
            pl.when(pl.col("es_misogina").fill_null(False)).then(pl.col("tipo_violencia_key"))
            .otherwise(pl.lit(NO_APLICA)).fill_null(NO_APLICA).alias("tipo_violencia_key")
        )
        .select("cancion_key", pl.col("spotify_id").alias("track_id"), pl.col("artista_principal").alias("artista"),
                "tipo_violencia_key")
    )
    hechos = (
        consumo.join(llaves_cancion, on="track_id", how="left")
        .with_columns(pl.col("fecha").dt.year().cast(pl.Int64).alias("anio"), clave_fecha(pl.col("fecha")).alias("fecha_key"))
        .join(pesos, on=["artista", "anio"], how="left")
        .join(entidades.select("entidad_key", "cve_ent"), on="cve_ent", how="left")
        .join(contexto.filter(~pl.col("es_nacional")).select("contexto_key", "cve_ent", "anio"), on=["cve_ent", "anio"], how="left")
        .with_columns(
            (pl.col("streams") * pl.col("peso")).alias("streams_estimados"),
            pl.col("peso").alias("peso_entidad"),
            pl.col("semanas_en_chart").alias("semanas_en_chart_nacional"),
            pl.col("mejor_posicion").alias("mejor_posicion_nacional"),
            pl.lit(FUENTE_CONSUMO).alias("fuente"),
        )
    )
    for llave in ["cancion_key", "entidad_key", "contexto_key", "peso_entidad"]:
        assert hechos[llave].null_count() == 0, f"fact_consumo_musical: hay filas sin {llave}"
    assert hechos.height == consumo.height * 32, "no todas las canciones y meses se repartieron entre las 32 entidades"
    diferencia = abs(hechos["streams_estimados"].sum() / consumo["streams"].sum() - 1)
    assert diferencia < TOLERANCIA_STREAMS, "el reparto no conserva los streams nacionales"
    return hechos


def ejecutar_estrella() -> dict[str, pl.DataFrame]:
    """Arma las dimensiones y fact_violencia, las revisa y las guarda.

    Returns:
        dict[str, pl.DataFrame]: las tablas con el esquema del DDL.
    """
    seccion("Esquema estrella en local")
    RUTA_ESTRELLA.mkdir(parents=True, exist_ok=True)
    violencia = pl.read_parquet(RUTA_LIMPIO_VIOLENCIA)
    entidades = dim_entidad()
    tipos = dim_tipo_violencia(violencia)
    contexto = dim_contexto_entidad()
    canciones = dim_cancion()
    tablas = {
        "dim_fecha": dim_fecha(),
        "dim_entidad": entidades,
        "dim_cancion": canciones,
        "dim_tipo_violencia": tipos,
        "dim_contexto_entidad": contexto,
        "fact_violencia": fact_violencia(violencia, tipos, entidades, contexto),
        "fact_consumo_musical": fact_consumo_musical(canciones, tipos, entidades, contexto),
    }
    llaves = {"dim_fecha": ["fecha_key"], "dim_entidad": ["entidad_key"], "dim_cancion": ["cancion_key"],
              "dim_tipo_violencia": ["tipo_violencia_key"], "dim_contexto_entidad": ["contexto_key"],
              "fact_violencia": ["fecha_key", "entidad_key", "tipo_violencia_key"],
              "fact_consumo_musical": ["fecha_key", "entidad_key", "cancion_key"]}
    for nombre, tabla in tablas.items():
        tabla = ajustar_esquema(tabla, nombre)
        assert tabla.select(pl.struct(llaves[nombre]).is_unique().all()).item(), f"{nombre}: la llave primaria se repite"
        tablas[nombre] = tabla
        tabla.write_parquet(RUTA_ESTRELLA / f"{nombre}.parquet")
        print(f"{nombre:<22} {tabla.height:>8,} filas  {tabla.width:>3} columnas")
        registrar_etapa("estrella", nombre, None, tabla.height, "armada en local con el esquema del DDL de E1")

    rangos = [
        (tablas["fact_consumo_musical"]["peso_entidad"].is_between(0, 1).all(), "peso_entidad fuera de [0, 1]"),
        ((tablas["fact_consumo_musical"]["streams_estimados"] >= 0).all(), "streams negativos"),
        ((tablas["fact_violencia"].select(pl.min_horizontal("carpetas", "victimas_mujeres")).drop_nulls() >= 0).to_series().all(),
         "conteos de violencia negativos"),
        (tablas["dim_contexto_entidad"].select(pl.col("^pct_.*$|^prevalencia_.*$")).unpivot().drop_nulls()["value"].is_between(0, 100).all(),
         "porcentajes fuera de [0, 100]"),
        (tablas["dim_fecha"]["mes"].is_between(1, 12).all(), "meses fuera de 1 a 12"),
    ]
    for correcto, mensaje in rangos:
        assert correcto, mensaje
    for hecho in ["fact_violencia", "fact_consumo_musical"]:
        assert tablas[hecho]["fecha_key"].is_in(tablas["dim_fecha"]["fecha_key"].implode()).all(), f"{hecho} tiene meses fuera de dim_fecha"
    violencia_f, consumo_f = tablas["fact_violencia"], tablas["fact_consumo_musical"]
    print(f"\nCarpetas en fact_violencia: {violencia_f['carpetas'].sum():,}; víctimas mujeres: {violencia_f['victimas_mujeres'].sum():,}")
    print(f"Streams estimados en fact_consumo_musical: {consumo_f['streams_estimados'].sum():,.0f}")
    return tablas


if __name__ == "__main__":
    ejecutar_estrella()
