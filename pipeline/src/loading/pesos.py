"""
Peso de cada entidad para repartir el consumo nacional, con la fórmula de E1:

    peso(e) = Trends(e) × usuarios(e) / suma sobre las 32 entidades de Trends × usuarios

Trends dice qué tanto pesa un artista o un género dentro de las búsquedas de
cada estado; multiplicarlo por los usuarios de internet lo acerca a un
volumen. Los pesos de las 32 entidades suman 1, así que repartir con ellos
conserva el consumo nacional.

Para cada artista y año el peso sale de un solo nivel, sin mezclar niveles
entre entidades:
    artista_anio: Trends del artista, si está entre los más escuchados del año
        en la muestra y tiene valor en suficientes entidades;
    genero_anio: Trends de su macrogénero, con la misma regla de entidades;
    usuarios_internet: solo los usuarios de internet de cada entidad.

Este script se ejecuta desde pipeline/ con:
    python -m src.loading.pesos
"""

import sys
from pathlib import Path

import polars as pl

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from config.parametros import ARTISTAS_POR_ANIO, ENTIDADES_MINIMAS  # noqa: E402
from config.rutas import (  # noqa: E402
    RUTA_LIMPIO_CANCIONES,
    RUTA_LIMPIO_CONSUMO,
    RUTA_LIMPIO_CONTEXTO,
    RUTA_LIMPIO_ENTIDADES,
    RUTA_PESOS_ENTIDAD,
    ruta_raw,
)
from src.registro import registrar_etapa, seccion  # noqa: E402

pl.Config.set_tbl_rows(20)

ORDEN_ORIGEN = ["artista_anio", "genero_anio", "usuarios_internet"]


def usuarios_internet() -> pl.DataFrame:
    """Lee los usuarios de internet de cada entidad y año (ENDUTIH, 2019 interpolado).

    Returns:
        pl.DataFrame: columnas `cve_ent`, `anio` (int) y `usuarios` (float).
    """
    return pl.read_parquet(RUTA_LIMPIO_CONTEXTO).select(
        "cve_ent",
        pl.col("anio").cast(pl.Int64),
        pl.col("usuarios_internet").cast(pl.Float64).alias("usuarios"),
    )


def consumo_artista_anio() -> pl.DataFrame:
    """Suma los streams nacionales de cada artista por año y marca a los más escuchados.

    Returns:
        pl.DataFrame: columnas `artista`, `macrogenero`, `anio`, `streams` y
            `es_top` (bool, entre los `ARTISTAS_POR_ANIO` con más streams del año).
    """
    canciones = pl.read_parquet(RUTA_LIMPIO_CANCIONES).select("track_id", "artista", "macrogenero")
    return (
        pl.read_parquet(RUTA_LIMPIO_CONSUMO)
        .join(canciones, on="track_id")
        .group_by("artista", "macrogenero", pl.col("fecha").dt.year().cast(pl.Int64).alias("anio"))
        .agg(pl.col("streams").sum())
        .with_columns(
            (pl.col("streams").rank("ordinal", descending=True).over("anio") <= ARTISTAS_POR_ANIO).alias("es_top")
        )
    )


def trends_validos(tabla: str, llave: str) -> pl.DataFrame:
    """Lee una tabla de Trends de staging y conserva las descargas con valor en suficientes entidades.

    Args:
        tabla (str): tabla de staging (macrogénero o artista).
        llave (str): columna que identifica el tema (`macrogenero` o `artista`).

    Returns:
        pl.DataFrame: columnas de la llave, `anio`, `cve_ent` e `interes`
            (int), solo de las descargas que pasan la regla.
    """
    claves = pl.read_parquet(RUTA_LIMPIO_ENTIDADES).select("cve_ent", pl.col("iso_3166_2").alias("geo_code"))
    trends = (
        pl.read_parquet(ruta_raw(tabla))
        .with_columns(pl.col("anio").cast(pl.Int64), pl.col("interes").cast(pl.Int64))
        .join(claves, on="geo_code", how="left")
    )
    assert trends["cve_ent"].null_count() == 0, f"{tabla}: hay códigos de Trends sin entidad"
    con_valor = trends.group_by(llave, "anio").agg((pl.col("interes") > 0).sum().alias("entidades_con_valor"))
    validas = con_valor.filter(pl.col("entidades_con_valor") >= ENTIDADES_MINIMAS).select(llave, "anio")
    print(f"{tabla}: {validas.height} de {con_valor.height} descargas con valor en {ENTIDADES_MINIMAS} entidades o más")
    return trends.join(validas, on=[llave, "anio"]).select(llave, "anio", "cve_ent", "interes")


def calcular_pesos() -> pl.DataFrame:
    """Calcula el peso de las 32 entidades para cada artista y año de la muestra.

    Returns:
        pl.DataFrame: una fila por artista, año y entidad, con `origen_peso`,
            `interes_busqueda` (int, nulo si el peso sale de los usuarios) y
            `peso` (float).
    """
    seccion("Pesos por entidad")
    artistas = consumo_artista_anio()
    usuarios = usuarios_internet()
    por_artista = trends_validos("stg_trends_artista_anio", "artista")
    por_genero = trends_validos("stg_trends_macrogenero_anio", "macrogenero")

    candidatos = artistas.join(usuarios, on="anio")
    nivel_artista = (
        candidatos.filter(pl.col("es_top"))
        .join(por_artista, on=["artista", "anio", "cve_ent"])
        .with_columns(pl.lit("artista_anio").alias("origen_peso"))
    )
    nivel_genero = candidatos.join(por_genero, on=["macrogenero", "anio", "cve_ent"]).with_columns(
        pl.lit("genero_anio").alias("origen_peso")
    )
    nivel_usuarios = candidatos.with_columns(
        pl.lit(None, dtype=pl.Int64).alias("interes"), pl.lit("usuarios_internet").alias("origen_peso")
    )
    columnas = ["artista", "anio", "cve_ent", "origen_peso", "interes", "usuarios"]
    todos = pl.concat([nivel_artista.select(columnas), nivel_genero.select(columnas), nivel_usuarios.select(columnas)])

    elegido = (
        todos.with_columns(pl.col("origen_peso").replace_strict(ORDEN_ORIGEN, list(range(3))).alias("prioridad"))
        .with_columns((pl.col("prioridad") == pl.col("prioridad").min().over("artista", "anio")).alias("gana"))
        .filter("gana")
    )
    pesos = (
        elegido.with_columns(pl.coalesce(pl.col("interes"), pl.lit(1)).mul(pl.col("usuarios")).alias("masa"))
        .with_columns((pl.col("masa") / pl.col("masa").sum().over("artista", "anio")).alias("peso"))
        .select("artista", "anio", "cve_ent", "origen_peso", pl.col("interes").alias("interes_busqueda"), "peso")
        .sort("anio", "artista", "cve_ent")
    )

    sumas = pesos.group_by("artista", "anio").agg(pl.col("peso").sum(), pl.len())
    assert (sumas["len"] == 32).all(), "hay artistas y años sin las 32 entidades"
    assert ((sumas["peso"] - 1).abs() < 1e-9).all(), "los pesos de un artista y año no suman 1"
    assert pesos.select(pl.struct("artista", "anio", "cve_ent").is_unique().all()).item()

    resumen = (
        pesos.select("artista", "anio", "origen_peso").unique()
        .join(artistas.select("artista", "anio", "streams"), on=["artista", "anio"])
        .group_by("origen_peso").agg(pl.len().alias("artistas_anio"), pl.col("streams").sum())
        .with_columns((pl.col("streams") / pl.col("streams").sum()).alias("parte_de_streams"))
        .sort("origen_peso")
    )
    print(resumen)
    return pesos


def ejecutar_pesos() -> pl.DataFrame:
    """Calcula y guarda los pesos por entidad.

    Returns:
        pl.DataFrame: los pesos.
    """
    pesos = calcular_pesos()
    pesos.write_parquet(RUTA_PESOS_ENTIDAD)
    registrar_etapa("preparacion", "pesos_entidad", None, pesos.height,
                    "reparto del consumo por artista y año con Trends y usuarios de internet")
    print(f"{RUTA_PESOS_ENTIDAD.name}: {pesos.height:,} filas")
    return pesos


if __name__ == "__main__":
    ejecutar_pesos()
