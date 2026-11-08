"""
Vistas del dataset analítico que usa el análisis exploratorio: tasas de
violencia por entidad y año, y consumo por canción y año. Se calculan aquí una
sola vez para que la estadística descriptiva y las medidas de concentración
trabajen sobre las mismas cifras.
"""

import sys
from pathlib import Path

import polars as pl

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from config.rutas import (  # noqa: E402
    RUTA_ANALITICO_CONSUMO,
    RUTA_ANALITICO_CONTEXTO,
    RUTA_ANALITICO_VIOLENCIA,
)

# Las tasas se expresan por cada 100 mil mujeres.
POR_CADA = 100_000

# Tipos de violencia del mapeo de E1 que se analizan; "control" es el homicidio
# doloso total, que sirve de referencia y no es violencia de género.
TIPOS_ANALIZADOS = ["feminicida", "sexual", "física", "sin distinguir"]


def violencia() -> pl.DataFrame:
    """Lee el dataset analítico de violencia y agrega el año.

    Returns:
        pl.DataFrame: una fila por entidad, delito o registro y mes, con la
            columna `anio` (int).
    """
    return pl.read_parquet(RUTA_ANALITICO_VIOLENCIA).with_columns(pl.col("fecha").dt.year().alias("anio"))


def consumo() -> pl.DataFrame:
    """Lee el dataset analítico de consumo y agrega el año.

    Returns:
        pl.DataFrame: una fila por canción y mes, con la columna `anio` (int).
    """
    return pl.read_parquet(RUTA_ANALITICO_CONSUMO).with_columns(pl.col("fecha").dt.year().alias("anio"))


def contexto() -> pl.DataFrame:
    """Lee el dataset analítico de contexto.

    Returns:
        pl.DataFrame: una fila por entidad y año.
    """
    return pl.read_parquet(RUTA_ANALITICO_CONTEXTO)


def tasas_entidad_anio() -> pl.DataFrame:
    """Calcula carpetas y tasas por 100 mil mujeres de cada tipo de violencia.

    Solo usa el SESNSP y los tipos de `TIPOS_ANALIZADOS`. La población es la de
    mujeres de CONAPO de la misma entidad y año.

    Returns:
        pl.DataFrame: una fila por entidad, año y tipo, con `carpetas` (int),
            `victimas_mujeres` (int), `poblacion_mujeres` (int) y
            `tasa_carpetas` y `tasa_victimas` (float, por 100 mil mujeres).
    """
    poblacion = contexto().select("cve_ent", pl.col("anio").cast(pl.Int32), "poblacion_mujeres")
    return (
        violencia()
        .filter((pl.col("origen") == "SESNSP") & pl.col("tipo_lgamvlv").is_in(TIPOS_ANALIZADOS))
        .group_by("cve_ent", "anio", "tipo_lgamvlv")
        .agg(pl.col("carpetas").sum(), pl.col("victimas_mujeres").sum())
        .join(poblacion, on=["cve_ent", "anio"], how="left")
        .with_columns(
            (pl.col("carpetas") / pl.col("poblacion_mujeres") * POR_CADA).alias("tasa_carpetas"),
            (pl.col("victimas_mujeres") / pl.col("poblacion_mujeres") * POR_CADA).alias("tasa_victimas"),
        )
        .sort("tipo_lgamvlv", "anio", "cve_ent")
    )


def consumo_cancion_anio() -> pl.DataFrame:
    """Suma los streams de cada canción por año.

    Solo quedan las combinaciones con streams mayores a cero, es decir, los
    años en que la canción estuvo en el chart.

    Returns:
        pl.DataFrame: una fila por canción y año, con artista, macrogénero,
            `streams` (float) y `semanas_en_chart` (int).
    """
    return (
        consumo()
        .group_by("track_id", "artista", "macrogenero", "anio")
        .agg(pl.col("streams").sum(), pl.col("semanas_en_chart").sum())
        .filter(pl.col("streams") > 0)
        .sort("anio", "track_id")
    )
