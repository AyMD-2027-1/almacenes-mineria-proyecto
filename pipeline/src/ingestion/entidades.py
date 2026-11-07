"""
Catálogo de entidades: relaciona cada forma de escribir un estado en las
distintas fuentes con su clave INEGI, tomando como referencia los nombres y
claves del SESNSP.
"""

import sys
from pathlib import Path

import polars as pl

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from config.parametros import EQUIVALENCIAS_ENTIDAD, NOMBRES_NACIONALES  # noqa: E402
from config.rutas import ruta_raw  # noqa: E402


def normalizar_nombre(columna: pl.Expr) -> pl.Expr:
    """Lleva un nombre de entidad a mayúsculas, sin acentos ni espacios sobrantes.

    Args:
        columna (pl.Expr): expresión con el nombre original.

    Returns:
        pl.Expr: expresión con el nombre normalizado.
    """
    return (
        columna.str.strip_chars()
        .str.to_uppercase()
        .str.replace_many(["Á", "É", "Í", "Ó", "Ú", "Ü"], ["A", "E", "I", "O", "U", "U"])
    )


def construir_catalogo() -> pl.DataFrame:
    """Arma el catálogo de nombres normalizados y su clave INEGI.

    Incluye los 32 nombres del SESNSP, las variantes declaradas en
    config.yaml y los nombres que usan las fuentes para el total del país,
    que reciben la clave 0.

    Returns:
        pl.DataFrame: columnas `nombre` (str, normalizado), `cve_ent` (int) y
            `entidad` (str, nombre oficial del SESNSP).
    """
    sesnsp = (
        pl.read_parquet(ruta_raw("stg_sesnsp_delitos"), columns=["clave_ent", "entidad"])
        .unique()
        .select(
            normalizar_nombre(pl.col("entidad")).alias("nombre"),
            pl.col("clave_ent").cast(pl.Int64).alias("cve_ent"),
            pl.col("entidad"),
        )
    )
    variantes = (
        pl.DataFrame({"nombre": list(EQUIVALENCIAS_ENTIDAD), "referencia": list(EQUIVALENCIAS_ENTIDAD.values())})
        .join(sesnsp.rename({"nombre": "referencia"}), on="referencia")
        .drop("referencia")
    )
    nacionales = pl.DataFrame(
        {"nombre": NOMBRES_NACIONALES, "cve_ent": [0] * len(NOMBRES_NACIONALES), "entidad": ["Nacional"] * len(NOMBRES_NACIONALES)}
    )
    catalogo = pl.concat([sesnsp, variantes, nacionales]).sort("cve_ent", "nombre")
    assert catalogo["nombre"].is_unique().all(), "un mismo nombre quedó con dos claves"
    assert sesnsp.height == 32, f"el SESNSP debería tener 32 entidades y tiene {sesnsp.height}"
    return catalogo


def asignar_clave(df: pl.DataFrame, columna: str, catalogo: pl.DataFrame) -> pl.DataFrame:
    """Agrega la clave INEGI a una tabla a partir de una columna con el nombre de la entidad.

    Los nombres que no están en el catálogo, como los valores reservados del
    RNPDNO, quedan con la clave nula.

    Args:
        df (pl.DataFrame): tabla con el nombre de la entidad.
        columna (str): columna que tiene el nombre.
        catalogo (pl.DataFrame): resultado de `construir_catalogo`.

    Returns:
        pl.DataFrame: la misma tabla con la columna `cve_ent` (int) al final.
    """
    claves = catalogo.select("nombre", "cve_ent")
    return (
        df.with_columns(normalizar_nombre(pl.col(columna)).alias("_nombre"))
        .join(claves, left_on="_nombre", right_on="nombre", how="left")
        .drop("_nombre")
    )
