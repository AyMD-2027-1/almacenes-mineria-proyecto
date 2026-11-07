"""
Extrae de los tabulados anuales de la ENDUTIH (INEGI, "Usuarios de internet,
por entidad federativa, según sexo") el número de usuarios de internet de 6
años y más de cada entidad y año, de 2015 a 2025.

Cada archivo viene con formato de presentación: títulos, encabezados en
varias filas y notas al pie. Aquí solo se toma la columna "Total, Absolutos"
de las filas de entidad, sin cambiar ningún valor; el resultado es el CSV que
se sube a staging como stg_endutih_usuarios_entidad_anio.

INEGI no publica tabulados de 2019, así que ese año no aparece; la limpieza lo
interpola entre 2018 y 2020.

Este script se ejecuta desde pipeline/ con:
    python -m src.ingestion.endutih
"""

import re
import sys
from pathlib import Path

import polars as pl

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from config.parametros import ARCHIVOS_STAGING, ENDUTIH_TABULADOS  # noqa: E402
from config.rutas import RUTA_DESCARGAS  # noqa: E402
from src.registro import seccion  # noqa: E402

ENTIDADES = 33
NACIONAL = "Estados Unidos Mexicanos"
TABLA = "stg_endutih_usuarios_entidad_anio"


def leer_tabulado(archivo: Path) -> pl.DataFrame:
    """Toma de un tabulado el total de usuarios de cada entidad.

    La primera hoja trae el nombre de la entidad en la primera columna y el
    total absoluto en la tercera; las filas de títulos, encabezados y notas no
    tienen un número en esa columna y se descartan.

    Args:
        archivo (Path): tabulado de un año, con el año al inicio del nombre.

    Returns:
        pl.DataFrame: columnas `anio`, `entidad`, `usuarios_internet` y
            `archivo`, todas como texto, una fila por entidad y la nacional.
    """
    anio = re.match(r"(\d{4})_", archivo.name).group(1)
    hoja = pl.read_excel(archivo, sheet_id=1, has_header=False, infer_schema_length=0)
    entidad, total = hoja.columns[0], hoja.columns[2]
    filas = hoja.filter(
        pl.col(entidad).is_not_null() & pl.col(total).str.contains(r"^\d+(\.0+)?$")
    ).select(
        pl.lit(anio).alias("anio"),
        pl.col(entidad).str.strip_chars().alias("entidad"),
        pl.col(total).str.replace(r"\.0+$", "").alias("usuarios_internet"),
        pl.lit(archivo.name).alias("archivo"),
    )
    assert filas.height == ENTIDADES, f"{archivo.name}: {filas.height} entidades, se esperaban {ENTIDADES}"
    assert NACIONAL in filas["entidad"].to_list(), f"{archivo.name}: falta el total nacional"
    return filas


def ejecutar_endutih() -> pl.DataFrame:
    """Junta los tabulados de todos los años en un CSV y revisa que cuadren.

    Returns:
        pl.DataFrame: la tabla que se sube a staging.
    """
    seccion("ENDUTIH: usuarios de internet por entidad y año")
    carpeta = RUTA_DESCARGAS / ENDUTIH_TABULADOS["carpeta"]
    archivos = sorted(carpeta.glob(ENDUTIH_TABULADOS["patron"]))
    tabla = pl.concat([leer_tabulado(a) for a in archivos])

    usuarios = tabla.with_columns(pl.col("usuarios_internet").cast(pl.Int64))
    estados = usuarios.filter(pl.col("entidad") != NACIONAL).group_by("anio").agg(pl.col("usuarios_internet").sum())
    nacional = usuarios.filter(pl.col("entidad") == NACIONAL).select("anio", pl.col("usuarios_internet").alias("nacional"))
    cuadre = estados.join(nacional, on="anio").with_columns((pl.col("usuarios_internet") - pl.col("nacional")).abs().alias("diferencia"))
    assert (cuadre["diferencia"] <= ENTIDADES).all(), "la suma de los estados no da el total nacional"

    destino = RUTA_DESCARGAS / ARCHIVOS_STAGING[TABLA]
    tabla.write_csv(destino)
    print(nacional.sort("anio"))
    print(f"{destino.name}: {tabla.height:,} filas de {len(archivos)} años")
    return tabla


if __name__ == "__main__":
    ejecutar_endutih()
