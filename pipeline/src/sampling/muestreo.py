"""
Etapa de muestreo. Con el tamaño del dataset limpio no hace falta muestrear:
cabe completo en memoria y la carga al almacén con load jobs no se cobra. La
etapa existe para que el pipeline lo diga y lo registre con las cifras de cada
corrida, en lugar de omitirlo en silencio.

Además deja en data/sample/ una muestra pequeña en CSV, la única parte de los
datos que se sube al repositorio, para que quien clone el proyecto vea la
forma de las tablas sin tener acceso a BigQuery.

Este script se ejecuta desde pipeline/ con:
    python -m src.sampling.muestreo
"""

import sys
from pathlib import Path

import polars as pl

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from config.parametros import SEMILLA  # noqa: E402
from config.rutas import (  # noqa: E402
    RUTA_DATA_SAMPLE,
    RUTA_LIMPIO_CANCIONES,
    RUTA_LIMPIO_CONSUMO,
    RUTA_LIMPIO_CONTEXTO,
    RUTA_LIMPIO_VIOLENCIA,
)
from src.registro import registrar_etapa, seccion  # noqa: E402

# Un dataset se muestrearía si pasara de este número de filas.
FILAS_LIMITE = 5_000_000

# Entidades y año de la muestra del repositorio: una del centro y una del sur.
ENTIDADES_MUESTRA = [9, 7]
ANIO_MUESTRA = 2023
CANCIONES_MUESTRA = 20


def decidir_muestreo() -> dict[str, int]:
    """Revisa el tamaño de cada tabla limpia y registra que no se muestrea.

    Returns:
        dict[str, int]: filas de cada tabla.
    """
    seccion("Muestreo")
    tablas = {
        "limpio_violencia": RUTA_LIMPIO_VIOLENCIA,
        "limpio_consumo": RUTA_LIMPIO_CONSUMO,
        "limpio_canciones": RUTA_LIMPIO_CANCIONES,
        "limpio_contexto": RUTA_LIMPIO_CONTEXTO,
    }
    filas = {nombre: pl.scan_parquet(ruta).select(pl.len()).collect().item() for nombre, ruta in tablas.items()}
    for nombre, n in filas.items():
        assert n < FILAS_LIMITE, f"{nombre} tiene {n:,} filas: revisar si conviene muestrear"
        print(f"{nombre:<18} {n:>8,} filas: no se muestrea")
        registrar_etapa("muestreo", nombre, n, n, f"no aplica: menos de {FILAS_LIMITE:,} filas; se usa completo")
    return filas


def escribir_muestra() -> None:
    """Guarda en data/sample/ una muestra pequeña y reproducible de cada tabla limpia."""
    RUTA_DATA_SAMPLE.mkdir(parents=True, exist_ok=True)
    violencia = pl.read_parquet(RUTA_LIMPIO_VIOLENCIA).filter(
        pl.col("cve_ent").is_in(ENTIDADES_MUESTRA) & (pl.col("fecha").dt.year() == ANIO_MUESTRA)
    )
    canciones = pl.read_parquet(RUTA_LIMPIO_CANCIONES).sample(CANCIONES_MUESTRA, seed=SEMILLA)
    consumo = pl.read_parquet(RUTA_LIMPIO_CONSUMO).join(canciones.select("track_id"), on="track_id")
    contexto = pl.read_parquet(RUTA_LIMPIO_CONTEXTO).filter(pl.col("cve_ent").is_in(ENTIDADES_MUESTRA))
    for nombre, datos in {"violencia": violencia, "canciones": canciones, "consumo": consumo, "contexto": contexto}.items():
        datos.write_csv(RUTA_DATA_SAMPLE / f"muestra_{nombre}.csv")
        print(f"muestra_{nombre}.csv: {datos.height:,} filas")


def ejecutar_muestreo() -> None:
    """Decide el muestreo y escribe la muestra del repositorio."""
    decidir_muestreo()
    escribir_muestra()


if __name__ == "__main__":
    ejecutar_muestreo()
