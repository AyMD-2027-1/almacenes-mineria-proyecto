"""
Funciones de salida que comparten todas las etapas del pipeline: títulos en
consola y registro de cuántas filas entran y salen de cada tabla.
"""

import sys
from datetime import datetime
from pathlib import Path

import polars as pl

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config.rutas import RUTA_REGISTRO_ETAPAS  # noqa: E402


def seccion(titulo: str) -> None:
    """Imprime un título de sección con formato.

    Args:
        titulo (str): texto del título.
    """
    print(f"\n=== {titulo} ===")


def registrar_etapa(
    etapa: str,
    tabla: str,
    filas_entrada: int | None,
    filas_salida: int,
    detalle: str,
) -> None:
    """Agrega una fila al registro de etapas (logs/registro_etapas.csv).

    Args:
        etapa (str): nombre de la etapa, por ejemplo "ingesta" o "limpieza".
        tabla (str): tabla sobre la que se trabajó.
        filas_entrada (int | None): filas antes de la etapa; None si la tabla
            no existía antes, como al leerla de BigQuery.
        filas_salida (int): filas después de la etapa.
        detalle (str): qué se hizo, en una frase corta.
    """
    fila = pl.DataFrame(
        {
            "fecha": [datetime.now().isoformat(timespec="seconds")],
            "etapa": [etapa],
            "tabla": [tabla],
            "filas_entrada": [filas_entrada],
            "filas_salida": [filas_salida],
            "detalle": [detalle],
        },
        schema_overrides={"filas_entrada": pl.Int64, "filas_salida": pl.Int64},
    )
    RUTA_REGISTRO_ETAPAS.parent.mkdir(parents=True, exist_ok=True)
    if RUTA_REGISTRO_ETAPAS.exists():
        fila = pl.concat([pl.read_csv(RUTA_REGISTRO_ETAPAS, schema=fila.schema), fila])
    fila.write_csv(RUTA_REGISTRO_ETAPAS)
