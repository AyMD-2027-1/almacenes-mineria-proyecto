"""
Punto de entrada único del pipeline de E2: lleva los datos de staging en
BigQuery hasta las tablas del esquema estrella listas para cargarse.

Etapas, en orden (cada una lee lo que dejó la anterior):
    ingesta        staging de BigQuery a data/data-raw/ (solo lo que falte)
    calidad        diagnóstico de cada tabla de staging antes de limpiar
    ensamblado     dataset analítico al grano de E1
    eda            descriptiva, concentración y cobertura de la muestra
    limpieza       tratamientos y registro de limpieza
    reduccion      chi-cuadrada, correlación, PCA y tabla de decisión
    muestreo       decide si se muestrea (no aplica) y deja la muestra del repositorio
    verificacion   comparación antes y después de la preparación
    pesos          reparto del consumo nacional entre entidades
    estrella       dimensiones y hechos con el esquema del DDL
    carga          carga al DW, validación, consistencia y DataMart

La extracción de las fuentes que se bajan de un sitio (Kworb, MusicBrainz,
Google Trends, ENDIREH) no es parte del pipeline: se hace una vez y queda en
staging, que es de donde lo lee la ingesta (ver README).

Uso, desde pipeline/:
    python run_pipeline.py                     corre todas las etapas
    python run_pipeline.py --desde limpieza    empieza en una etapa
    python run_pipeline.py --solo ingesta      corre una sola etapa
    python run_pipeline.py --sin-carga         todo menos la carga al DW

La carga vacía y vuelve a llenar las tablas del DW, así que correr el
pipeline dos veces deja el mismo resultado.
"""

import argparse
import random
import time
from collections.abc import Callable

import numpy as np

from config.parametros import SEMILLA
from config.rutas import RUTA_REGISTRO_ETAPAS
from src.cleaning.limpieza import ejecutar_limpieza
from src.eda.inspeccion import ejecutar_inspeccion
from src.eda.cobertura import ejecutar_cobertura
from src.eda.concentracion import ejecutar_concentracion
from src.eda.descriptiva import ejecutar_descriptiva
from src.ingestion.ensamblado import ejecutar_ensamblado
from src.ingestion.ingesta import ejecutar_ingesta
from src.loading.carga_dw import ejecutar_carga_dw
from src.loading.estrella import ejecutar_estrella
from src.loading.pesos import ejecutar_pesos
from src.reduction.reduccion import ejecutar_reduccion
from src.registro import seccion
from src.sampling.muestreo import ejecutar_muestreo
from src.verification.verificacion import ejecutar_verificacion


def eda() -> None:
    """Corre las tres partes del análisis exploratorio."""
    ejecutar_descriptiva()
    ejecutar_concentracion()
    ejecutar_cobertura()


def carga() -> None:
    """Carga al DW reemplazando lo que haya, para que el pipeline sea idempotente."""
    ejecutar_carga_dw(reemplazar=True)


ETAPAS: dict[str, Callable[[], object]] = {
    "ingesta": ejecutar_ingesta,
    "calidad": ejecutar_inspeccion,
    "ensamblado": ejecutar_ensamblado,
    "eda": eda,
    "limpieza": ejecutar_limpieza,
    "reduccion": ejecutar_reduccion,
    "muestreo": ejecutar_muestreo,
    "verificacion": ejecutar_verificacion,
    "pesos": ejecutar_pesos,
    "estrella": ejecutar_estrella,
    "carga": carga,
}


def fijar_semilla(semilla: int) -> None:
    """Fija la semilla de los generadores aleatorios para que cada corrida sea igual.

    Args:
        semilla (int): semilla declarada en config.yaml.
    """
    random.seed(semilla)
    np.random.seed(semilla)


def elegir_etapas(desde: str | None, solo: str | None, sin_carga: bool = False) -> list[str]:
    """Decide qué etapas correr según los argumentos.

    Args:
        desde (str | None): primera etapa a correr; None empieza desde el inicio.
        solo (str | None): única etapa a correr; tiene prioridad sobre `desde`.
        sin_carga (bool): omitir la carga al DW.

    Returns:
        list[str]: nombres de las etapas, en orden.
    """
    nombres = list(ETAPAS)
    if solo:
        return [solo]
    elegidas = nombres[nombres.index(desde):] if desde else nombres
    return [e for e in elegidas if not (sin_carga and e == "carga")]


def ejecutar_pipeline(etapas: list[str]) -> None:
    """Corre las etapas indicadas, en orden, y reporta cuánto tardó cada una.

    Cuando se corre desde el principio, el registro de etapas se reinicia para
    que solo refleje esta corrida.

    Args:
        etapas (list[str]): nombres de las etapas.
    """
    fijar_semilla(SEMILLA)
    if etapas[0] == next(iter(ETAPAS)) and RUTA_REGISTRO_ETAPAS.exists():
        RUTA_REGISTRO_ETAPAS.unlink()
    tiempos = {}
    for nombre in etapas:
        seccion(f"ETAPA: {nombre}")
        inicio = time.perf_counter()
        ETAPAS[nombre]()
        tiempos[nombre] = time.perf_counter() - inicio
    seccion("Resumen")
    for nombre, segundos in tiempos.items():
        print(f"{nombre:<14} {segundos:>7.1f} s")
    print(f"Registro de etapas: {RUTA_REGISTRO_ETAPAS}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline de E2: de staging al esquema estrella.")
    grupo = parser.add_mutually_exclusive_group()
    grupo.add_argument("--desde", choices=list(ETAPAS), help="empieza en esta etapa")
    grupo.add_argument("--solo", choices=list(ETAPAS), help="corre solo esta etapa")
    parser.add_argument("--sin-carga", action="store_true", help="no carga al DW (no escribe en BigQuery)")
    args = parser.parse_args()
    ejecutar_pipeline(elegir_etapas(args.desde, args.solo, args.sin_carga))
