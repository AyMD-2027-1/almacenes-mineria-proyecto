"""
Sube a staging las fuentes que se descargan a mano de un sitio: los CSV de
Google Trends (interés por estado de cada macrogénero y de cada artista, una
descarga por año) y la prevalencia de la ENDIREH 2021 que extrae
src/ingestion/endireh.py del tabulado de INEGI.

Los archivos se suben sin cambios (todas las columnas como texto), igual que
las demás tablas de staging. La carga usa WRITE_EMPTY: si la tabla ya existe
en BigQuery no se toca y el script avisa, así que nunca sobrescribe. Además
deja la copia local en data/data-raw/ para que la ingesta no tenga que
volver a consultar BigQuery.

Este script se ejecuta desde pipeline/ con:
    python -m src.ingestion.carga_staging
    python -m src.ingestion.carga_staging --solo-local
    python -m src.ingestion.carga_staging --tablas stg_endutih_usuarios_entidad_anio
La opción --solo-local guarda la copia en data/data-raw/ sin tocar BigQuery.
"""

import argparse
import sys
from pathlib import Path

import polars as pl
from google.api_core.exceptions import Conflict
from google.cloud import bigquery

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from config.parametros import ARCHIVOS_STAGING, DATASETS, PROYECTO_GCP, TABLAS_STAGING  # noqa: E402
from config.rutas import RUTA_DATA_RAW, RUTA_DESCARGAS, ruta_raw  # noqa: E402
from src.ingestion.ingesta import crear_cliente  # noqa: E402
from src.registro import registrar_etapa, seccion  # noqa: E402

ESTADOS = 32


def leer_csv(tabla: str, archivo: str) -> pl.DataFrame:
    """Lee un CSV descargado como texto y revisa que tenga lo esperado.

    En los de Trends revisa además que cada descarga traiga los 32 estados.

    Args:
        tabla (str): tabla de staging a la que va el archivo.
        archivo (str): ruta del CSV dentro de data/descargas/.

    Returns:
        pl.DataFrame: el archivo con todas sus columnas como texto.
    """
    datos = pl.read_csv(RUTA_DESCARGAS / archivo, infer_schema=False)
    esperadas = TABLAS_STAGING[tabla]["columnas"]
    assert datos.columns == esperadas, f"{archivo}: columnas {datos.columns}, se esperaban {esperadas}"
    if tabla.startswith("stg_trends"):
        por_descarga = datos.group_by(datos.columns[0], "anio").len()["len"].unique().to_list()
        assert por_descarga == [ESTADOS], f"{archivo}: hay descargas que no traen los {ESTADOS} estados"
    return datos


def subir(cliente: bigquery.Client, tabla: str, ruta: Path) -> bool:
    """Carga un Parquet a staging con un load job, solo si la tabla no existe.

    Args:
        cliente (bigquery.Client): cliente de BigQuery.
        tabla (str): nombre de la tabla en el dataset de staging.
        ruta (Path): archivo Parquet a cargar.

    Returns:
        bool: True si se cargó; False si la tabla ya existía y se dejó igual.
    """
    destino = f"{PROYECTO_GCP}.{DATASETS['staging']}.{tabla}"
    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_EMPTY,
    )
    try:
        with open(ruta, "rb") as archivo:
            cliente.load_table_from_file(archivo, destino, job_config=config).result()
    except Conflict:
        return False
    return True


def ejecutar_carga_staging(solo_local: bool = False, tablas: list[str] | None = None) -> None:
    """Guarda la copia local de cada archivo descargado y la sube a staging.

    Args:
        solo_local (bool): si es True, no se conecta a BigQuery.
        tablas (list[str] | None): tablas a procesar; None procesa todas las
            de `archivos_staging`. Sirve para no subir un archivo cuya
            descarga todavía no termina.
    """
    seccion("Archivos descargados a staging")
    RUTA_DATA_RAW.mkdir(parents=True, exist_ok=True)
    cliente = None if solo_local else crear_cliente()
    for tabla, archivo in ARCHIVOS_STAGING.items():
        if tablas and tabla not in tablas:
            continue
        if not (RUTA_DESCARGAS / archivo).exists():
            print(f"{tabla:<30} falta {archivo}; se omite")
            continue
        datos = leer_csv(tabla, archivo)
        ruta = ruta_raw(tabla)
        datos.write_parquet(ruta)
        estado = "solo copia local"
        if cliente is not None:
            estado = "cargada a staging" if subir(cliente, tabla, ruta) else "ya existía en staging, no se tocó"
        print(f"{tabla:<30} {datos.height:>7,} filas  {estado}")
        registrar_etapa("ingesta", tabla, None, datos.height, f"archivo {archivo}; {estado}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sube a staging los archivos descargados a mano.")
    parser.add_argument("--solo-local", action="store_true", help="guarda la copia local sin tocar BigQuery")
    parser.add_argument("--tablas", nargs="+", choices=list(ARCHIVOS_STAGING), help="solo estas tablas")
    args = parser.parse_args()
    ejecutar_carga_staging(solo_local=args.solo_local, tablas=args.tablas)
