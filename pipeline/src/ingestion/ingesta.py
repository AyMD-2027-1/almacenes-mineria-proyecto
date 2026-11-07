"""
Ingesta: copia las tablas de staging de BigQuery a archivos Parquet locales en
data/data-raw/, leyendo solo las columnas declaradas en config.yaml.

Este script se ejecuta desde pipeline/ con:
    python -m src.ingestion.ingesta
    python -m src.ingestion.ingesta --estimar
La opción --estimar solo calcula cuánto escanearía cada consulta, sin costo.
"""

import argparse
import sys
from pathlib import Path

import polars as pl
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from config.parametros import (  # noqa: E402
    DATASETS,
    MAX_BYTES_FACTURADOS,
    PROYECTO_GCP,
    TABLAS_STAGING,
    UBICACION,
    USAR_CACHE_LOCAL,
)
from config.rutas import RUTA_DATA_RAW, ruta_raw  # noqa: E402
from src.registro import registrar_etapa, seccion  # noqa: E402

pl.Config.set_tbl_rows(20)


def crear_cliente() -> bigquery.Client:
    """Crea el cliente de BigQuery con las credenciales de aplicación.

    Las credenciales se generan una sola vez con
    `gcloud auth application-default login`.

    Returns:
        bigquery.Client: cliente apuntando al proyecto y la ubicación del config.
    """
    return bigquery.Client(project=PROYECTO_GCP, location=UBICACION)


def construir_sql(tabla: str, columnas: list[str], filtro: str | None = None) -> str:
    """Arma la consulta que lee una tabla de staging.

    BigQuery cobra por las columnas que lee, no por las filas, así que se piden
    solo las necesarias.

    Args:
        tabla (str): nombre de la tabla en el dataset de staging.
        columnas (list[str]): columnas a leer.
        filtro (str | None): condición WHERE opcional, en SQL de BigQuery.

    Returns:
        str: la consulta lista para ejecutarse.
    """
    lista = ", ".join(f"`{c}`" for c in columnas)
    sql = f"SELECT {lista} FROM `{PROYECTO_GCP}.{DATASETS['staging']}.{tabla}`"
    if filtro:
        sql += f" WHERE {filtro}"
    return sql


def megabytes_estimados(cliente: bigquery.Client, sql: str) -> float:
    """Calcula cuánto escanearía una consulta sin ejecutarla (dry run, sin costo).

    Args:
        cliente (bigquery.Client): cliente de BigQuery.
        sql (str): consulta a evaluar.

    Returns:
        float: megabytes que la consulta procesaría.
    """
    config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    return cliente.query(sql, job_config=config).total_bytes_processed / 1e6


def extraer(cliente: bigquery.Client, sql: str) -> pl.DataFrame:
    """Ejecuta una consulta con tope de bytes y devuelve el resultado en polars.

    Args:
        cliente (bigquery.Client): cliente de BigQuery.
        sql (str): consulta a ejecutar.

    Returns:
        pl.DataFrame: resultado de la consulta, con las columnas como texto,
            tal como están en staging.
    """
    config = bigquery.QueryJobConfig(maximum_bytes_billed=MAX_BYTES_FACTURADOS)
    return pl.from_arrow(cliente.query(sql, job_config=config).to_arrow())


def ejecutar_ingesta(solo_estimar: bool = False) -> pl.DataFrame:
    """Copia a data/data-raw/ cada tabla de staging declarada en config.yaml.

    Las tablas que ya tienen copia local no se vuelven a consultar.

    Args:
        solo_estimar (bool): si es True, solo reporta cuánto escanearía cada
            tabla y no descarga nada.

    Returns:
        pl.DataFrame: una fila por tabla con su origen (cache, bigquery o
            estimado), los megabytes escaneados y las filas obtenidas.
    """
    seccion("Ingesta de staging")
    cliente = crear_cliente()
    RUTA_DATA_RAW.mkdir(parents=True, exist_ok=True)

    resumen = []
    for tabla, spec in TABLAS_STAGING.items():
        archivo = ruta_raw(tabla)

        if USAR_CACHE_LOCAL and archivo.exists() and not solo_estimar:
            filas = pl.scan_parquet(archivo).select(pl.len()).collect().item()
            print(f"{tabla:<32} copia local   {filas:>9,} filas")
            resumen.append({"tabla": tabla, "origen": "cache", "mb_escaneados": 0.0, "filas": filas})
            continue

        try:
            cliente.get_table(f"{PROYECTO_GCP}.{DATASETS['staging']}.{tabla}")
        except NotFound:
            print(f"{tabla:<32} no existe todavía en staging; se omite")
            resumen.append({"tabla": tabla, "origen": "no existe", "mb_escaneados": 0.0, "filas": None})
            continue

        sql = construir_sql(tabla, spec["columnas"], spec.get("filtro"))
        mb = megabytes_estimados(cliente, sql)

        if solo_estimar:
            print(f"{tabla:<32} escanearía {mb:>8.1f} MB")
            resumen.append({"tabla": tabla, "origen": "estimado", "mb_escaneados": mb, "filas": None})
            continue

        df = extraer(cliente, sql)
        assert df.height > 0, f"{tabla}: la consulta no regresó filas; revisar columnas y filtro"
        df.write_parquet(archivo)
        print(f"{tabla:<32} BigQuery      {df.height:>9,} filas  {df.width:>3} columnas  {mb:>6.1f} MB")
        registrar_etapa("ingesta", tabla, None, df.height, f"{df.width} columnas, {mb:.1f} MB escaneados")
        resumen.append({"tabla": tabla, "origen": "bigquery", "mb_escaneados": mb, "filas": df.height})

    resumen = pl.DataFrame(resumen, schema_overrides={"filas": pl.Int64})
    print(f"\nTotal escaneado: {resumen['mb_escaneados'].sum():,.1f} MB")
    return resumen


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Copia las tablas de staging de BigQuery a Parquet local.")
    parser.add_argument("--estimar", action="store_true", help="solo calcula cuántos MB escanearía, sin costo")
    args = parser.parse_args()
    ejecutar_ingesta(solo_estimar=args.estimar)
