"""
Carga al almacén las tablas del esquema estrella que arma src/loading/estrella.py
y ejecuta las vistas del DataMart.

Las tablas del DW ya existen desde E1 (sql/ddl_dw.sql), con descripciones de
columnas y llaves primarias y foráneas. Para no perder nada de eso no se
recrean: se revisa que el Parquet local tenga exactamente sus columnas y
tipos, y se cargan con un load job WRITE_APPEND, que no se cobra.

Una tabla que ya tiene filas no se toca, salvo que se pida con --reemplazar;
en ese caso se vacía con TRUNCATE TABLE antes de cargar. Así una segunda
corrida nunca duplica filas.

Después de cargar se comparan las filas (metadatos, sin costo) y las sumas de
las medidas principales contra lo local. Las vistas del DataMart se recrean
con sql/consultas_mart.sql (crear una vista no se cobra), porque las que
existían en BigQuery eran de una versión anterior al archivo final de E1, y se
ejecutan con un tope de bytes facturados. Los resultados quedan en
data/data-processed/mart/.

Este script se ejecuta desde pipeline/ con:
    python -m src.loading.carga_dw                    carga lo que esté vacío, valida y corre el DataMart
    python -m src.loading.carga_dw --estimar          solo revisa esquemas y estima bytes, sin escribir
    python -m src.loading.carga_dw --reemplazar --tablas dim_cancion fact_consumo_musical
"""

import argparse
import sys
from pathlib import Path

import polars as pl
from google.cloud import bigquery

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from config.parametros import DATASETS, MAX_BYTES_FACTURADOS, PROYECTO_GCP  # noqa: E402
from config.rutas import RUTA_DATA_PROCESSED, RUTA_ESTRELLA, RUTA_SQL  # noqa: E402
from src.ingestion.ingesta import crear_cliente  # noqa: E402
from src.registro import registrar_etapa, seccion  # noqa: E402

pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_width_chars(200)

# Orden de carga: primero dimensiones y después hechos.
TABLAS = ["dim_fecha", "dim_entidad", "dim_cancion", "dim_tipo_violencia", "dim_contexto_entidad",
          "fact_violencia", "fact_consumo_musical"]

VISTAS = ["p1_proporcion_misogina_genero", "p1_proporcion_misogina_entidad", "p2_serie_mensual_nacional",
          "p2_correlacion_nacional", "p2_panel_entidad_anio", "p3_tendencia_tipo_violencia",
          "p4_tipos_musica_vs_oficial"]

TIPOS_BQ = {pl.Int64: "INTEGER", pl.Float64: "FLOAT", pl.String: "STRING", pl.Boolean: "BOOLEAN", pl.Date: "DATE"}

# Llaves de cada hecho: el grano y la dimensión a la que apunta cada una.
LLAVES_HECHO = {
    "fact_violencia": {"fecha_key": "dim_fecha", "entidad_key": "dim_entidad",
                       "tipo_violencia_key": "dim_tipo_violencia", "contexto_key": "dim_contexto_entidad"},
    "fact_consumo_musical": {"fecha_key": "dim_fecha", "entidad_key": "dim_entidad", "cancion_key": "dim_cancion",
                             "tipo_violencia_key": "dim_tipo_violencia", "contexto_key": "dim_contexto_entidad"},
}
GRANO = {"fact_violencia": ["fecha_key", "entidad_key", "tipo_violencia_key"],
         "fact_consumo_musical": ["fecha_key", "entidad_key", "cancion_key"]}

# Medidas cuya suma se compara entre BigQuery y lo local.
SUMAS = {"fact_violencia": ["carpetas", "victimas_mujeres"], "fact_consumo_musical": ["streams_estimados"]}

RUTA_MART = RUTA_DATA_PROCESSED / "mart"
TOLERANCIA = 1e-6


def nombre_completo(tabla: str, dataset: str = "dw") -> str:
    """Arma el nombre completo de una tabla o vista.

    Args:
        tabla (str): nombre de la tabla.
        dataset (str): clave del dataset en config.yaml ("dw" o "mart").

    Returns:
        str: proyecto.dataset.tabla.
    """
    return f"{PROYECTO_GCP}.{DATASETS[dataset]}.{tabla}"


def revisar_esquema(cliente: bigquery.Client, tabla: str) -> tuple[bigquery.Table, pl.DataFrame]:
    """Comprueba que el Parquet local tenga las mismas columnas y tipos que la tabla del DW.

    Args:
        cliente (bigquery.Client): cliente de BigQuery.
        tabla (str): nombre de la tabla.

    Returns:
        tuple[bigquery.Table, pl.DataFrame]: la tabla de BigQuery y los datos locales.
    """
    destino = cliente.get_table(nombre_completo(tabla))
    local = pl.read_parquet(RUTA_ESTRELLA / f"{tabla}.parquet")
    esperado = [(c.name, c.field_type) for c in destino.schema]
    actual = [(n, TIPOS_BQ.get(t, str(t))) for n, t in local.schema.items()]
    assert esperado == actual, f"{tabla}: el esquema local no coincide con el DW\n{esperado}\n{actual}"
    requeridas = [c.name for c in destino.schema if c.mode == "REQUIRED" and local[c.name].null_count() > 0]
    assert not requeridas, f"{tabla}: nulos en columnas obligatorias {requeridas}"
    return destino, local


def cargar_tabla(cliente: bigquery.Client, tabla: str, reemplazar: bool) -> str:
    """Carga una tabla del DW desde su Parquet local.

    Args:
        cliente (bigquery.Client): cliente de BigQuery.
        tabla (str): nombre de la tabla.
        reemplazar (bool): si la tabla ya tiene filas, vaciarla antes de cargar.

    Returns:
        str: qué se hizo con la tabla.
    """
    destino, local = revisar_esquema(cliente, tabla)
    if destino.num_rows:
        if not reemplazar:
            return f"ya tenía {destino.num_rows:,} filas; no se tocó (usar --reemplazar)"
        cliente.query(f"TRUNCATE TABLE `{nombre_completo(tabla)}`").result()
    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=destino.schema,
    )
    with open(RUTA_ESTRELLA / f"{tabla}.parquet", "rb") as archivo:
        cliente.load_table_from_file(archivo, nombre_completo(tabla), job_config=config).result()
    cargadas = cliente.get_table(nombre_completo(tabla)).num_rows
    assert cargadas == local.height, f"{tabla}: se cargaron {cargadas:,} filas y hay {local.height:,} en local"
    registrar_etapa("carga", tabla, local.height, cargadas, "load job WRITE_APPEND al DW")
    return f"cargada: {cargadas:,} filas"


def consultar(cliente: bigquery.Client, sql: str) -> pl.DataFrame:
    """Ejecuta una consulta con tope de bytes facturados.

    Args:
        cliente (bigquery.Client): cliente de BigQuery.
        sql (str): consulta.

    Returns:
        pl.DataFrame: el resultado.
    """
    config = bigquery.QueryJobConfig(maximum_bytes_billed=MAX_BYTES_FACTURADOS)
    return pl.from_arrow(cliente.query(sql, job_config=config).to_arrow())


def megabytes(cliente: bigquery.Client, sql: str) -> float:
    """Estima con un dry run cuántos MB procesaría una consulta, sin costo.

    Args:
        cliente (bigquery.Client): cliente de BigQuery.
        sql (str): consulta.

    Returns:
        float: megabytes procesados.
    """
    config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    return cliente.query(sql, job_config=config).total_bytes_processed / 1e6


def validar_sumas(cliente: bigquery.Client) -> None:
    """Compara las sumas de las medidas principales entre el DW y los Parquet locales.

    Args:
        cliente (bigquery.Client): cliente de BigQuery.
    """
    for tabla, columnas in SUMAS.items():
        local = pl.read_parquet(RUTA_ESTRELLA / f"{tabla}.parquet", columns=columnas)
        sql = f"SELECT {', '.join(f'SUM({c}) AS {c}' for c in columnas)} FROM `{nombre_completo(tabla)}`"
        remoto = consultar(cliente, sql)
        for columna in columnas:
            a, b = float(local[columna].sum()), float(remoto[columna][0] or 0)
            assert abs(a - b) <= TOLERANCIA * max(abs(a), 1), f"{tabla}.{columna}: local {a:,.0f} y DW {b:,.0f}"
            print(f"{tabla}.{columna}: {b:,.0f} (igual que en local)")


def revisar_consistencia(cliente: bigquery.Client) -> pl.DataFrame:
    """Comprueba en el DW que los hechos no tengan llaves nulas ni huérfanas y que su grano sea único.

    Una llave huérfana es un valor que no existe en su dimensión. El grano es
    único si el número de filas es igual al de combinaciones distintas de las
    llaves del grano.

    Args:
        cliente (bigquery.Client): cliente de BigQuery.

    Returns:
        pl.DataFrame: una fila por hecho y revisión, con su resultado.
    """
    filas = []
    for hecho, llaves in LLAVES_HECHO.items():
        tabla = f"`{nombre_completo(hecho)}`"
        grano = ", ".join(f"CAST({c} AS STRING)" for c in GRANO[hecho])
        nulos = " + ".join(f"COUNTIF({c} IS NULL)" for c in llaves)
        base = consultar(cliente, f"SELECT COUNT(*) AS filas, COUNT(DISTINCT CONCAT({grano.replace(', ', ", '-', ")})) AS grano, "
                                  f"{nulos} AS nulos FROM {tabla}").row(0, named=True)
        filas += [{"hecho": hecho, "revision": "filas", "valor": base["filas"]},
                  {"hecho": hecho, "revision": "combinaciones distintas del grano", "valor": base["grano"]},
                  {"hecho": hecho, "revision": "llaves nulas", "valor": base["nulos"]}]
        for llave, dimension in llaves.items():
            huerfanas = consultar(
                cliente,
                f"SELECT COUNT(*) AS n FROM {tabla} AS f LEFT JOIN `{nombre_completo(dimension)}` AS d "
                f"ON d.{llave} = f.{llave} WHERE d.{llave} IS NULL",
            )["n"][0]
            filas.append({"hecho": hecho, "revision": f"huérfanas en {llave} ({dimension})", "valor": huerfanas})
    resultado = pl.DataFrame(filas)
    with pl.Config(tbl_rows=-1, fmt_str_lengths=60):
        print(resultado)
    for hecho in LLAVES_HECHO:
        r = dict(resultado.filter(pl.col("hecho") == hecho).select("revision", "valor").iter_rows())
        assert r["filas"] == r["combinaciones distintas del grano"], f"{hecho}: el grano se repite"
        assert r["llaves nulas"] == 0, f"{hecho}: hay llaves nulas"
        assert all(v == 0 for k, v in r.items() if k.startswith("huérfanas")), f"{hecho}: hay llaves huérfanas"
    RUTA_MART.mkdir(parents=True, exist_ok=True)
    resultado.write_parquet(RUTA_MART / "consistencia_dw.parquet")
    return resultado


def recrear_vistas(cliente: bigquery.Client) -> None:
    """Crea o reemplaza las vistas del DataMart con sql/consultas_mart.sql.

    El archivo se ejecuta completo como un solo script, en el orden en que
    están las vistas (algunas leen de otras).

    Args:
        cliente (bigquery.Client): cliente de BigQuery.
    """
    script = (RUTA_SQL / "consultas_mart.sql").read_text(encoding="utf-8")
    cliente.query(script).result()
    existentes = {t.table_id for t in cliente.list_tables(f"{PROYECTO_GCP}.{DATASETS['mart']}")}
    faltantes = set(VISTAS) - existentes
    assert not faltantes, f"no se crearon las vistas {sorted(faltantes)}"
    print(f"{len(VISTAS)} vistas creadas o reemplazadas desde consultas_mart.sql")


def ejecutar_mart(cliente: bigquery.Client) -> dict[str, pl.DataFrame]:
    """Ejecuta cada vista del DataMart y guarda su resultado.

    Args:
        cliente (bigquery.Client): cliente de BigQuery.

    Returns:
        dict[str, pl.DataFrame]: resultado de cada vista.
    """
    RUTA_MART.mkdir(parents=True, exist_ok=True)
    resultados = {}
    for vista in VISTAS:
        sql = f"SELECT * FROM `{nombre_completo(vista, 'mart')}`"
        datos = consultar(cliente, sql)
        datos.write_parquet(RUTA_MART / f"{vista}.parquet")
        resultados[vista] = datos
        print(f"\n{vista}: {datos.height:,} filas")
        print(datos.head(5))
    return resultados


def ejecutar_carga_dw(estimar: bool = False, reemplazar: bool = False, tablas: list[str] | None = None) -> None:
    """Carga las tablas, valida y ejecuta el DataMart.

    Args:
        estimar (bool): solo revisar esquemas y estimar bytes, sin escribir nada.
        reemplazar (bool): vaciar y volver a cargar las tablas que ya tienen filas.
        tablas (list[str] | None): tablas a cargar; None carga todas.
    """
    cliente = crear_cliente()
    elegidas = [t for t in TABLAS if not tablas or t in tablas]

    seccion("Esquemas locales contra el DW")
    for tabla in elegidas:
        destino, local = revisar_esquema(cliente, tabla)
        print(f"{tabla:<22} {local.height:>8,} filas locales  {destino.num_rows:>8,} en el DW  esquema igual")
    if estimar:
        seccion("Bytes que procesarían las vistas del DataMart (dry run)")
        for vista in VISTAS:
            sql = f"SELECT * FROM `{nombre_completo(vista, 'mart')}`"
            print(f"{vista:<34} {megabytes(cliente, sql):>8.1f} MB")
        return

    seccion("Carga")
    for tabla in elegidas:
        print(f"{tabla:<22} {cargar_tabla(cliente, tabla, reemplazar)}")
    seccion("Validación")
    validar_sumas(cliente)
    seccion("Consistencia del DW")
    revisar_consistencia(cliente)
    seccion("DataMart")
    recrear_vistas(cliente)
    ejecutar_mart(cliente)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Carga el esquema estrella al DW y ejecuta el DataMart.")
    parser.add_argument("--estimar", action="store_true", help="solo revisa y estima, sin escribir")
    parser.add_argument("--reemplazar", action="store_true", help="vacía y vuelve a cargar las tablas con filas")
    parser.add_argument("--tablas", nargs="+", choices=TABLAS, help="solo estas tablas")
    args = parser.parse_args()
    ejecutar_carga_dw(estimar=args.estimar, reemplazar=args.reemplazar, tablas=args.tablas)
