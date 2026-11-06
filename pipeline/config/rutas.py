"""
Rutas del pipeline que lleva los datos de staging al almacén.
"""

from pathlib import Path

RUTA_PIPELINE = Path(__file__).resolve().parent.parent
RUTA_PROYECTO = RUTA_PIPELINE.parent

# Umbrales, semilla, datasets y columnas que se leen de cada tabla.
RUTA_CONFIG = RUTA_PIPELINE / "config.yaml"

RUTA_DATA = RUTA_PIPELINE / "data"
# Copia local de las tablas de staging. No se modifica nunca.
RUTA_DATA_RAW = RUTA_DATA / "data-raw"
# Tablas limpias y parámetros guardados.
RUTA_DATA_PROCESSED = RUTA_DATA / "data-processed"
# Páginas de Kworb descargadas, para no volver a pedirlas.
RUTA_KWORB_HTML = RUTA_DATA / "kworb_html"
# Dataset analítico al grano de E1, sin limpiar: es la entrada del EDA.
RUTA_ANALITICO_VIOLENCIA = RUTA_DATA_PROCESSED / "analitico_violencia.parquet"
RUTA_ANALITICO_CONSUMO = RUTA_DATA_PROCESSED / "analitico_consumo.parquet"
RUTA_ANALITICO_CONTEXTO = RUTA_DATA_PROCESSED / "analitico_contexto.parquet"
RUTA_CATALOGO_ENTIDADES = RUTA_DATA_PROCESSED / "catalogo_entidades.parquet"
# Dataset limpio: lo que se carga al almacén.
RUTA_LIMPIO_VIOLENCIA = RUTA_DATA_PROCESSED / "limpio_violencia.parquet"
RUTA_LIMPIO_CONSUMO = RUTA_DATA_PROCESSED / "limpio_consumo.parquet"
RUTA_LIMPIO_CONTEXTO = RUTA_DATA_PROCESSED / "limpio_contexto.parquet"
RUTA_LIMPIO_CANCIONES = RUTA_DATA_PROCESSED / "limpio_canciones.parquet"
RUTA_LIMPIO_ENTIDADES = RUTA_DATA_PROCESSED / "limpio_entidades.parquet"
# Peso de cada entidad para repartir el consumo nacional de cada artista y año.
RUTA_PESOS_ENTIDAD = RUTA_DATA_PROCESSED / "pesos_entidad.parquet"
# Tablas del esquema estrella de E1, listas para cargarse al almacén.
RUTA_ESTRELLA = RUTA_DATA_PROCESSED / "estrella"
# Tablas de resultados del EDA (descriptiva, concentración y heterogeneidad).
RUTA_TABLAS_EDA = RUTA_DATA_PROCESSED / "eda"
# Pruebas, correlaciones, PCA y tabla de decisión por variable.
RUTA_TABLAS_REDUCCION = RUTA_DATA_PROCESSED / "reduccion"
# Comparación del dataset antes y después de la preparación.
RUTA_TABLAS_VERIFICACION = RUTA_DATA_PROCESSED / "verificacion"
# Única carpeta de datos que se sube al repositorio.
RUTA_DATA_SAMPLE = RUTA_DATA / "sample"

# Filas de entrada y salida de cada etapa, y registro de limpieza.
RUTA_LOGS = RUTA_PIPELINE / "logs"
RUTA_REGISTRO_ETAPAS = RUTA_LOGS / "registro_etapas.csv"
RUTA_REGISTRO_LIMPIEZA = RUTA_LOGS / "registro_limpieza.csv"

# Fuentes que se descargan de un sitio (Kworb, Google Trends, MusicBrainz y los
# tabulados de INEGI), una carpeta por fuente. Se suben a staging, que es de
# donde las lee el pipeline; esta carpeta no se sube al repositorio.
RUTA_DESCARGAS = RUTA_DATA / "descargas"

# Las gráficas se guardan en la carpeta de imágenes de la página de E2.
RUTA_GRAPHS = RUTA_PROYECTO / "entregas" / "E2_eda_preparacion" / "img"

# DDL y consultas del DataMart.
RUTA_SQL = RUTA_PROYECTO / "sql"


def ruta_raw(tabla: str) -> Path:
    """Construye la ruta del archivo Parquet local de una tabla de staging.

    Args:
        tabla (str): nombre de la tabla en BigQuery, por ejemplo "stg_rnpdno".

    Returns:
        Path: ruta del archivo dentro de data/data-raw/.
    """
    return RUTA_DATA_RAW / f"{tabla}.parquet"
