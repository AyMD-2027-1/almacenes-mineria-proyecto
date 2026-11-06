"""
Parámetros del pipeline, leídos de config.yaml.
"""

import yaml

from config.rutas import RUTA_CONFIG

with open(RUTA_CONFIG, encoding="utf-8") as f:
    PARAMETROS = yaml.safe_load(f)

PROYECTO_GCP = PARAMETROS["proyecto_gcp"]
UBICACION = PARAMETROS["ubicacion"]
DATASETS = PARAMETROS["datasets"]
SEMILLA = PARAMETROS["semilla"]

# Si una consulta escanearía más que esto, BigQuery la rechaza sin cobrarla.
MAX_BYTES_FACTURADOS = PARAMETROS["bigquery"]["max_bytes_billed"]
# Si la copia local de una tabla ya existe, no se vuelve a consultar BigQuery.
USAR_CACHE_LOCAL = PARAMETROS["bigquery"]["usar_cache_local"]

TABLAS_STAGING = PARAMETROS["ingesta"]
ARCHIVOS_STAGING = PARAMETROS["archivos_staging"]
# Tablas de staging de las que sale el consumo y el género de cada artista.
TABLA_KWORB_SEMANAL = PARAMETROS["consumo"]["semanal"]
TABLA_KWORB_CATALOGO = PARAMETROS["consumo"]["catalogo"]
TABLAS_MUSICBRAINZ = PARAMETROS["consumo"]["musicbrainz"]
MUSICBRAINZ = PARAMETROS["musicbrainz"]
ENDIREH = PARAMETROS["endireh"]
ENDUTIH_TABULADOS = PARAMETROS["endutih_tabulados"]

# Ventana de análisis de E0: primer y último mes que se conservan.
VENTANA_INICIO = PARAMETROS["ventana"]["inicio"]
VENTANA_FIN = PARAMETROS["ventana"]["fin"]

EQUIVALENCIAS_ENTIDAD = PARAMETROS["equivalencias_entidad"]
NOMBRES_NACIONALES = PARAMETROS["nombres_nacionales"]
MAPEO_LGAMVLV = PARAMETROS["mapeo_lgamvlv"]
DELITO_IMPUNIDAD = PARAMETROS["delito_impunidad"]
MACROGENEROS = PARAMETROS["macrogeneros"]
MACROGENERO_CORREGIDO = PARAMETROS["macrogenero_corregido"]

# Umbrales de la limpieza.
Z_ROBUSTO_UMBRAL = PARAMETROS["limpieza"]["z_robusto_umbral"]
IQR_FACTOR = PARAMETROS["limpieza"]["iqr_factor"]
MESES_CERO_INICIAL_MIN = PARAMETROS["limpieza"]["meses_cero_inicial_min"]
EDAD_MAXIMA = PARAMETROS["limpieza"]["edad_maxima"]

# Criterios de la reducción de dimensionalidad.
ALFA = PARAMETROS["reduccion"]["alfa"]
CRAMER_V_MIN = PARAMETROS["reduccion"]["cramer_v_min"]
RHO_REDUNDANCIA = PARAMETROS["reduccion"]["rho_redundancia"]
PCA_VARIANZA_OBJETIVO = PARAMETROS["reduccion"]["pca_varianza_objetivo"]

# Reparto del consumo nacional entre entidades.
ARTISTAS_POR_ANIO = PARAMETROS["pesos"]["artistas_por_anio"]
ENTIDADES_MINIMAS = PARAMETROS["pesos"]["entidades_minimas"]

REGIONES_BANXICO = PARAMETROS["regiones_banxico"]
CATEGORIAS_MISONGYNY = PARAMETROS["categorias_misongyny"]
