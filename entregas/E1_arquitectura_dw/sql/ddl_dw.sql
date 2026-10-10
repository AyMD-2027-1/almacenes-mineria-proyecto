-- =====================================================================
-- E1 · Arquitectura del DW — DDL
-- Misoginia en la música consumida en México y violencia de género
-- Ejecutar en BigQuery con el proyecto del equipo seleccionado.
-- Los tres datasets deben quedar en la misma ubicación (US).
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. Datasets (capas)
-- ---------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS misoginia_staging
  OPTIONS (location = 'US',
           description = 'Bronze: datos crudos, tal como vienen de cada fuente del catálogo');

CREATE SCHEMA IF NOT EXISTS misoginia_dw
  OPTIONS (location = 'US',
           description = 'Gold: esquema estrella (hechos y dimensiones)');

CREATE SCHEMA IF NOT EXISTS misoginia_mart
  OPTIONS (location = 'US',
           description = 'Gold: vistas del DataMart, una por pregunta de E0');

-- ---------------------------------------------------------------------
-- 2. Dimensiones
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE misoginia_dw.dim_fecha (
  fecha_key   INT64  NOT NULL OPTIONS (description = 'Llave sustituta AAAAMM01, primer día del mes'),
  fecha       DATE   NOT NULL OPTIONS (description = 'Primer día del mes'),
  anio        INT64  NOT NULL,
  semestre    INT64,
  trimestre   INT64,
  mes         INT64  NOT NULL,
  nombre_mes  STRING,
  PRIMARY KEY (fecha_key) NOT ENFORCED
)
OPTIONS (description = 'Tiempo a nivel mes, calendario generado. SCD tipo 0. Responde: P1, P2, P3');

CREATE OR REPLACE TABLE misoginia_dw.dim_entidad (
  entidad_key                  INT64  NOT NULL OPTIONS (description = 'Llave sustituta: una por entidad y año'),
  cve_ent                      INT64  NOT NULL OPTIONS (description = 'Clave INEGI 1-32; 0 = Nacional'),
  entidad                      STRING NOT NULL,
  region                       STRING OPTIONS (description = 'Jerarquía entidad -> región'),
  iso_3166_2                   STRING OPTIONS (description = 'Código ISO del estado, liga con Google Trends y Natural Earth'),
  geometry_wkt                 STRING OPTIONS (description = 'Natural Earth (F18)'),
  anio                         INT64  NOT NULL OPTIONS (description = 'Año de vigencia de la versión'),
  valido_desde                 DATE,
  valido_hasta                 DATE,
  es_actual                    BOOL,
  poblacion_mujeres            INT64  OPTIONS (description = 'CONAPO (F3), mitad de año'),
  poblacion_mujeres_15_mas     INT64  OPTIONS (description = 'CONAPO (F3), mitad de año'),
  pct_viviendas_internet       FLOAT64 OPTIONS (description = 'Censo 2020 ITER (F14)'),
  pct_usuarios_internet        FLOAT64 OPTIONS (description = 'ENDUTIH (F16)'),
  pct_pobreza                  FLOAT64 OPTIONS (description = 'CONEVAL (F15)'),
  indice_impunidad             FLOAT64 OPTIONS (description = 'México Evalúa (F17)'),
  prevalencia_violencia_total  FLOAT64 OPTIONS (description = 'ENDIREH (F2), % de mujeres de 15 años y más'),
  prevalencia_psicologica      FLOAT64 OPTIONS (description = 'ENDIREH (F2)'),
  prevalencia_fisica           FLOAT64 OPTIONS (description = 'ENDIREH (F2)'),
  prevalencia_sexual           FLOAT64 OPTIONS (description = 'ENDIREH (F2)'),
  prevalencia_economica_patrimonial FLOAT64 OPTIONS (description = 'ENDIREH (F2)'),
  feminicidios_prensa          INT64  OPTIONS (description = 'Mapa de Salguero (F10)'),
  PRIMARY KEY (entidad_key) NOT ENFORCED
)
OPTIONS (description = 'Entidad federativa con contexto anual. SCD tipo 2: una versión por entidad y año. Responde: P1, P2, P4');

CREATE OR REPLACE TABLE misoginia_dw.dim_contenido_musical (
  contenido_key        INT64  NOT NULL OPTIONS (description = 'Llave sustituta: una por contenido y versión del modelo'),
  tipo_contenido       STRING NOT NULL OPTIONS (description = 'cancion, termino_genero o termino_artista'),
  spotify_id           STRING OPTIONS (description = 'ID de canción de Spotify (Kworb, Kaggle)'),
  titulo               STRING,
  artista_principal    STRING,
  artistas             STRING,
  genero               STRING OPTIONS (description = 'MusicBrainz (F8) o Last.fm (F19)'),
  pais_artista         STRING OPTIONS (description = 'MusicBrainz (F8)'),
  termino_busqueda     STRING OPTIONS (description = 'Término consultado en Google Trends (F11)'),
  tiene_letra          BOOL   OPTIONS (description = 'Letra encontrada en LRCLIB (F7)'),
  en_corpus_misongyny  BOOL   OPTIONS (description = 'La canción está etiquetada en MiSonGyny (F6)'),
  etiqueta_misongyny   STRING OPTIONS (description = 'Etiqueta de referencia de MiSonGyny'),
  es_misogina          BOOL   OPTIONS (description = 'Salida del clasificador'),
  categoria_misoginia  STRING OPTIONS (description = 'Sexualización, violencia, odio o no relacionada'),
  score_misoginia      FLOAT64 OPTIONS (description = 'Probabilidad del clasificador'),
  version_modelo       STRING,
  valido_desde         DATE,
  valido_hasta         DATE,
  es_actual            BOOL,
  PRIMARY KEY (contenido_key) NOT ENFORCED
)
OPTIONS (description = 'Canción o término de búsqueda. SCD tipo 2 por versión del clasificador. Responde: P1, P3, P4');

CREATE OR REPLACE TABLE misoginia_dw.dim_tipo_violencia (
  tipo_violencia_key    INT64  NOT NULL OPTIONS (description = 'Llave sustituta; -1 = no aplica'),
  tipo_lgamvlv          STRING NOT NULL OPTIONS (description = 'psicológica, física, patrimonial, económica, sexual, interpósita persona, feminicida, mediática, otra, no aplica'),
  origen_clasificacion  STRING OPTIONS (description = 'SESNSP, RNPDNO o clasificador de música'),
  categoria_origen      STRING OPTIONS (description = 'Categoría tal como la nombra la fuente'),
  bien_juridico         STRING,
  tipo_delito           STRING,
  subtipo_delito        STRING,
  modalidad             STRING,
  PRIMARY KEY (tipo_violencia_key) NOT ENFORCED
)
OPTIONS (description = 'Tipos de violencia mapeados a la LGAMVLV; conformada entre ambos hechos. SCD tipo 1. Responde: P2, P3, P4');

CREATE OR REPLACE TABLE misoginia_dw.dim_fuente (
  fuente_key             INT64  NOT NULL,
  codigo_catalogo        STRING NOT NULL OPTIONS (description = 'F1 a F19'),
  dataset                STRING NOT NULL,
  institucion            STRING,
  tipo_fuente            STRING,
  granularidad_original  STRING OPTIONS (description = 'dia, semana, mes, anio'),
  nivel_geografico       STRING OPTIONS (description = 'nacional, estado'),
  fecha_corte            DATE,
  url                    STRING,
  PRIMARY KEY (fuente_key) NOT ENFORCED
)
OPTIONS (description = 'Origen de cada fila de hechos. SCD tipo 1. Responde: P1, P2, P3, P4');

-- ---------------------------------------------------------------------
-- 3. Hechos
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE misoginia_dw.fact_violencia (
  fecha_key           INT64 NOT NULL,
  entidad_key         INT64 NOT NULL,
  tipo_violencia_key  INT64 NOT NULL,
  fuente_key          INT64 NOT NULL,
  conteo              INT64 OPTIONS (description = 'Carpetas, víctimas o personas desaparecidas. Aditiva'),
  conteo_mujeres      INT64 OPTIONS (description = 'Solo donde la fuente distingue sexo. Aditiva'),
  PRIMARY KEY (fecha_key, entidad_key, tipo_violencia_key, fuente_key) NOT ENFORCED,
  FOREIGN KEY (fecha_key) REFERENCES misoginia_dw.dim_fecha (fecha_key) NOT ENFORCED,
  FOREIGN KEY (entidad_key) REFERENCES misoginia_dw.dim_entidad (entidad_key) NOT ENFORCED,
  FOREIGN KEY (tipo_violencia_key) REFERENCES misoginia_dw.dim_tipo_violencia (tipo_violencia_key) NOT ENFORCED,
  FOREIGN KEY (fuente_key) REFERENCES misoginia_dw.dim_fuente (fuente_key) NOT ENFORCED
)
OPTIONS (description = 'Grano: una fila por entidad, mes, tipo de violencia y fuente. Fuentes: SESNSP (F1), RNPDNO (F13). Responde: P2, P4');

CREATE OR REPLACE TABLE misoginia_dw.fact_consumo_musical (
  fecha_key             INT64 NOT NULL,
  entidad_key           INT64 NOT NULL,
  contenido_key         INT64 NOT NULL,
  tipo_violencia_key    INT64 NOT NULL,
  fuente_key            INT64 NOT NULL,
  streams               INT64   OPTIONS (description = 'Reproducciones en el mes. Aditiva'),
  dias_en_chart         INT64   OPTIONS (description = 'Días del mes en el chart. Aditiva'),
  mejor_posicion        INT64   OPTIONS (description = 'Mejor posición del mes. No aditiva'),
  popularidad_promedio  FLOAT64 OPTIONS (description = 'Índice de popularidad de Spotify, Top 73 (F12). No aditiva'),
  interes_busqueda      FLOAT64 OPTIONS (description = 'Google Trends (F11), 0-100. No aditiva'),
  interes_ancla         FLOAT64 OPTIONS (description = 'Interés del término ancla en la misma consulta. No aditiva'),
  PRIMARY KEY (fecha_key, entidad_key, contenido_key, fuente_key) NOT ENFORCED,
  FOREIGN KEY (fecha_key) REFERENCES misoginia_dw.dim_fecha (fecha_key) NOT ENFORCED,
  FOREIGN KEY (entidad_key) REFERENCES misoginia_dw.dim_entidad (entidad_key) NOT ENFORCED,
  FOREIGN KEY (contenido_key) REFERENCES misoginia_dw.dim_contenido_musical (contenido_key) NOT ENFORCED,
  FOREIGN KEY (tipo_violencia_key) REFERENCES misoginia_dw.dim_tipo_violencia (tipo_violencia_key) NOT ENFORCED,
  FOREIGN KEY (fuente_key) REFERENCES misoginia_dw.dim_fuente (fuente_key) NOT ENFORCED
)
OPTIONS (description = 'Grano: una fila por contenido musical, entidad, mes y fuente. Fuentes: Kaggle (F5), Kworb (F4), Top 73 (F12), Google Trends (F11). Responde: P1, P2, P3, P4');
