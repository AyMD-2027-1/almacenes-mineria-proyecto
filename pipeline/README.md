# Pipeline de E2: de staging al DW

Lleva los datos de la capa de staging en BigQuery (`misoginia_staging`) hasta el esquema estrella poblado
(`misoginia_dw`) y ejecuta las vistas del DataMart (`misoginia_mart`). La descripción completa, con las decisiones y
los resultados, está en el sitio: E2 · EDA y preparación.

## Requisitos

- Python 3.12
- Google Cloud SDK (`gcloud`) con acceso al proyecto `proyecto-511216`
- Quarto, solo para compilar el sitio

## Instalación

Desde la carpeta `pipeline/`:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Autenticación

El pipeline usa las credenciales de aplicación por defecto (ADC) de `gcloud`; no hay llaves en el repositorio y
ningún archivo `.json` se sube (`.gitignore`).

```bash
gcloud auth application-default login
gcloud auth application-default set-quota-project proyecto-511216
```

## Ejecución

```bash
python run_pipeline.py              # todo: de staging al DataMart
python run_pipeline.py --sin-carga  # todo menos escribir en BigQuery
python run_pipeline.py --desde limpieza
python run_pipeline.py --solo eda
```

| Etapa | Qué hace | Escribe en BigQuery |
|---|---|---|
| `ingesta` | Copia las tablas de staging a `data/data-raw/` (solo las que faltan) | No (solo lee) |
| `calidad` | Diagnóstico de cada tabla de staging | No |
| `ensamblado` | Dataset analítico al grano de E1 | No |
| `eda` | Estadística descriptiva, Gini, entropía, cobertura y figuras | No |
| `limpieza` | Tratamientos y registro de limpieza | No |
| `reduccion` | χ², correlación, PCA y tabla de decisión | No |
| `muestreo` | Comprueba que no hace falta muestrear y escribe `data/sample/` | No |
| `verificacion` | Antes contra después | No |
| `pesos` | Reparto del consumo nacional entre entidades | No |
| `estrella` | Dimensiones y hechos con el esquema del DDL | No |
| `carga` | Carga al DW, valida, recrea y ejecuta el DataMart | **Sí** |

Cada etapa también se puede correr sola, por ejemplo `python -m src.cleaning.limpieza`.

**Tiempo aproximado.** Ingesta desde cero: 1 a 2 minutos (unos 130 MB escaneados en BigQuery). De `calidad` a
`estrella`: unos 10 segundos. Carga y DataMart: alrededor de 1 minuto.

**Costo.** La ingesta lee solo las columnas declaradas y cada consulta tiene un tope de 1 GB facturado. Las cargas
al DW son *load jobs*, que BigQuery no cobra. La primera ingesta queda guardada en `data/data-raw/` y no se vuelve a
consultar.

**Idempotencia.** La etapa `carga` vacía las tablas del DW y las vuelve a llenar, y las vistas se crean con
`CREATE OR REPLACE`: correr el pipeline dos veces deja el mismo resultado.

## Salidas

| Carpeta | Contenido | Se sube al repositorio |
|---|---|---|
| `data/data-raw/` | Copia local de staging | No |
| `data/data-processed/` | Dataset analítico, limpio, tablas del EDA, reducción, verificación, estrella y resultados del DataMart | No |
| `data/descargas/` | Fuentes descargadas de un sitio (Kworb, Trends, MusicBrainz, INEGI) | No |
| `data/sample/` | Muestra pequeña de las tablas limpias, en CSV | Sí |
| `logs/registro_etapas.csv` | Filas de entrada y salida de cada etapa | No |
| `logs/registro_limpieza.csv` | Registro de limpieza | No |
| `../entregas/E2_eda_preparacion/img/` | Figuras | Sí |

## Configuración

Todos los parámetros están en `config.yaml`: proyecto y datasets, tope de bytes, semilla (42), ventana
(2015-01 a 2025-12), columnas de cada tabla de staging, equivalencias de entidad, mapeo LGAMVLV, reglas de
macrogénero, umbrales de limpieza y reducción, y la regla de los pesos. Las rutas están en `config/rutas.py` y son
relativas a la carpeta del proyecto.

## Extracción de fuentes (una sola vez)

Algunas fuentes no están en BigQuery de origen: se descargan de un sitio y se suben a staging como tablas nuevas.
Esto ya está hecho y **no es parte del pipeline**; solo hace falta repetirlo para actualizar una fuente. Los
archivos descargados quedan en `data/descargas/`, una carpeta por fuente (no se suben al repositorio).

| Fuente | Script | Tabla de staging | Tiempo |
|---|---|---|---|
| Kworb, Top 200 semanal completo | `python -m src.ingestion.kworb` | `stg_kworb_catalogo_completo`, `stg_kworb_mx_semanal_completo` | unas 5 horas (1.5 s entre páginas; se puede interrumpir y retomar) |
| MusicBrainz, artistas nuevos | `python -m src.ingestion.musicbrainz` | `stg_musicbrainz_tags_completo` | unos 25 minutos |
| ENDIREH 2021 (tabulado de INEGI, cuadro 21.2) | `python -m src.ingestion.endireh` | `stg_endireh_2021_prevalencias` | segundos |
| ENDUTIH 2015–2025 (tabulados de INEGI) | `python -m src.ingestion.endutih` | `stg_endutih_usuarios_entidad_anio` | segundos |
| Google Trends por género y por artista | descarga desde el navegador (`src/ingestion/trends_descarga.js`) | `stg_trends_macrogenero_anio`, `stg_trends_artista_anio` | unos 30 minutos |

Después de generar los CSV se suben con:

```bash
python -m src.ingestion.carga_staging --tablas stg_endireh_2021_prevalencias
```

La carga usa `WRITE_EMPTY`: si la tabla ya existe en staging no se toca.

## Estructura

```
pipeline/
├── run_pipeline.py      punto de entrada
├── config.yaml          parámetros
├── config/              rutas y lectura de parámetros
├── requirements.txt     versiones fijadas
├── src/
│   ├── registro.py      registro de etapas
│   ├── ingestion/       staging, ensamblado y extracción de fuentes
│   ├── eda/             inspección, medidas, descriptiva, concentración, cobertura
│   ├── cleaning/        calidad y limpieza
│   ├── reduction/       χ², correlación, PCA
│   ├── sampling/        muestreo
│   ├── verification/    antes contra después
│   └── loading/         pesos, esquema estrella y carga al DW
├── data/
│   ├── descargas/       fuentes descargadas de un sitio, una carpeta por fuente (no se sube)
│   └── sample/          muestra que se sube al repositorio
└── logs/                registros de cada corrida
```
