"""
Género musical de cada artista a partir de las búsquedas guardadas de
MusicBrainz. Se toma el primer resultado de cada búsqueda y solo se acepta si
su nombre coincide con el artista buscado; de sus etiquetas se deriva un
macrogénero con las reglas de config.yaml.
"""

import json
import sys
from pathlib import Path

import polars as pl

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from config.parametros import MACROGENERO_CORREGIDO, MACROGENEROS, TABLAS_MUSICBRAINZ  # noqa: E402
from config.rutas import ruta_raw  # noqa: E402
from src.ingestion.entidades import normalizar_nombre  # noqa: E402

# Solo cuentan las etiquetas más votadas: las de pocos votos suelen ser ruido.
ETIQUETAS_USADAS = 5

# Palabras iniciales que una fuente incluye y otra no ("Grupo Marca Registrada" y
# "Marca Registrada" son el mismo artista).
ARTICULOS_INICIALES = r"^((GRUPO|LOS|LAS|LA|EL|THE) )+"


def comparable(nombre: pl.Expr) -> pl.Expr:
    """Simplifica un nombre de artista para comparar dos fuentes.

    Quita acentos, puntuación y artículos iniciales, y deja mayúsculas.

    Args:
        nombre (pl.Expr): nombre del artista.

    Returns:
        pl.Expr: nombre simplificado.
    """
    return (
        normalizar_nombre(nombre)
        .str.replace_all(r"[^A-Z0-9Ñ ]", "")
        .str.replace_all(r" +", " ")
        .str.strip_chars()
        .str.replace(ARTICULOS_INICIALES, "")
    )


def leer_primer_resultado(respuesta: str) -> dict:
    """Extrae del JSON de MusicBrainz los datos del primer artista encontrado.

    Args:
        respuesta (str): respuesta de la búsqueda tal como se guardó en staging.

    Returns:
        dict: nombre, país y las etiquetas más votadas, en orden; con
            valores vacíos si la búsqueda no encontró a nadie.
    """
    artistas = json.loads(respuesta).get("artist-list") or []
    if not artistas:
        return {"nombre_mb": None, "pais_artista": None, "etiquetas": []}
    primero = artistas[0]
    etiquetas = sorted(primero.get("tag-list", []), key=lambda t: -int(t["count"]))
    return {
        "nombre_mb": primero.get("name"),
        "pais_artista": primero.get("country"),
        "etiquetas": [t["name"] for t in etiquetas][:ETIQUETAS_USADAS],
    }


def expresion_macrogenero(etiquetas: pl.Expr) -> pl.Expr:
    """Asigna un macrogénero según la primera regla de config.yaml que coincide.

    Args:
        etiquetas (pl.Expr): columna de listas de etiquetas, de más a menos votadas.

    Returns:
        pl.Expr: el macrogénero; "otro" si ninguna regla coincide y
            "sin etiqueta" si la lista está vacía.
    """
    texto = etiquetas.list.join(" | ").str.to_lowercase()
    resultado = pl.when(etiquetas.list.len() == 0).then(pl.lit("sin etiqueta"))
    for macrogenero, palabras in MACROGENEROS.items():
        resultado = resultado.when(texto.str.contains_any(palabras)).then(pl.lit(macrogenero))
    return resultado.otherwise(pl.lit("otro"))


def generos_por_artista() -> pl.DataFrame:
    """Arma la tabla de género por artista.

    Cuando el nombre del resultado no coincide con el del artista buscado
    aun sin puntuación ni artículos iniciales (por ejemplo, la búsqueda de
    "Alemán" devuelve a Oscar Alemán), el resultado se descarta y el artista
    queda "sin etiqueta".

    Los artistas de `macrogenero_corregido` en config.yaml reciben el
    macrogénero declarado ahí, con `origen_genero` igual a "corregido".

    Returns:
        pl.DataFrame: una fila por artista con `artista` (str), `nombre_mb`
            (str), `coincide_nombre` (bool), `pais_artista` (str),
            `etiquetas_fuente` (str), `macrogenero` (str) y `origen_genero`
            (str: "musicbrainz", "corregido" o "sin dato").
    """
    # Si un artista se buscó en más de una tabla, cuenta la primera de la lista.
    busquedas = pl.concat(
        [pl.read_parquet(ruta_raw(t), columns=["artista_buscado", "respuesta_json"]) for t in TABLAS_MUSICBRAINZ]
    ).unique("artista_buscado", keep="first", maintain_order=True)
    filas = []
    for artista, respuesta in busquedas.iter_rows():
        filas.append({"artista": artista, **leer_primer_resultado(respuesta)})

    tabla = pl.DataFrame(filas).with_columns(
        (comparable(pl.col("artista")) == comparable(pl.col("nombre_mb")))
        .fill_null(False)
        .alias("coincide_nombre")
    )
    return tabla.with_columns(
        pl.when(pl.col("coincide_nombre")).then(pl.col("etiquetas")).otherwise(pl.lit([])).alias("etiquetas"),
        pl.when(pl.col("coincide_nombre")).then(pl.col("pais_artista")).alias("pais_artista"),
    ).select(
        "artista",
        "nombre_mb",
        "coincide_nombre",
        "pais_artista",
        pl.col("etiquetas").list.join(", ").alias("etiquetas_fuente"),
        expresion_macrogenero(pl.col("etiquetas")).alias("macrogenero"),
    ).with_columns(
        pl.when(pl.col("artista").is_in(list(MACROGENERO_CORREGIDO)))
        .then(pl.lit("corregido"))
        .when(pl.col("macrogenero") == "sin etiqueta")
        .then(pl.lit("sin dato"))
        .otherwise(pl.lit("musicbrainz"))
        .alias("origen_genero"),
        pl.col("artista").replace_strict(MACROGENERO_CORREGIDO, default=pl.col("macrogenero")).alias("macrogenero"),
    )
