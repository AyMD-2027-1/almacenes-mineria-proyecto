"""
Busca en MusicBrainz a los artistas del catálogo completo de Kworb que E1 no
buscó, con el mismo método que E1 (musicbrainzngs, búsqueda por nombre con
hasta 5 resultados, una solicitud por segundo). Se guarda el JSON de la
respuesta tal cual, incluidos los errores; la decisión de qué resultado es
el artista correcto la toma src/ingestion/generos.py.

El resultado es un CSV con el mismo formato que stg_musicbrainz_tags, que se
sube a staging como stg_musicbrainz_tags_completo con
src/ingestion/collection/carga_staging.py. Si se interrumpe, al volver a correr solo
busca a los artistas que faltan.

MusicBrainz pide que cada programa se identifique con un nombre, una versión
y un contacto; el contacto se configura en config.yaml.

Este script se ejecuta desde pipeline/ con:
    python -m src.ingestion.collection.musicbrainz
"""

import json
import sys
import time
from pathlib import Path

import musicbrainzngs as mb
import polars as pl

sys.path.append(str(Path(__file__).resolve().parents[3]))

from config.parametros import ARCHIVOS_STAGING, MUSICBRAINZ, TABLAS_MUSICBRAINZ  # noqa: E402
from config.rutas import RUTA_DESCARGAS, RUTA_KWORB_HTML, ruta_raw  # noqa: E402
from src.ingestion.collection.kworb import leer_lista  # noqa: E402
from src.registro import seccion  # noqa: E402

TABLA_SALIDA = "stg_musicbrainz_tags_completo"
RESULTADOS = 5
PAUSA_SEGUNDOS = 1.2
GUARDAR_CADA = 50


def artistas_pendientes(destino: Path) -> list[str]:
    """Lista los artistas del catálogo completo que todavía no tienen búsqueda.

    Args:
        destino (Path): CSV de salida; los artistas que ya están ahí se omiten.

    Returns:
        list[str]: nombres de artista en orden alfabético.
    """
    catalogo = leer_lista((RUTA_KWORB_HTML / "mx_weekly_totals.html").read_text(encoding="utf-8"))
    buscados = set()
    for tabla in TABLAS_MUSICBRAINZ:
        if ruta_raw(tabla).exists():
            buscados |= set(pl.read_parquet(ruta_raw(tabla), columns=["artista_buscado"])["artista_buscado"])
    if destino.exists():
        buscados |= set(pl.read_csv(destino, infer_schema=False)["artista_buscado"])
    artistas = catalogo["artista"].str.strip_chars().unique().sort().to_list()
    return [a for a in artistas if a and a not in buscados]


def guardar(filas: list[dict[str, str]], destino: Path) -> None:
    """Agrega las búsquedas nuevas al CSV de salida.

    Args:
        filas (list[dict[str, str]]): búsquedas con `artista_buscado` y `respuesta_json`.
        destino (Path): CSV de salida.
    """
    nuevas = pl.DataFrame(filas, schema={"artista_buscado": pl.String, "respuesta_json": pl.String})
    if destino.exists():
        nuevas = pl.concat([pl.read_csv(destino, infer_schema=False), nuevas])
    nuevas.write_csv(destino)


def ejecutar_musicbrainz() -> int:
    """Busca a los artistas pendientes y guarda las respuestas.

    Returns:
        int: número de artistas buscados en esta corrida.
    """
    seccion("MusicBrainz: artistas del catálogo completo")
    destino = RUTA_DESCARGAS / ARCHIVOS_STAGING[TABLA_SALIDA]
    destino.parent.mkdir(parents=True, exist_ok=True)
    pendientes = artistas_pendientes(destino)
    print(f"Artistas por buscar: {len(pendientes):,}")
    mb.set_useragent(MUSICBRAINZ["aplicacion"], MUSICBRAINZ["version"], MUSICBRAINZ.get("contacto"))

    lote = []
    for i, artista in enumerate(pendientes, start=1):
        try:
            respuesta = mb.search_artists(artist=artista, limit=RESULTADOS)
        except Exception as error:  # noqa: BLE001 - se guarda el error como en E1
            respuesta = {"error": str(error)}
        lote.append({"artista_buscado": artista, "respuesta_json": json.dumps(respuesta, ensure_ascii=False, default=str)})
        if i % GUARDAR_CADA == 0 or i == len(pendientes):
            guardar(lote, destino)
            lote = []
            print(f"{i:,} de {len(pendientes):,}", flush=True)
        time.sleep(PAUSA_SEGUNDOS)

    total = pl.read_csv(destino, infer_schema=False) if destino.exists() else pl.DataFrame()
    errores = total["respuesta_json"].str.starts_with('{"error"').sum() if total.height else 0
    print(f"{destino.name}: {total.height:,} artistas; búsquedas con error: {errores}")
    return len(pendientes)


if __name__ == "__main__":
    ejecutar_musicbrainz()
