"""
Descarga el historial semanal completo del chart de Spotify México desde
Kworb: la lista de todas las canciones que han estado en el chart semanal
(mx_weekly_totals) y la página de historial de cada una, de la que se toma la
columna de México.

El staging de E1 solo tenía las 300 canciones con más streams; esta descarga
trae todas, que es lo que E1 dejó planeado para la preparación. El resultado
son dos CSV con el mismo formato que las tablas de E1 (`track_id`, `semana`,
`valor` y el catálogo con artista y título), que se suben a staging como
tablas nuevas con src/ingestion/carga_staging.py.

Cada página se guarda en data/kworb_html/ y no se vuelve a pedir, así que la
descarga se puede interrumpir y reanudar. Entre páginas se espera
`PAUSA_SEGUNDOS`; el robots.txt de Kworb permite descargar todo el sitio.

Este script se ejecuta desde pipeline/ con:
    python -m src.ingestion.kworb
"""

import html
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import polars as pl

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from config.parametros import ARCHIVOS_STAGING  # noqa: E402
from config.rutas import RUTA_DESCARGAS, RUTA_KWORB_HTML  # noqa: E402
from src.registro import seccion  # noqa: E402

URL_LISTA = "https://kworb.net/spotify/country/mx_weekly_totals.html"
URL_CANCION = "https://kworb.net/spotify/track/{}.html"
AGENTE = "Mozilla/5.0 (compatible; academic-research-script)"
PAUSA_SEGUNDOS = 1.5
INTENTOS = 4
ESPERA_REINTENTO_SEGUNDOS = 30
PAIS = "MX"
SIN_CHART = "--"

PATRON_FILA_LISTA = re.compile(r'<td class="text mp"><div>(.*?)</div></td>', re.S)
PATRON_CANCION = re.compile(r'<a href="\.\./track/([A-Za-z0-9]{22})\.html">(.*?)</a>')
PATRON_ARTISTA = re.compile(r'<a href="\.\./artist/[A-Za-z0-9]+\.html">(.*?)</a>')


def descargar(url: str, destino: Path) -> str | None:
    """Devuelve el HTML de una página, desde la copia local si ya existe.

    Si la red falla, reintenta hasta `INTENTOS` veces con una espera creciente.
    Si Kworb ya no tiene la página (error 404), no reintenta.

    Args:
        url (str): dirección de la página.
        destino (Path): archivo donde se guarda la copia.

    Returns:
        str | None: el HTML de la página, o None si la página no existe.
    """
    if destino.exists():
        return destino.read_text(encoding="utf-8")
    peticion = urllib.request.Request(url, headers={"User-Agent": AGENTE})
    for intento in range(1, INTENTOS + 1):
        try:
            with urllib.request.urlopen(peticion, timeout=60) as respuesta:
                texto = respuesta.read().decode("utf-8")
            break
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return None
            if intento == INTENTOS:
                raise
            print(f"Falló {url} ({error}); reintento {intento} de {INTENTOS - 1}", flush=True)
            time.sleep(ESPERA_REINTENTO_SEGUNDOS * intento)
        except (TimeoutError, OSError) as error:
            # Un corte de red no debe detener una descarga de horas: se espera y se reintenta.
            if intento == INTENTOS:
                raise
            print(f"Falló {url} ({error}); reintento {intento} de {INTENTOS - 1}", flush=True)
            time.sleep(ESPERA_REINTENTO_SEGUNDOS * intento)
    destino.write_text(texto, encoding="utf-8")
    time.sleep(PAUSA_SEGUNDOS)
    return texto


def texto_plano(fragmento: str) -> str:
    """Quita las etiquetas HTML y las entidades de un fragmento.

    Args:
        fragmento (str): HTML de una celda.

    Returns:
        str: el texto visible, sin espacios sobrantes.
    """
    return html.unescape(re.sub(r"<[^>]+>", "", fragmento)).strip()


def leer_lista(pagina: str) -> pl.DataFrame:
    """Extrae de mx_weekly_totals el ID, el artista y el título de cada canción.

    Args:
        pagina (str): HTML de la lista.

    Returns:
        pl.DataFrame: columnas `track_id`, `artista` y `titulo`.
    """
    filas = []
    for celda in PATRON_FILA_LISTA.findall(pagina):
        cancion = PATRON_CANCION.search(celda)
        artistas = [texto_plano(a) for a in PATRON_ARTISTA.findall(celda)]
        filas.append({"track_id": cancion.group(1), "artista": " & ".join(artistas), "titulo": texto_plano(cancion.group(2))})
    return pl.DataFrame(filas)


def historial_mexico(track_id: str, pagina: str) -> list[dict[str, str]]:
    """Toma de la tabla semanal de una canción la columna de México.

    Args:
        track_id (str): ID de Spotify de la canción.
        pagina (str): HTML de la página de historial.

    Returns:
        list[dict[str, str]]: una fila por semana (y las filas Total y Peak)
            con `track_id`, `semana` y `valor` ("posición (streams)" o "--").
    """
    semanal = pagina[pagina.find('<div class="weekly">'):pagina.find('<div class="daily">')]
    encabezados = re.findall(r"<th>(.*?)</th>", semanal)
    if PAIS not in encabezados:
        return []
    columna = encabezados.index(PAIS)
    filas = []
    for fila in re.findall(r"<tr[^>]*>(.*?)</tr>", semanal, flags=re.S)[1:]:
        celdas = re.findall(r"<td[^>]*>(.*?)</td>", fila, flags=re.S)
        if len(celdas) <= columna:
            continue
        valor = texto_plano(celdas[columna]) or SIN_CHART
        filas.append({"track_id": track_id, "semana": texto_plano(celdas[0]), "valor": valor})
    return filas


def ejecutar_kworb() -> tuple[pl.DataFrame, pl.DataFrame]:
    """Descarga la lista y el historial de cada canción y escribe los dos CSV.

    Returns:
        tuple[pl.DataFrame, pl.DataFrame]: el catálogo de canciones y el historial semanal.
    """
    seccion("Kworb: chart semanal completo de México")
    RUTA_KWORB_HTML.mkdir(parents=True, exist_ok=True)
    catalogo = leer_lista(descargar(URL_LISTA, RUTA_KWORB_HTML / "mx_weekly_totals.html"))
    assert catalogo["track_id"].is_unique().all(), "la lista repite canciones"
    print(f"Canciones en la lista semanal: {catalogo.height:,}")

    filas, sin_mexico, sin_pagina = [], [], []
    for i, track_id in enumerate(catalogo["track_id"], start=1):
        pagina = descargar(URL_CANCION.format(track_id), RUTA_KWORB_HTML / f"{track_id}.html")
        if pagina is None:
            sin_pagina.append(track_id)
            continue
        historial = historial_mexico(track_id, pagina)
        filas.extend(historial)
        if not historial:
            sin_mexico.append(track_id)
        if i % 250 == 0:
            print(f"{i:,} de {catalogo.height:,} páginas", flush=True)
    semanal = pl.DataFrame(filas)
    print(f"Filas del historial: {semanal.height:,}; canciones sin columna de México: {len(sin_mexico)}; "
          f"canciones cuya página ya no existe en Kworb: {len(sin_pagina)}")

    for tabla, datos in (("stg_kworb_catalogo_completo", catalogo), ("stg_kworb_mx_semanal_completo", semanal)):
        destino = RUTA_DESCARGAS / ARCHIVOS_STAGING[tabla]
        destino.parent.mkdir(parents=True, exist_ok=True)
        datos.write_csv(destino)
        print(f"{destino.name}: {datos.height:,} filas")
    return catalogo, semanal


if __name__ == "__main__":
    ejecutar_kworb()
