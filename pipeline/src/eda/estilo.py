"""
Estilo común de las gráficas del EDA: colores, marcadores y tipos de línea
para que cada serie se distinga también en escala de grises, y una función
para guardar las figuras en la carpeta de imágenes de la página de E2.
"""

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from config.rutas import RUTA_GRAPHS  # noqa: E402

# Paleta categórica en orden fijo: cada serie conserva su color en todas las figuras.
COLORES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
MARCADORES = ["o", "s", "^", "D", "v", "P", "X", "*"]
LINEAS = ["-", "--", ":", "-."]

TINTA = "#0b0b0b"
TINTA_SECUNDARIA = "#52514e"
REJILLA = "#dedcd5"
FONDO = "#fcfcfb"
REFERENCIA = "#8a8984"


def aplicar_estilo() -> None:
    """Configura matplotlib con el estilo de las figuras del proyecto."""
    plt.rcParams.update(
        {
            "figure.facecolor": FONDO,
            "axes.facecolor": FONDO,
            "axes.edgecolor": REJILLA,
            "axes.labelcolor": TINTA_SECUNDARIA,
            "axes.titlecolor": TINTA,
            "axes.titlesize": 12,
            "axes.titleweight": "bold",
            "axes.titlelocation": "left",
            "axes.labelsize": 10,
            "axes.grid": True,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "grid.color": REJILLA,
            "grid.linewidth": 0.6,
            "xtick.color": TINTA_SECUNDARIA,
            "ytick.color": TINTA_SECUNDARIA,
            "legend.frameon": False,
            "lines.linewidth": 2,
            "lines.markersize": 6,
            "font.size": 10,
            "savefig.dpi": 150,
            "savefig.bbox": "tight",
        }
    )


def estilo_serie(i: int) -> dict[str, str]:
    """Devuelve color, marcador y tipo de línea de la serie i.

    Args:
        i (int): posición de la serie, empezando en 0.

    Returns:
        dict[str, str]: argumentos `color`, `marker` y `linestyle` para matplotlib.
    """
    return {
        "color": COLORES[i % len(COLORES)],
        "marker": MARCADORES[i % len(MARCADORES)],
        "linestyle": LINEAS[i % len(LINEAS)],
    }


def guardar(fig: plt.Figure, nombre: str) -> Path:
    """Guarda una figura en PNG en la carpeta de imágenes de E2 y la cierra.

    Args:
        fig (plt.Figure): figura a guardar.
        nombre (str): nombre del archivo sin extensión.

    Returns:
        Path: ruta del archivo guardado.
    """
    RUTA_GRAPHS.mkdir(parents=True, exist_ok=True)
    ruta = RUTA_GRAPHS / f"{nombre}.png"
    fig.savefig(ruta)
    plt.close(fig)
    return ruta
