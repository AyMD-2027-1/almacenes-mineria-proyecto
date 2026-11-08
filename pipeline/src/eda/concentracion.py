"""
Concentración y heterogeneidad del dataset analítico: coeficiente de Gini con
su curva de Lorenz y entropía normalizada, calculados por año para poder
compararlos en el tiempo.

Gini sobre:
    los streams entre canciones y entre artistas de la muestra;
    las carpetas de feminicidio y de violencia familiar entre entidades,
    en conteo y en tasa por 100 mil mujeres.
Entropía sobre:
    el macrogénero de los streams;
    el tipo de violencia de las carpetas, nacional y por entidad.

Este script se ejecuta desde pipeline/ con:
    python -m src.eda.concentracion
"""

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import polars as pl

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from config.rutas import RUTA_TABLAS_EDA  # noqa: E402
from src.eda import vistas  # noqa: E402
from src.eda.estilo import REFERENCIA, aplicar_estilo, estilo_serie, guardar  # noqa: E402
from src.eda.medidas import curva_lorenz, indices_categoricos, indices_concentracion  # noqa: E402
from src.registro import seccion  # noqa: E402

pl.Config.set_tbl_rows(60)
pl.Config.set_tbl_cols(20)
pl.Config.set_tbl_width_chars(220)
pl.Config.set_tbl_hide_dataframe_shape(True)
pl.Config.set_float_precision(3)

# Número de unidades más grandes para la razón de concentración CR_k.
K_TOP = 10

# Los artistas sin etiqueta de género no son una categoría musical: se excluyen
# de la entropía del macrogénero y su peso se reporta como cobertura.
SIN_ETIQUETA = "sin etiqueta"

# Año más reciente completo, usado en las curvas de Lorenz.
ANIO_LORENZ = 2025
ANIO_LORENZ_COMPARACION = 2019


def masas_por_anio() -> dict[str, tuple[str, pl.DataFrame, str]]:
    """Arma cada masa cuya concentración se mide, con su unidad y su columna de valor.

    En las entidades se conservan los ceros: un estado sin carpetas en un año
    sigue siendo una de las 32 unidades. En canciones y artistas solo cuentan
    los que tuvieron streams ese año, porque la muestra no tiene canciones
    fuera del chart.

    Returns:
        dict[str, tuple[str, pl.DataFrame, str]]: para cada variable, la unidad
            de análisis, una tabla con `anio` y el valor, y el nombre de la
            columna del valor.
    """
    anual = vistas.consumo_cancion_anio()
    artistas = anual.group_by("artista", "anio").agg(pl.col("streams").sum())
    tasas = vistas.tasas_entidad_anio()
    feminicida = tasas.filter(pl.col("tipo_lgamvlv") == "feminicida")
    familiar = tasas.filter(pl.col("tipo_lgamvlv") == "sin distinguir")
    return {
        "streams_canciones": ("canción", anual, "streams"),
        "streams_artistas": ("artista", artistas, "streams"),
        "feminicidio_conteo": ("entidad", feminicida, "carpetas"),
        "feminicidio_tasa": ("entidad", feminicida, "tasa_carpetas"),
        "familiar_conteo": ("entidad", familiar, "carpetas"),
        "familiar_tasa": ("entidad", familiar, "tasa_carpetas"),
    }


def gini_por_anio() -> pl.DataFrame:
    """Calcula los índices de concentración de cada masa en cada año.

    Returns:
        pl.DataFrame: una fila por variable y año con la unidad, el número de
            unidades, Gini, Gini normalizado, participación del 10 % más
            grande, CR_10, HHI y unidades efectivas.
    """
    filas = []
    for variable, (unidad, datos, valor) in masas_por_anio().items():
        for anio in sorted(datos["anio"].unique().to_list()):
            x = datos.filter(pl.col("anio") == anio)[valor].fill_null(0).to_numpy()
            if x.size < 2 or x.sum() <= 0:
                continue
            filas.append({"variable": variable, "unidad": unidad, "anio": anio, **indices_concentracion(x, K_TOP)})
    return pl.DataFrame(filas)


def entropia_por_anio() -> pl.DataFrame:
    """Calcula la entropía normalizada del macrogénero y del tipo de violencia por año.

    El macrogénero se mide sobre los streams sin contar a los artistas sin
    etiqueta; la columna `cobertura` dice qué parte de los streams sí tiene
    etiqueta. El tipo de violencia se mide sobre las carpetas nacionales.

    Returns:
        pl.DataFrame: una fila por variable y año con la cobertura y los
            índices de heterogeneidad.
    """
    consumo = vistas.consumo()
    violencia = vistas.violencia().filter(
        (pl.col("origen") == "SESNSP") & pl.col("tipo_lgamvlv").is_in(vistas.TIPOS_ANALIZADOS)
    )
    filas = []
    for anio in sorted(consumo["anio"].unique().to_list()):
        del_anio = consumo.filter(pl.col("anio") == anio)
        etiquetado = del_anio.filter(pl.col("macrogenero") != SIN_ETIQUETA)
        conteos = etiquetado.group_by("macrogenero").agg(pl.col("streams").sum())["streams"].to_numpy()
        if conteos.sum() > 0:
            filas.append({"variable": "macrogenero_streams", "anio": anio,
                          "cobertura": etiquetado["streams"].sum() / del_anio["streams"].sum(),
                          **indices_categoricos(conteos)})
    for anio in sorted(violencia["anio"].unique().to_list()):
        conteos = (violencia.filter(pl.col("anio") == anio)
                   .group_by("tipo_lgamvlv").agg(pl.col("carpetas").sum())["carpetas"].to_numpy())
        filas.append({"variable": "tipo_violencia_carpetas", "anio": anio, "cobertura": 1.0,
                      **indices_categoricos(conteos)})
    return pl.DataFrame(filas)


def entropia_por_entidad(anio: int) -> pl.DataFrame:
    """Calcula la entropía normalizada del tipo de violencia en cada entidad.

    Args:
        anio (int): año que se analiza.

    Returns:
        pl.DataFrame: una fila por entidad con su tipo predominante, la
            proporción de ese tipo y la entropía normalizada.
    """
    violencia = vistas.violencia().filter(
        (pl.col("origen") == "SESNSP")
        & pl.col("tipo_lgamvlv").is_in(vistas.TIPOS_ANALIZADOS)
        & (pl.col("anio") == anio)
    )
    por_tipo = violencia.group_by("cve_ent", "tipo_lgamvlv").agg(pl.col("carpetas").sum())
    filas = []
    for cve_ent in sorted(por_tipo["cve_ent"].unique().to_list()):
        datos = por_tipo.filter(pl.col("cve_ent") == cve_ent).sort("carpetas", descending=True)
        indices = indices_categoricos(datos["carpetas"].to_numpy())
        filas.append({"cve_ent": cve_ent, "tipo_predominante": datos[0, "tipo_lgamvlv"],
                      "proporcion_moda": indices["proporcion_moda"],
                      "entropia_normalizada": indices["entropia_normalizada"]})
    return pl.DataFrame(filas).sort("entropia_normalizada")


def figura_lorenz() -> None:
    """Curvas de Lorenz de los streams entre canciones y del feminicidio entre entidades.

    El panel de canciones compara dos años; el de entidades compara el conteo
    con la tasa del mismo año, para mostrar que el tamaño de la población
    cambia la lectura.
    """
    masas = masas_por_anio()
    paneles = [
        ("Streams entre canciones de la muestra", "streams_canciones",
         [(ANIO_LORENZ_COMPARACION, "streams_canciones"), (ANIO_LORENZ, "streams_canciones")], "canciones"),
        (f"Feminicidio entre entidades, {ANIO_LORENZ}", "feminicidio_conteo",
         [(ANIO_LORENZ, "feminicidio_conteo"), (ANIO_LORENZ, "feminicidio_tasa")], "entidades"),
    ]
    fig, ejes = plt.subplots(1, 2, figsize=(11, 5))
    for ax, (titulo, _, curvas, unidades) in zip(ejes, paneles):
        ax.plot([0, 1], [0, 1], color=REFERENCIA, linestyle="--", linewidth=1.2, label="Igualdad")
        for i, (anio, variable) in enumerate(curvas):
            _, datos, valor = masas[variable]
            x = datos.filter(pl.col("anio") == anio)[valor].fill_null(0).to_numpy()
            u, masa = curva_lorenz(x)
            gini = indices_concentracion(x, K_TOP)["gini"]
            etiqueta = (f"{anio} (n = {x.size}, Gini = {gini:.2f})" if variable.startswith("streams")
                        else f"{'Conteo' if variable.endswith('conteo') else 'Tasa por 100 mil mujeres'} (Gini = {gini:.2f})")
            estilo = estilo_serie(i)
            ax.plot(u, masa, color=estilo["color"], linestyle=estilo["linestyle"], marker=estilo["marker"],
                    markersize=3, markevery=max(1, x.size // 12), label=etiqueta)
        ax.set_title(titulo, fontsize=11)
        ax.set_xlabel(f"Fracción acumulada de {unidades}, de menor a mayor")
        ax.set_ylabel("Fracción acumulada del total")
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.legend(loc="upper left")
    fig.suptitle("El consumo tiene una concentración moderada entre canciones; "
                 "el feminicidio se concentra más en conteo que en tasa",
                 x=0.02, ha="left", fontweight="bold", fontsize=11)
    fig.tight_layout()
    guardar(fig, "concentracion_lorenz")


def figura_gini_por_anio(gini: pl.DataFrame) -> None:
    """Gini por año de cada masa, en dos paneles: música y violencia.

    Args:
        gini (pl.DataFrame): resultado de `gini_por_anio`.
    """
    paneles = [
        ("Música", [("streams_canciones", "Entre canciones"), ("streams_artistas", "Entre artistas")]),
        ("Violencia entre entidades", [("feminicidio_conteo", "Feminicidio, conteo"),
                                       ("feminicidio_tasa", "Feminicidio, tasa"),
                                       ("familiar_conteo", "Familiar y de género, conteo"),
                                       ("familiar_tasa", "Familiar y de género, tasa")]),
    ]
    fig, ejes = plt.subplots(1, 2, figsize=(11, 4.5), sharey=True)
    for ax, (titulo, series) in zip(ejes, paneles):
        for i, (variable, etiqueta) in enumerate(series):
            datos = gini.filter(pl.col("variable") == variable).sort("anio")
            ax.plot(datos["anio"], datos["gini"], label=etiqueta, **estilo_serie(i))
        ax.set_title(titulo, fontsize=11)
        ax.set_xlabel("Año")
        ax.set_ylim(0, 1)
        ax.legend(loc="upper right", fontsize=9)
    ejes[0].set_ylabel("Coeficiente de Gini")
    fig.suptitle("Los streams se concentran cada vez más en pocos artistas; "
                 "la violencia entre estados se reparte más que en 2015 y luego se estabiliza",
                 x=0.02, ha="left", fontweight="bold")
    fig.tight_layout()
    guardar(fig, "concentracion_gini_anio")


def figura_entropia_por_anio(entropia: pl.DataFrame) -> None:
    """Entropía normalizada por año del macrogénero y del tipo de violencia.

    Args:
        entropia (pl.DataFrame): resultado de `entropia_por_anio`.
    """
    series = [("macrogenero_streams", "Macrogénero de los streams"),
              ("tipo_violencia_carpetas", "Tipo de violencia de las carpetas")]
    fig, ax = plt.subplots(figsize=(8, 4.5))
    for i, (variable, etiqueta) in enumerate(series):
        datos = entropia.filter(pl.col("variable") == variable).sort("anio")
        ax.plot(datos["anio"], datos["entropia_normalizada"], label=etiqueta, **estilo_serie(i))
    ax.set_ylim(0, 1)
    ax.set_xlabel("Año")
    ax.set_ylabel("Entropía normalizada (0 a 1)")
    ax.set_title("Desde 2020 la música se concentra en un solo macrogénero;\n"
                 "la mezcla de tipos de violencia casi no cambia")
    ax.legend(loc="lower right")
    guardar(fig, "concentracion_entropia_anio")


def ejecutar_concentracion() -> dict[str, pl.DataFrame]:
    """Calcula, imprime y guarda las medidas de concentración y heterogeneidad.

    Returns:
        dict[str, pl.DataFrame]: las tablas `gini`, `entropia` y `entropia_entidad`.
    """
    aplicar_estilo()
    RUTA_TABLAS_EDA.mkdir(parents=True, exist_ok=True)

    gini = gini_por_anio()
    seccion("Gini por año")
    print(gini.select("variable", "anio", "unidades", "gini", "gini_normalizado",
                      "participacion_top_10pct", f"cr_{K_TOP}", "unidades_efectivas"))

    entropia = entropia_por_anio()
    seccion("Entropía normalizada por año")
    print(entropia.select("variable", "anio", "cobertura", "k", "proporcion_moda",
                          "entropia_normalizada", "hill_1"))

    entropia_entidad = entropia_por_entidad(ANIO_LORENZ)
    seccion(f"Entropía del tipo de violencia por entidad, {ANIO_LORENZ}")
    print(entropia_entidad.describe())
    print(entropia_entidad.head(5))
    print(entropia_entidad["tipo_predominante"].value_counts())

    figura_lorenz()
    figura_gini_por_anio(gini)
    figura_entropia_por_anio(entropia)

    tablas = {"gini": gini, "entropia": entropia, "entropia_entidad": entropia_entidad}
    for nombre, tabla in tablas.items():
        tabla.write_parquet(RUTA_TABLAS_EDA / f"concentracion_{nombre}.parquet")
    print(f"\nTablas en {RUTA_TABLAS_EDA}")
    return tablas


if __name__ == "__main__":
    ejecutar_concentracion()
