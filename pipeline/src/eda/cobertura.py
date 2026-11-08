"""
Cobertura de la muestra de Kworb: qué parte del consumo nacional del Top 200
de Spotify México representan las 300 canciones del historial, medida contra
el chart diario de Kaggle (2017–2021), que sí trae todas las canciones del
Top 200 de cada día.

También compara, para las canciones que están en ambas fuentes, los streams
de Kworb con los de Kaggle, para saber si la serie de Kworb es consistente.
El chart viral50 de Kaggle no trae streams y no se usa.

Este script se ejecuta desde pipeline/ con:
    python -m src.eda.cobertura
"""

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import polars as pl

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from config.parametros import TABLA_KWORB_CATALOGO  # noqa: E402
from config.rutas import RUTA_TABLAS_EDA  # noqa: E402
from src.eda import vistas  # noqa: E402
from src.eda.estilo import aplicar_estilo, estilo_serie, guardar  # noqa: E402
from src.ingestion.ensamblado import leer  # noqa: E402
from src.registro import seccion  # noqa: E402

CHART = "top200"
PATRON_TRACK = r"track/([A-Za-z0-9]+)"


def top200_kaggle() -> pl.DataFrame:
    """Lee el Top 200 diario de Kaggle con el ID de Spotify de cada canción.

    Returns:
        pl.DataFrame: columnas `track_id`, `fecha` (date), `anio` y `streams` (int).
    """
    return (
        leer("stg_kaggle_charts_mx")
        .filter(pl.col("chart") == CHART)
        .select(
            pl.col("url").str.extract(PATRON_TRACK).alias("track_id"),
            pl.col("date").str.to_date("%Y-%m-%d").alias("fecha"),
            pl.col("streams").cast(pl.Int64),
        )
        .with_columns(pl.col("fecha").dt.year().alias("anio"))
    )


def cobertura_por_anio(kaggle: pl.DataFrame, muestra: pl.Series) -> pl.DataFrame:
    """Calcula, por año, qué parte del Top 200 de Kaggle está en la muestra de Kworb.

    Args:
        kaggle (pl.DataFrame): resultado de `top200_kaggle`.
        muestra (pl.Series): IDs de Spotify de las canciones de Kworb.

    Returns:
        pl.DataFrame: una fila por año con días, canciones del Top 200,
            canciones de la muestra, streams totales y la cobertura en
            canciones y en streams.
    """
    return (
        kaggle.with_columns(pl.col("track_id").is_in(muestra.implode()).alias("en_muestra"))
        .group_by("anio")
        .agg(
            pl.col("fecha").n_unique().alias("dias"),
            pl.col("fecha").min().alias("desde"),
            pl.col("fecha").max().alias("hasta"),
            pl.col("track_id").n_unique().alias("canciones_top200"),
            pl.col("track_id").filter("en_muestra").n_unique().alias("canciones_en_muestra"),
            pl.col("streams").sum().alias("streams_top200"),
            pl.col("streams").filter("en_muestra").sum().alias("streams_en_muestra"),
        )
        .with_columns(
            (pl.col("canciones_en_muestra") / pl.col("canciones_top200")).alias("cobertura_canciones"),
            (pl.col("streams_en_muestra") / pl.col("streams_top200")).alias("cobertura_streams"),
        )
        .sort("anio")
    )


def consistencia(kaggle: pl.DataFrame) -> pl.DataFrame:
    """Compara los streams anuales de Kworb y Kaggle de las canciones que están en ambas.

    Kworb cuenta las semanas en que la canción estuvo en el chart semanal y
    Kaggle los días en que estuvo en el diario, así que no tienen por qué
    coincidir exactamente; una razón cercana a 1 indica que miden lo mismo.

    Args:
        kaggle (pl.DataFrame): resultado de `top200_kaggle`.

    Returns:
        pl.DataFrame: una fila por año con los streams de cada fuente para
            las mismas canciones, la razón Kworb/Kaggle y la correlación de
            Spearman entre canciones.
    """
    kworb = vistas.consumo_cancion_anio().select("track_id", pl.col("anio").cast(pl.Int32), pl.col("streams").alias("kworb"))
    por_cancion = (
        kaggle.group_by("track_id", pl.col("anio").cast(pl.Int32)).agg(pl.col("streams").sum().alias("kaggle"))
        .join(kworb, on=["track_id", "anio"])
    )
    return (
        por_cancion.group_by("anio")
        .agg(
            pl.len().alias("canciones"),
            pl.col("kworb").sum(),
            pl.col("kaggle").sum(),
            pl.corr("kworb", "kaggle", method="spearman").alias("spearman"),
        )
        .with_columns((pl.col("kworb") / pl.col("kaggle")).alias("razon_kworb_kaggle"))
        .sort("anio")
    )


def figura_cobertura(cobertura: pl.DataFrame) -> None:
    """Dibuja la cobertura de la muestra por año, en canciones y en streams.

    Args:
        cobertura (pl.DataFrame): resultado de `cobertura_por_anio`.
    """
    fig, ax = plt.subplots(figsize=(8, 4.2))
    for i, (columna, etiqueta) in enumerate((("cobertura_streams", "Parte de los streams"),
                                             ("cobertura_canciones", "Parte de las canciones"))):
        ax.plot(cobertura["anio"], cobertura[columna], **estilo_serie(i), label=etiqueta)
        ultimo = cobertura.row(-1, named=True)
        ax.annotate(f"{ultimo[columna]:.0%}", (ultimo["anio"], ultimo[columna]), xytext=(6, 0),
                    textcoords="offset points", va="center", fontsize=9)
    ax.set_ylim(0, 1)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0%}"))
    ax.set_xticks(cobertura["anio"].to_list())
    ax.set_xlabel("Año")
    ax.set_ylabel("Cobertura del Top 200 diario")
    ax.legend(loc="upper left")
    inicio, fin = cobertura.row(0, named=True), cobertura.row(-1, named=True)
    ax.set_title(f"Las 300 canciones cubren {inicio['cobertura_streams']:.0%} de los streams del Top 200 en "
                 f"{inicio['anio']} y {fin['cobertura_streams']:.0%} en {fin['anio']}", fontsize=11)
    guardar(fig, "cobertura_kworb")


def ejecutar_cobertura() -> dict[str, pl.DataFrame]:
    """Mide la cobertura y la consistencia de Kworb y guarda las tablas y la figura.

    Returns:
        dict[str, pl.DataFrame]: tablas `cobertura` y `consistencia`.
    """
    aplicar_estilo()
    RUTA_TABLAS_EDA.mkdir(parents=True, exist_ok=True)
    kaggle = top200_kaggle()
    assert kaggle["track_id"].null_count() == 0, "hay filas de Kaggle sin ID de Spotify en la URL"
    muestra = leer(TABLA_KWORB_CATALOGO)["track_id"]

    seccion("Cobertura de la muestra de Kworb en el Top 200 de Kaggle")
    cobertura = cobertura_por_anio(kaggle, muestra)
    print(cobertura)
    seccion("Streams de Kworb contra Kaggle en las mismas canciones")
    comparacion = consistencia(kaggle)
    print(comparacion)
    figura_cobertura(cobertura)

    tablas = {"cobertura": cobertura, "consistencia": comparacion}
    for nombre, tabla in tablas.items():
        tabla.write_parquet(RUTA_TABLAS_EDA / f"cobertura_{nombre}.parquet")
    return tablas


if __name__ == "__main__":
    ejecutar_cobertura()
