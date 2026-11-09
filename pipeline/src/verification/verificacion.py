"""
Verificación antes contra después de la preparación. Se vuelven a calcular
las medidas clave sobre el dataset analítico (antes) y sobre el limpio
(después) para comprobar que la limpieza no borró ni inventó el fenómeno que
estudian las preguntas de E0.

Se comparan:
    la distribución de cuatro variables numéricas, con sus estadísticos y
    la prueba de Kolmogorov–Smirnov;
    los hallazgos principales del EDA (entropía del macrogénero, Gini entre
    artistas y Gini del feminicidio entre estados), año por año.

Este script se ejecuta desde pipeline/ con:
    python -m src.verification.verificacion
"""

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import polars as pl
from scipy import stats

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from config.rutas import (  # noqa: E402
    RUTA_LIMPIO_CANCIONES,
    RUTA_LIMPIO_CONSUMO,
    RUTA_LIMPIO_VIOLENCIA,
    RUTA_TABLAS_VERIFICACION,
)
from src.eda import vistas  # noqa: E402
from src.eda.concentracion import SIN_ETIQUETA  # noqa: E402
from src.eda.estilo import aplicar_estilo, estilo_serie, guardar  # noqa: E402
from src.eda.medidas import indices_categoricos, indices_concentracion, resumen_numerico  # noqa: E402
from src.reduction.reduccion import panel_entidad_anio  # noqa: E402
from src.registro import registrar_etapa, seccion  # noqa: E402

pl.Config.set_tbl_rows(40)
pl.Config.set_tbl_cols(12)
pl.Config.set_tbl_width_chars(200)
pl.Config.set_float_precision(3)

ESTADISTICOS = ["n", "media", "mediana", "desv_estandar", "iqr", "asimetria"]

# Diferencia en entropía o Gini por debajo de la cual un hallazgo se considera igual.
TOLERANCIA = 0.01


def consumo_despues() -> pl.DataFrame:
    """Une el consumo limpio con el artista y el macrogénero de cada canción.

    Returns:
        pl.DataFrame: una fila por canción y mes, con `anio`, artista,
            macrogénero y streams.
    """
    canciones = pl.read_parquet(RUTA_LIMPIO_CANCIONES).select("track_id", "artista", "macrogenero")
    return (
        pl.read_parquet(RUTA_LIMPIO_CONSUMO)
        .join(canciones, on="track_id", how="left")
        .with_columns(pl.col("fecha").dt.year().alias("anio"))
    )


def variables_comparadas() -> dict[str, tuple[pl.Series, pl.Series, str]]:
    """Arma cada variable numérica en su versión antes y después.

    Returns:
        dict[str, tuple[pl.Series, pl.Series, str]]: para cada variable, los
            valores antes, los valores después y qué tratamiento la cambió.
    """
    tasas_antes = vistas.tasas_entidad_anio()
    panel = panel_entidad_anio()
    rnpdno_antes = vistas.violencia().filter(pl.col("origen") == "RNPDNO")
    rnpdno_despues = pl.read_parquet(RUTA_LIMPIO_VIOLENCIA).filter(pl.col("origen") == "RNPDNO")
    return {
        "streams por canción y mes": (
            vistas.consumo()["streams"], pl.read_parquet(RUTA_LIMPIO_CONSUMO)["streams"],
            "se quitan los meses con 0 streams"),
        "tasa de feminicidio por entidad y año": (
            tasas_antes.filter(pl.col("tipo_lgamvlv") == "feminicida")["tasa_carpetas"], panel["tasa_feminicidio"],
            "los ceros antes de que el estado registrara el delito pasan a nulos"),
        "tasa de violencia familiar y de género por entidad y año": (
            tasas_antes.filter(pl.col("tipo_lgamvlv") == "sin distinguir")["tasa_carpetas"],
            panel["tasa_familiar_genero"], "ninguno (sirve de control)"),
        "mujeres desaparecidas por entidad y mes (RNPDNO)": (
            rnpdno_antes["victimas_mujeres"], rnpdno_despues["victimas_mujeres"],
            "edades imposibles a nulo; no se quitan filas"),
    }


def tabla_estadisticos() -> pl.DataFrame:
    """Compara los estadísticos de cada variable antes y después, con la prueba KS.

    Returns:
        pl.DataFrame: una fila por variable y estadístico con su valor antes,
            después y el cambio relativo, más el estadístico y el p de KS.
    """
    filas = []
    for variable, (antes, despues, tratamiento) in variables_comparadas().items():
        resumen_antes = resumen_numerico(antes)
        resumen_despues = resumen_numerico(despues)
        ks = stats.ks_2samp(antes.drop_nulls().to_numpy(), despues.drop_nulls().to_numpy())
        for estadistico in ESTADISTICOS:
            a, d = resumen_antes[estadistico], resumen_despues[estadistico]
            filas.append({
                "variable": variable,
                "tratamiento": tratamiento,
                "estadistico": estadistico,
                "antes": a,
                "despues": d,
                "cambio_pct": 100 * (d - a) / abs(a) if a else None,
                "ks_d": ks.statistic,
                "ks_p": ks.pvalue,
            })
    return pl.DataFrame(filas)


def entropia_macrogenero(consumo: pl.DataFrame) -> pl.DataFrame:
    """Entropía normalizada del macrogénero de los streams por año, sin los artistas sin etiqueta.

    Args:
        consumo (pl.DataFrame): una fila por canción y mes con `anio`, `macrogenero` y `streams`.

    Returns:
        pl.DataFrame: columnas `anio` y `valor`.
    """
    etiquetado = consumo.filter(pl.col("macrogenero") != SIN_ETIQUETA)
    filas = []
    for anio in sorted(etiquetado["anio"].unique().to_list()):
        conteos = (etiquetado.filter(pl.col("anio") == anio)
                   .group_by("macrogenero").agg(pl.col("streams").sum())["streams"].to_numpy())
        filas.append({"anio": anio, "valor": indices_categoricos(conteos)["entropia_normalizada"]})
    return pl.DataFrame(filas)


def gini_anual(datos: pl.DataFrame, valor: str) -> pl.DataFrame:
    """Gini por año de una masa repartida entre unidades.

    Las unidades con valor nulo no cuentan; en el dataset limpio son los
    estados que ese año todavía no registraban el delito.

    Args:
        datos (pl.DataFrame): una fila por unidad y año, con `anio`.
        valor (str): columna con la masa.

    Returns:
        pl.DataFrame: columnas `anio`, `valor` (Gini) y `unidades`.
    """
    filas = []
    for anio in sorted(datos["anio"].unique().to_list()):
        x = datos.filter(pl.col("anio") == anio)[valor].drop_nulls().to_numpy()
        if x.size >= 2 and x.sum() > 0:
            filas.append({"anio": anio, "valor": indices_concentracion(x)["gini"], "unidades": x.size})
    return pl.DataFrame(filas)


def tabla_hallazgos() -> pl.DataFrame:
    """Recalcula los hallazgos principales del EDA antes y después, año por año.

    Returns:
        pl.DataFrame: una fila por hallazgo y año con el valor antes, después,
            la diferencia y las unidades que entraron en cada cálculo.
    """
    consumo_antes = vistas.consumo()
    consumo_limpio = consumo_despues()
    artistas = {
        nombre: df.group_by("artista", "anio").agg(pl.col("streams").sum()).filter(pl.col("streams") > 0)
        for nombre, df in (("antes", consumo_antes), ("despues", consumo_limpio))
    }
    tasas_antes = vistas.tasas_entidad_anio().filter(pl.col("tipo_lgamvlv") == "feminicida")
    panel = panel_entidad_anio()

    comparaciones = {
        "entropía del macrogénero (streams)": (entropia_macrogenero(consumo_antes), entropia_macrogenero(consumo_limpio)),
        "Gini de streams entre artistas": (gini_anual(artistas["antes"], "streams"),
                                           gini_anual(artistas["despues"], "streams")),
        "Gini de la tasa de feminicidio entre estados": (gini_anual(tasas_antes, "tasa_carpetas"),
                                                          gini_anual(panel, "tasa_feminicidio")),
    }
    tablas = []
    for hallazgo, (antes, despues) in comparaciones.items():
        unidades = ["unidades"] if "unidades" in antes.columns else []
        tablas.append(
            antes.rename({"valor": "antes", **{u: "unidades_antes" for u in unidades}})
            .join(despues.rename({"valor": "despues", **{u: "unidades_despues" for u in unidades}}),
                  on="anio", how="full", coalesce=True)
            .with_columns(pl.lit(hallazgo).alias("hallazgo"), (pl.col("despues") - pl.col("antes")).alias("diferencia"))
        )
    return pl.concat(tablas, how="diagonal").select(
        "hallazgo", "anio", "antes", "despues", "diferencia", "unidades_antes", "unidades_despues"
    ).sort("hallazgo", "anio")


def conclusion(hallazgos: pl.DataFrame) -> pl.DataFrame:
    """Resume, por hallazgo, si la preparación lo conservó.

    Args:
        hallazgos (pl.DataFrame): resultado de `tabla_hallazgos`.

    Returns:
        pl.DataFrame: una fila por hallazgo con el primer y el último valor
            antes y después, la mayor diferencia anual y los años que cambiaron.
    """
    return (
        hallazgos.drop_nulls(["antes", "despues"])
        .group_by("hallazgo")
        .agg(
            pl.col("anio").first().alias("primer_anio"),
            pl.col("antes").first().alias("primero_antes"),
            pl.col("despues").first().alias("primero_despues"),
            pl.col("anio").last().alias("ultimo_anio"),
            pl.col("antes").last().alias("ultimo_antes"),
            pl.col("despues").last().alias("ultimo_despues"),
            pl.col("diferencia").abs().max().alias("mayor_diferencia"),
            pl.col("anio").filter(pl.col("diferencia").abs() >= TOLERANCIA).cast(pl.String).str.join(", ")
            .alias("anios_que_cambian"),
        )
        .with_columns(
            pl.when(pl.col("mayor_diferencia") < TOLERANCIA).then(pl.lit("se conserva igual"))
            .when((pl.col("ultimo_despues") - pl.col("primero_despues")).sign()
                  == (pl.col("ultimo_antes") - pl.col("primero_antes")).sign())
            .then(pl.lit("se conserva la tendencia; cambia la magnitud en algunos años"))
            .otherwise(pl.lit("cambia la tendencia: revisar")).alias("lectura")
        )
        .sort("hallazgo")
    )


def figura_antes_despues(hallazgos: pl.DataFrame, resumen: pl.DataFrame) -> None:
    """Dibuja cada hallazgo por año antes y después de la preparación, un panel por hallazgo.

    Args:
        hallazgos (pl.DataFrame): resultado de `tabla_hallazgos`.
        resumen (pl.DataFrame): resultado de `conclusion`, para el título.
    """
    nombres = hallazgos["hallazgo"].unique(maintain_order=True).to_list()
    fig, ejes = plt.subplots(1, len(nombres), figsize=(5 * len(nombres), 4.2), sharex=True)
    for ax, nombre in zip(np.atleast_1d(ejes), nombres):
        datos = hallazgos.filter(pl.col("hallazgo") == nombre)
        for i, (columna, etiqueta) in enumerate((("antes", "Antes de limpiar"), ("despues", "Después de limpiar"))):
            serie = datos.drop_nulls(columna)
            ax.plot(serie["anio"], serie[columna], **estilo_serie(i), label=etiqueta,
                    alpha=0.9 if i == 0 else 1.0, markersize=7 if i == 0 else 5)
        ax.set_title(nombre[0].upper() + nombre[1:], fontsize=10)
        ax.set_ylim(0, 1)
        ax.set_xlabel("Año")
        ax.legend(loc="lower left")
    np.atleast_1d(ejes)[0].set_ylabel("Valor del índice (0 a 1)")
    anios = resumen.filter(pl.col("hallazgo").str.contains("feminicidio"))["anios_que_cambian"][0].split(", ")
    fig.suptitle("La limpieza conserva los hallazgos de la música; en el feminicidio solo cambia "
                 f"la desigualdad entre estados de {anios[0]} a {anios[-1]}", x=0.01, ha="left", fontsize=12,
                 fontweight="bold")
    fig.tight_layout()
    guardar(fig, "verificacion_antes_despues")


def ejecutar_verificacion() -> dict[str, pl.DataFrame]:
    """Compara el dataset antes y después de la preparación y guarda los resultados.

    Returns:
        dict[str, pl.DataFrame]: tablas `estadisticos`, `hallazgos` y `conclusion`.
    """
    aplicar_estilo()
    RUTA_TABLAS_VERIFICACION.mkdir(parents=True, exist_ok=True)

    seccion("Estadísticos antes y después")
    estadisticos = tabla_estadisticos()
    print(estadisticos.drop("tratamiento"))

    seccion("Hallazgos del EDA antes y después")
    hallazgos = tabla_hallazgos()
    print(hallazgos)
    resumen = conclusion(hallazgos)
    with pl.Config(fmt_str_lengths=70):
        print(resumen.select("hallazgo", "primero_antes", "primero_despues", "ultimo_antes", "ultimo_despues",
                             "mayor_diferencia", "anios_que_cambian", "lectura"))
    assert not resumen["lectura"].str.starts_with("cambia la tendencia").any(), "la preparación cambió un hallazgo"
    figura_antes_despues(hallazgos, resumen)

    tablas = {"estadisticos": estadisticos, "hallazgos": hallazgos, "conclusion": resumen}
    for nombre, tabla in tablas.items():
        tabla.write_parquet(RUTA_TABLAS_VERIFICACION / f"{nombre}.parquet")
    registrar_etapa("verificacion", "antes_despues", None, resumen.height,
                    "; ".join(f"{h}: {lectura}" for h, lectura in resumen.select("hallazgo", "lectura").iter_rows()))
    return tablas


if __name__ == "__main__":
    ejecutar_verificacion()
