"""
Estadística descriptiva del dataset analítico en las cuatro dimensiones que
pide la entrega: localización, variabilidad y forma de las variables
numéricas, comparación entre grupos, y heterogeneidad de las categóricas.
Guarda las tablas en data/data-processed/eda/ y las figuras en la carpeta de
imágenes de E2.

Este script se ejecuta desde pipeline/ con:
    python -m src.eda.descriptiva
"""

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import polars as pl

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

import re  # noqa: E402

from matplotlib.collections import PolyCollection  # noqa: E402

from config.parametros import REGIONES_BANXICO  # noqa: E402
from config.rutas import RUTA_TABLAS_EDA  # noqa: E402
from src.cleaning.limpieza import limpiar_entidades  # noqa: E402
from src.eda import vistas  # noqa: E402
from src.eda.estilo import COLORES, REFERENCIA, aplicar_estilo, estilo_serie, guardar  # noqa: E402
from src.eda.medidas import descomposicion_anova, indices_categoricos, resumen_numerico  # noqa: E402
from src.ingestion.entidades import construir_catalogo  # noqa: E402
from src.registro import seccion  # noqa: E402

pl.Config.set_tbl_rows(40)
pl.Config.set_tbl_cols(20)
pl.Config.set_tbl_width_chars(220)
pl.Config.set_tbl_hide_dataframe_shape(True)
pl.Config.set_float_precision(3)

# En una normal, la desviación estándar es IQR / 1.349; una razón mucho mayor
# que 1 indica colas pesadas o atípicos.
IQR_NORMAL = 1.349

# Año más reciente, para las comparaciones entre estados y regiones.
ANIO_RECIENTE = 2025

# Nombre de cada tipo en las figuras.
NOMBRE_TIPO = {"feminicida": "Feminicidio", "sexual": "Sexual", "física": "Física (lesiones)",
               "sin distinguir": "Familiar y de género"}

# Cada tipo de violencia conserva su color en todas las figuras.
COLOR_TIPO = {"sin distinguir": COLORES[0], "física": COLORES[1], "sexual": COLORES[2], "feminicida": COLORES[3]}

LOCALIZACION = ["n", "media", "mediana", "moda", "media_recortada_10", "p10", "q1", "q3", "p90"]
VARIABILIDAD = ["minimo", "maximo", "rango", "desv_estandar", "iqr", "mad", "coef_variacion"]
FORMA = ["media_sobre_mediana", "de_sobre_iqr_normal", "asimetria", "curtosis"]


def variables_numericas() -> dict[str, tuple[str, pl.Series]]:
    """Reúne las variables numéricas que se describen, con su unidad de análisis.

    Returns:
        dict[str, tuple[str, pl.Series]]: para cada variable, la unidad de
            análisis (texto) y sus valores.
    """
    consumo = vistas.consumo().filter(pl.col("streams") > 0)
    anual = vistas.consumo_cancion_anio()
    tasas = vistas.tasas_entidad_anio()
    contexto = vistas.contexto()

    variables = {
        "streams_mes": ("canción y mes en el chart", consumo["streams"]),
        "semanas_en_chart_mes": ("canción y mes en el chart", consumo["semanas_en_chart"]),
        "streams_anio": ("canción y año en el chart", anual["streams"]),
    }
    for tipo in vistas.TIPOS_ANALIZADOS:
        datos = tasas.filter(pl.col("tipo_lgamvlv") == tipo)
        variables[f"tasa_{tipo.replace(' ', '_')}"] = ("entidad y año", datos["tasa_carpetas"])
    # Las carpetas de lesiones incluyen víctimas hombres; esta tasa cuenta solo a las mujeres.
    fisica = tasas.filter(pl.col("tipo_lgamvlv") == "física")
    variables["tasa_victimas_fisica_mujeres"] = ("entidad y año", fisica["tasa_victimas"])
    for columna in ("pct_pobreza", "pct_viviendas_internet", "pct_usuarios_internet", "indice_impunidad"):
        variables[columna] = ("entidad, año medido", contexto[columna])
    return variables


def tabla_numericas() -> pl.DataFrame:
    """Calcula las medidas de las cuatro dimensiones para cada variable numérica.

    Agrega dos comparaciones: media entre mediana (mayor a 1 indica sesgo a la
    derecha) y desviación estándar entre la que tendría una normal con el
    mismo IQR (mayor a 1 indica colas pesadas o atípicos).

    Returns:
        pl.DataFrame: una fila por variable con todas las medidas.
    """
    filas = []
    for nombre, (unidad, valores) in variables_numericas().items():
        medidas = resumen_numerico(valores)
        medidas["media_sobre_mediana"] = medidas["media"] / medidas["mediana"] if medidas["mediana"] else np.nan
        medidas["de_sobre_iqr_normal"] = (
            medidas["desv_estandar"] / (medidas["iqr"] / IQR_NORMAL) if medidas["iqr"] else np.nan
        )
        filas.append({"variable": nombre, "unidad": unidad, **medidas})
    return pl.DataFrame(filas)


def region_banxico() -> pl.DataFrame:
    """Devuelve la región del Banco de México de cada entidad.

    Returns:
        pl.DataFrame: columnas `cve_ent` (int) y `region` (str).
    """
    return pl.DataFrame(
        [(cve, nombre) for nombre, claves in REGIONES_BANXICO.items() for cve in claves],
        schema={"cve_ent": pl.Int64, "region": pl.String}, orient="row",
    )


def comparacion_por_grupo() -> pl.DataFrame:
    """Compara variables numéricas entre grupos: medianas, IQR y eta cuadrada.

    Las tasas se comparan entre años; los streams anuales por canción, entre
    macrogéneros. Los streams se pasan a logaritmo base 10 para eta cuadrada,
    porque su sesgo haría que unas pocas canciones dominaran las sumas de
    cuadrados.

    Returns:
        pl.DataFrame: una fila por variable y grupo con n, mediana, q1, q3,
            IQR, y la eta y omega cuadradas de la variable completa.
    """
    tasas = vistas.tasas_entidad_anio()
    anual = vistas.consumo_cancion_anio().with_columns(pl.col("streams").log10().alias("log10_streams"))

    comparaciones = [
        (f"tasa_{tipo.replace(' ', '_')}", tasas.filter(pl.col("tipo_lgamvlv") == tipo), "tasa_carpetas", "anio")
        for tipo in vistas.TIPOS_ANALIZADOS
    ]
    fisica = tasas.filter(pl.col("tipo_lgamvlv") == "física")
    comparaciones.append(("tasa_victimas_fisica_mujeres", fisica, "tasa_victimas", "anio"))
    recientes = tasas.filter(pl.col("anio") == ANIO_RECIENTE).join(region_banxico(), on="cve_ent")
    for tipo in vistas.TIPOS_ANALIZADOS:
        columna = "tasa_victimas" if tipo == "física" else "tasa_carpetas"
        nombre = "tasa_victimas_fisica_mujeres" if tipo == "física" else f"tasa_{tipo.replace(' ', '_')}"
        comparaciones.append((f"{nombre}_{ANIO_RECIENTE}_region",
                              recientes.filter(pl.col("tipo_lgamvlv") == tipo), columna, "region"))
    comparaciones.append(("log10_streams_anio", anual, "log10_streams", "macrogenero"))

    resultados = []
    for nombre, datos, valor, grupo in comparaciones:
        anova = descomposicion_anova(datos[valor], datos[grupo].cast(pl.String))
        resumen = (
            datos.group_by(pl.col(grupo).cast(pl.String).alias("grupo"))
            .agg(
                pl.len().alias("n"),
                pl.col(valor).median().alias("mediana"),
                pl.col(valor).quantile(0.25).alias("q1"),
                pl.col(valor).quantile(0.75).alias("q3"),
            )
            .with_columns(
                (pl.col("q3") - pl.col("q1")).alias("iqr"),
                pl.lit(nombre).alias("variable"),
                pl.lit(anova["eta2"]).alias("eta2"),
                pl.lit(anova["omega2"]).alias("omega2"),
            )
            .sort("grupo")
        )
        resultados.append(resumen)
    return pl.concat(resultados).select("variable", "grupo", "n", "mediana", "q1", "q3", "iqr", "eta2", "omega2")


def tabla_categoricas() -> pl.DataFrame:
    """Calcula la heterogeneidad de las variables categóricas del dataset.

    Cada variable se mide sobre la masa que importa para la pregunta de E0:
    el macrogénero por streams y por canciones, los tipos de violencia por
    carpetas y el país del artista por canciones.

    Returns:
        pl.DataFrame: una fila por variable con su base, la categoría moda y
            los índices de heterogeneidad.
    """
    consumo = vistas.consumo()
    canciones = consumo.select("track_id", "macrogenero", "pais_artista").unique()
    violencia = vistas.violencia().filter(pl.col("origen") == "SESNSP")

    casos = [
        ("macrogenero", "streams", consumo.group_by("macrogenero").agg(pl.col("streams").sum().alias("masa"))),
        ("macrogenero", "canciones", canciones.group_by("macrogenero").agg(pl.len().alias("masa"))),
        ("pais_artista", "canciones", canciones.group_by(pl.col("pais_artista").fill_null("sin dato")).agg(pl.len().alias("masa"))),
        ("tipo_lgamvlv", "carpetas", violencia.filter(pl.col("tipo_lgamvlv").is_in(vistas.TIPOS_ANALIZADOS))
         .group_by("tipo_lgamvlv").agg(pl.col("carpetas").sum().alias("masa"))),
        ("subtipo_de_delito", "carpetas", violencia.group_by("subtipo_de_delito").agg(pl.col("carpetas").sum().alias("masa"))),
    ]
    filas = []
    for variable, base, conteos in casos:
        conteos = conteos.sort("masa", descending=True)
        indices = indices_categoricos(conteos["masa"].to_numpy())
        filas.append({"variable": variable, "base": base, "moda": str(conteos[0, 0]), **indices})
    return pl.DataFrame(filas)


def figura_streams() -> None:
    """Histograma de los streams anuales por canción en escala logarítmica.

    Cada observación es una canción en un año en que estuvo en el chart.

    La escala logarítmica se justifica porque los streams abarcan varios
    órdenes de magnitud: en escala lineal casi todas las canciones quedarían
    en la primera barra.
    """
    anual = vistas.consumo_cancion_anio()
    log_streams = anual["streams"].log10().to_numpy()
    mediana = float(np.median(anual["streams"].to_numpy()))
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.hist(log_streams, bins=30, color=COLORES[0], edgecolor="white", linewidth=0.8)
    ax.axvline(np.log10(mediana), color=REFERENCIA, linestyle="--", linewidth=1.5)
    ax.annotate(f"mediana: {mediana / 1e6:.1f} millones", (np.log10(mediana), ax.get_ylim()[1] * 0.92),
                xytext=(8, 0), textcoords="offset points", color=REFERENCIA)
    marcas = np.arange(np.floor(log_streams.min()), np.ceil(log_streams.max()) + 1)
    ax.set_xticks(marcas, [f"{10**m:,.0f}" for m in marcas], rotation=30)
    ax.set_title("Una canción suma unos 55 millones de streams en un año típico;\n"
                 "la cola baja son los años en que apenas entró o salió del chart")
    ax.set_xlabel("Streams de la canción en el año (escala logarítmica)")
    ax.set_ylabel("Canciones y años")
    guardar(fig, "descriptiva_hist_streams")


def figura_tasas_por_anio() -> None:
    """Boxplots de la tasa de carpetas por entidad para cada año, uno por tipo de violencia."""
    tasas = vistas.tasas_entidad_anio()
    anios = sorted(tasas["anio"].unique().to_list())
    fig, ejes = plt.subplots(2, 2, figsize=(11, 7), sharex=True)
    for tipo, ax in zip(vistas.TIPOS_ANALIZADOS, ejes.flat):
        color = COLOR_TIPO[tipo]
        datos = [tasas.filter((pl.col("tipo_lgamvlv") == tipo) & (pl.col("anio") == a))["tasa_carpetas"].to_numpy()
                 for a in anios]
        ax.boxplot(datos, tick_labels=[str(a) for a in anios], widths=0.6, patch_artist=True,
                   boxprops={"facecolor": color, "alpha": 0.35, "edgecolor": color},
                   medianprops={"color": color, "linewidth": 2},
                   whiskerprops={"color": color}, capprops={"color": color},
                   flierprops={"marker": "o", "markersize": 3, "markerfacecolor": color, "markeredgecolor": "none"})
        ax.set_title(f"Violencia {tipo}" if tipo != "sin distinguir" else "Violencia familiar y de género", fontsize=11)
        ax.tick_params(axis="x", rotation=45)
        ax.set_ylabel("Carpetas por 100 mil mujeres")
    fig.suptitle("Las tasas por entidad varían mucho entre estados y la familiar crece casi cada año",
                 x=0.02, ha="left", fontweight="bold")
    fig.tight_layout()
    guardar(fig, "descriptiva_box_tasas_anio")


def figura_macrogeneros() -> None:
    """Barras ordenadas de la participación de cada macrogénero en los streams."""
    consumo = vistas.consumo()
    total = consumo["streams"].sum()
    datos = (
        consumo.group_by("macrogenero")
        .agg((pl.col("streams").sum() / total * 100).alias("pct"))
        .sort("pct")
    )
    fig, ax = plt.subplots(figsize=(8, 4.5))
    colores = [REFERENCIA if g == "sin etiqueta" else COLORES[0] for g in datos["macrogenero"]]
    ax.barh(datos["macrogenero"], datos["pct"], color=colores, height=0.6)
    for y, pct in enumerate(datos["pct"]):
        ax.annotate(f"{pct:.1f} %", (pct, y), xytext=(4, 0), textcoords="offset points", va="center")
    ax.set_title("El regional mexicano y el urbano latino suman más de la mitad del consumo de la muestra")
    ax.set_xlabel("Porcentaje de los streams 2015–2025")
    ax.grid(axis="y", visible=False)
    guardar(fig, "descriptiva_barras_macrogenero")


def figura_series_violencia() -> None:
    """Serie mensual nacional de carpetas de cada tipo de violencia, un panel por tipo.

    Los tipos tienen escalas muy distintas (decenas de feminicidios contra
    miles de carpetas de violencia familiar), así que cada uno va en su
    propio panel con su propio eje en lugar de compartir uno solo.
    """
    serie = (
        vistas.violencia()
        .filter((pl.col("origen") == "SESNSP") & pl.col("tipo_lgamvlv").is_in(vistas.TIPOS_ANALIZADOS))
        .group_by("fecha", "tipo_lgamvlv")
        .agg(pl.col("carpetas").sum())
        .sort("fecha")
    )
    fig, ejes = plt.subplots(2, 2, figsize=(11, 6.5), sharex=True)
    for tipo, ax in zip(vistas.TIPOS_ANALIZADOS, ejes.flat):
        datos = serie.filter(pl.col("tipo_lgamvlv") == tipo)
        ax.plot(datos["fecha"], datos["carpetas"], color=COLOR_TIPO[tipo], linewidth=1.6)
        ax.set_title(f"Violencia {tipo}" if tipo != "sin distinguir" else "Violencia familiar y de género", fontsize=11)
        ax.set_ylabel("Carpetas por mes")
        ax.set_ylim(bottom=0)
    fig.suptitle("Las carpetas de violencia sexual y familiar se triplican de 2015 a 2023; "
                 "el feminicidio sube hasta 2021 y después baja",
                 x=0.02, ha="left", fontweight="bold")
    fig.tight_layout()
    guardar(fig, "descriptiva_series_violencia")


def figura_canciones_por_anio() -> None:
    """Número de canciones de la muestra con streams en cada año."""
    anual = vistas.consumo_cancion_anio().group_by("anio").agg(pl.len().alias("canciones")).sort("anio")
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(anual["anio"], anual["canciones"], **estilo_serie(0))
    for anio, n in anual.filter(pl.col("anio").is_in([2015, 2025])).iter_rows():
        ax.annotate(f"{n}", (anio, n), xytext=(0, 8), textcoords="offset points", ha="center")
    ax.set_title("La muestra de 300 canciones casi no tiene canciones de los primeros años")
    ax.set_xlabel("Año")
    ax.set_ylabel("Canciones con streams en el año")
    ax.set_ylim(bottom=0)
    guardar(fig, "descriptiva_canciones_por_anio")


def entidades() -> pl.DataFrame:
    """Arma las 32 entidades con su nombre oficial y su geometría desde staging.

    El EDA corre antes de la limpieza, así que no puede leer la tabla limpia
    de entidades; usa la misma función que la limpieza, sin guardar nada.

    Returns:
        pl.DataFrame: columnas `cve_ent`, `entidad`, `iso_3166_2` y `geometry_wkt`.
    """
    return limpiar_entidades([], construir_catalogo())


def anillos_wkt(wkt: str) -> list[np.ndarray]:
    """Extrae los contornos exteriores de un POLYGON o MULTIPOLYGON en texto WKT.

    En WKT el contorno exterior de cada polígono va justo después de "((" y
    los huecos después de ", (". Los huecos no se dibujan: el Estado de
    México tiene uno donde está la Ciudad de México y, si se rellenara, la
    taparía.

    Args:
        wkt (str): geometría en WKT.

    Returns:
        list[np.ndarray]: un arreglo de coordenadas (lon, lat) por contorno.
    """
    return [np.array([[float(v) for v in par.split()] for par in anillo.split(",")])
            for anillo in re.findall(r"\(\(([^()]+)\)", wkt)]


def figura_mapa_tasas() -> None:
    """Mapas coropléticos de la tasa de violencia familiar y sexual por entidad en el año reciente.

    Se usan tasas por 100 mil mujeres y no conteos, porque un mapa de conteos
    mostraría dónde vive más gente y no dónde es más frecuente la violencia.
    """
    geometrias = entidades().select("cve_ent", "geometry_wkt")
    tasas = vistas.tasas_entidad_anio().filter(pl.col("anio") == ANIO_RECIENTE)
    fig, ejes = plt.subplots(1, 2, figsize=(13, 4.6), layout="constrained")
    for ax, tipo in zip(ejes, ["sin distinguir", "sexual"]):
        datos = geometrias.join(tasas.filter(pl.col("tipo_lgamvlv") == tipo).select("cve_ent", "tasa_carpetas"), on="cve_ent")
        poligonos, valores = [], []
        for wkt, valor in datos.select("geometry_wkt", "tasa_carpetas").iter_rows():
            for anillo in anillos_wkt(wkt):
                poligonos.append(anillo)
                valores.append(valor)
        coleccion = PolyCollection(poligonos, array=np.array(valores), cmap="Blues", edgecolor="white", linewidth=0.4)
        ax.add_collection(coleccion)
        ax.autoscale_view()
        ax.set_aspect("equal")
        ax.axis("off")
        fig.colorbar(coleccion, ax=ax, shrink=0.7, label="Carpetas por 100 mil mujeres")
        mayor = datos.sort("tasa_carpetas", descending=True).row(0, named=True)
        menor = datos.sort("tasa_carpetas").row(0, named=True)
        ax.set_title(f"{NOMBRE_TIPO[tipo]}, {ANIO_RECIENTE}: de {menor['tasa_carpetas']:.0f} a {mayor['tasa_carpetas']:.0f}",
                     fontsize=11)
    fig.suptitle(f"Las tasas cambian decenas de veces entre estados ({ANIO_RECIENTE}); los valores más bajos son "
                 "estados que casi no registran el delito", x=0.02, ha="left", fontweight="bold")
    guardar(fig, "descriptiva_mapa_tasas")


def figura_tipos_por_entidad() -> None:
    """Barras apiladas al 100 % de la mezcla de tipos de violencia en cada entidad (año reciente)."""
    nombres = entidades().select("cve_ent", "entidad")
    mezcla = (
        vistas.tasas_entidad_anio().filter(pl.col("anio") == ANIO_RECIENTE)
        .with_columns((pl.col("carpetas") / pl.col("carpetas").sum().over("cve_ent") * 100).alias("pct"))
        .join(nombres, on="cve_ent")
        .pivot(on="tipo_lgamvlv", index="entidad", values="pct")
        .fill_null(0)
        .sort("sin distinguir")
    )
    fig, ax = plt.subplots(figsize=(9, 9))
    izquierda = np.zeros(mezcla.height)
    for tipo in ["sin distinguir", "física", "sexual", "feminicida"]:
        ax.barh(mezcla["entidad"], mezcla[tipo], left=izquierda, color=COLOR_TIPO[tipo], height=0.75,
                edgecolor="white", linewidth=0.5, label=NOMBRE_TIPO[tipo])
        izquierda += mezcla[tipo].to_numpy()
    ax.set_xlim(0, 100)
    ax.set_xlabel(f"Porcentaje de las carpetas de violencia de género, {ANIO_RECIENTE}")
    ax.grid(axis="y", visible=False)
    ax.legend(loc="lower center", bbox_to_anchor=(0.5, 1.0), ncol=4, frameon=False)
    ax.set_title("La violencia familiar es el tipo más frecuente en casi todos los estados", pad=28)
    guardar(fig, "descriptiva_tipos_por_entidad")


def figura_tasas_por_region() -> None:
    """Boxplots de la tasa de cada tipo por región del Banco de México, ordenados por mediana."""
    tasas = vistas.tasas_entidad_anio().filter(pl.col("anio") == ANIO_RECIENTE).join(region_banxico(), on="cve_ent")
    paneles = [("sin distinguir", "tasa_carpetas"), ("sexual", "tasa_carpetas"),
               ("física", "tasa_victimas"), ("feminicida", "tasa_carpetas")]
    fig, ejes = plt.subplots(2, 2, figsize=(11, 7))
    for (tipo, columna), ax in zip(paneles, ejes.flat):
        color = COLOR_TIPO[tipo]
        datos = tasas.filter(pl.col("tipo_lgamvlv") == tipo)
        orden = datos.group_by("region").agg(pl.col(columna).median()).sort(columna)["region"].to_list()
        valores = [datos.filter(pl.col("region") == r)[columna].to_numpy() for r in orden]
        ax.boxplot(valores, tick_labels=orden, widths=0.55, patch_artist=True,
                   boxprops={"facecolor": color, "alpha": 0.35, "edgecolor": color},
                   medianprops={"color": color, "linewidth": 2},
                   whiskerprops={"color": color}, capprops={"color": color},
                   flierprops={"marker": "o", "markersize": 4, "markerfacecolor": color, "markeredgecolor": "none"})
        unidad = "Víctimas mujeres" if columna == "tasa_victimas" else "Carpetas"
        ax.set_ylabel(f"{unidad} por 100 mil mujeres")
        ax.set_title(NOMBRE_TIPO[tipo] + (" (solo víctimas mujeres)" if columna == "tasa_victimas" else ""), fontsize=11)
    fig.suptitle(f"Las diferencias entre regiones son menores que las diferencias dentro de cada región ({ANIO_RECIENTE})",
                 x=0.02, ha="left", fontweight="bold")
    fig.tight_layout()
    guardar(fig, "descriptiva_tasas_region")


def ejecutar_descriptiva() -> dict[str, pl.DataFrame]:
    """Calcula, imprime y guarda la estadística descriptiva y sus figuras.

    Returns:
        dict[str, pl.DataFrame]: las tablas `numericas`, `por_grupo` y `categoricas`.
    """
    aplicar_estilo()
    RUTA_TABLAS_EDA.mkdir(parents=True, exist_ok=True)

    numericas = tabla_numericas()
    seccion("Localización")
    print(numericas.select("variable", "unidad", *LOCALIZACION))
    seccion("Variabilidad")
    print(numericas.select("variable", *VARIABILIDAD))
    seccion("Forma y comparaciones (media/mediana, DE/DE esperada por IQR)")
    print(numericas.select("variable", *FORMA))

    por_grupo = comparacion_por_grupo()
    seccion("Comparación entre grupos")
    print(por_grupo)

    categoricas = tabla_categoricas()
    seccion("Heterogeneidad de variables categóricas")
    print(categoricas)

    figura_streams()
    figura_tasas_por_anio()
    figura_macrogeneros()
    figura_series_violencia()
    figura_canciones_por_anio()
    figura_mapa_tasas()
    figura_tipos_por_entidad()
    figura_tasas_por_region()

    tablas = {"numericas": numericas, "por_grupo": por_grupo, "categoricas": categoricas}
    for nombre, tabla in tablas.items():
        tabla.write_parquet(RUTA_TABLAS_EDA / f"descriptiva_{nombre}.parquet")
    print(f"\nTablas en {RUTA_TABLAS_EDA}")
    return tablas


if __name__ == "__main__":
    ejecutar_descriptiva()
