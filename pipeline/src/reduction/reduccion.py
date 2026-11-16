"""
Reducción de dimensionalidad sobre el dataset limpio: chi-cuadrada con V de
Cramér entre variables categóricas, correlación de Spearman entre numéricas y
PCA sobre los indicadores de contexto. Con los resultados se arma la tabla de
decisión, con una fila por cada variable del dataset.

Los criterios (alfa, V mínima, umbral de redundancia y varianza objetivo del
PCA) están en config.yaml.

Fuga de información: el PCA y la estandarización se ajustan aquí con todas
las entidades porque E2 no entrena modelos. `ajustar_pca` recibe la tabla con
la que se ajusta, así que en el modelado se le pasará solo la partición de
entrenamiento y las demás se transformarán con el mismo objeto.

Este script se ejecuta desde pipeline/ con:
    python -m src.reduction.reduccion
"""

import sys
from itertools import combinations
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import polars as pl
from matplotlib.colors import LinearSegmentedColormap
from scipy import stats
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from config.parametros import ALFA, CRAMER_V_MIN, PCA_VARIANZA_OBJETIVO, RHO_REDUNDANCIA  # noqa: E402
from config.rutas import (  # noqa: E402
    RUTA_LIMPIO_CANCIONES,
    RUTA_LIMPIO_CONSUMO,
    RUTA_LIMPIO_CONTEXTO,
    RUTA_LIMPIO_VIOLENCIA,
    RUTA_TABLAS_REDUCCION,
)
from src.eda.estilo import COLORES, TINTA_SECUNDARIA, aplicar_estilo, estilo_serie, guardar  # noqa: E402
from src.eda.vistas import POR_CADA  # noqa: E402
from src.registro import seccion  # noqa: E402

# Frecuencia esperada mínima por celda para confiar en la aproximación de chi-cuadrada.
ESPERADA_MINIMA = 5

# Tipos de violencia del panel y nombre corto de su tasa.
TASAS = {
    "feminicida": "tasa_feminicidio",
    "sexual": "tasa_sexual",
    "física": "tasa_fisica",
    "sin distinguir": "tasa_familiar_genero",
    "control": "tasa_homicidio",
    "desaparición": "tasa_desaparicion",
}

# Variables numéricas del panel entidad y año, en orden de cercanía a las
# preguntas de E0: ante un par redundante se conserva la que aparece primero.
NUMERICAS_PANEL = [
    "tasa_feminicidio", "tasa_victimas_feminicidio", "tasa_sexual", "tasa_fisica", "tasa_victimas_fisica",
    "tasa_familiar_genero", "tasa_desaparicion", "tasa_homicidio",
    "pct_usuarios_internet", "pct_pobreza", "pct_viviendas_internet", "indice_impunidad",
    "poblacion_mujeres", "poblacion_mujeres_15_mas", "poblacion_total", "poblacion_hombres",
]
NUMERICAS_CONSUMO = ["streams", "semanas_en_chart", "mejor_posicion"]

# Indicadores de contexto que entran al PCA (la impunidad no entra: 9 entidades no tienen dato).
VARIABLES_PCA = ["pct_usuarios_internet", "pct_viviendas_internet", "pct_pobreza", "log_poblacion"]

MAPA_DIVERGENTE = LinearSegmentedColormap.from_list("azul_rojo", [COLORES[0], "#f0efec", COLORES[7]])


def holm(p: np.ndarray) -> np.ndarray:
    """Ajusta valores p por comparaciones múltiples con el método de Holm.

    Args:
        p (np.ndarray): valores p sin ajustar.

    Returns:
        np.ndarray: valores p ajustados, en el mismo orden.
    """
    p = np.asarray(p, dtype=float)
    orden = np.argsort(p)
    ajustados = np.maximum.accumulate((len(p) - np.arange(len(p))) * p[orden])
    resultado = np.empty_like(p)
    resultado[orden] = np.minimum(ajustados, 1.0)
    return resultado


def prueba_chi2(datos: pl.DataFrame, fila: str, columna: str, peso: str | None = None) -> dict[str, float]:
    """Prueba chi-cuadrada de independencia entre dos categóricas, con V de Cramér.

    En tablas de 2×2 agrega la prueba exacta de Fisher, que sirve cuando hay
    celdas con frecuencia esperada menor a 5.

    Args:
        datos (pl.DataFrame): una fila por observación, o por combinación si se da `peso`.
        fila (str): primera variable.
        columna (str): segunda variable.
        peso (str | None): columna con el conteo de cada combinación; None
            cuenta una vez cada fila.

    Returns:
        dict[str, float]: n, dimensiones de la tabla, chi2, grados de
            libertad, p, V de Cramér, frecuencia esperada mínima, porcentaje de
            celdas con esperada menor a 5 y p de Fisher (nulo si no es 2×2).
    """
    conteo = pl.col(peso).sum() if peso else pl.len()
    tabla = (
        datos.drop_nulls([fila, columna])
        .group_by(fila, columna)
        .agg(conteo.alias("n"))
        .pivot(on=columna, index=fila, values="n")
        .fill_null(0)
        .sort(fila)
    )
    observada = tabla.drop(fila).to_numpy().astype(float)
    chi2, p, gl, esperada = stats.chi2_contingency(observada, correction=False)
    n = observada.sum()
    r, c = observada.shape
    return {
        "n": n,
        "filas": r,
        "columnas": c,
        "chi2": chi2,
        "gl": gl,
        "p": p,
        "v_cramer": np.sqrt(chi2 / (n * (min(r, c) - 1))),
        "esperada_minima": esperada.min(),
        "pct_celdas_esperada_menor_5": 100 * (esperada < ESPERADA_MINIMA).mean(),
        "p_fisher": stats.fisher_exact(observada).pvalue if (r, c) == (2, 2) else None,
    }


def pruebas_categoricas() -> pl.DataFrame:
    """Corre las pruebas chi-cuadrada del proyecto y ajusta los p por Holm.

    Las categorías raras se agrupan antes de la prueba para acercarse a las
    frecuencias esperadas mínimas; el agrupamiento se describe en `prueba`.

    Returns:
        pl.DataFrame: una fila por prueba, con los resultados de
            `prueba_chi2`, el p ajustado y si la asociación cuenta.
    """
    canciones = pl.read_parquet(RUTA_LIMPIO_CANCIONES).with_columns(
        pl.when(pl.col("macrogenero").is_in(["regional mexicano", "urbano latino"]))
        .then(pl.col("macrogenero")).otherwise(pl.lit("otros")).alias("macrogenero_agrupado"),
        pl.when(pl.col("macrogenero") == "urbano latino").then(pl.lit("urbano latino"))
        .otherwise(pl.lit("resto")).alias("es_urbano"),
        pl.when(pl.col("pais_artista").is_null()).then(None)
        .when(pl.col("pais_artista") == "MX").then(pl.lit("México")).otherwise(pl.lit("otro país")).alias("pais_agrupado"),
    )
    etiquetadas = canciones.drop_nulls("es_misogina")
    violencia = (
        pl.read_parquet(RUTA_LIMPIO_VIOLENCIA)
        .filter((pl.col("origen") == "SESNSP") & (pl.col("tipo_lgamvlv") != "control"))
        .with_columns(pl.col("fecha").dt.year().alias("anio"))
    )
    ultimo_anio = violencia.filter(pl.col("tipo_lgamvlv") == "feminicida").drop_nulls("carpetas")["anio"].max()

    pruebas = [
        ("es_misogina × urbano latino o resto", "canciones etiquetadas", etiquetadas, "es_misogina", "es_urbano", None),
        ("es_misogina × macrogénero (regional, urbano, otros)", "canciones etiquetadas", etiquetadas,
         "es_misogina", "macrogenero_agrupado", None),
        ("es_misogina × país del artista (México u otro)", "canciones etiquetadas", etiquetadas,
         "es_misogina", "pais_agrupado", None),
        ("macrogénero × país del artista", "canciones del chart", canciones, "macrogenero_agrupado", "pais_agrupado", None),
        ("tipo de violencia × año", "carpetas SESNSP", violencia, "tipo_lgamvlv", "anio", "carpetas"),
        (f"tipo de violencia × entidad ({ultimo_anio})", "carpetas SESNSP",
         violencia.filter(pl.col("anio") == ultimo_anio), "tipo_lgamvlv", "cve_ent", "carpetas"),
    ]
    filas = [
        {"prueba": nombre, "unidad": unidad, "variable_1": a, "variable_2": b, **prueba_chi2(df, a, b, peso)}
        for nombre, unidad, df, a, b, peso in pruebas
    ]
    resultado = pl.DataFrame(filas).with_columns(pl.Series("p_holm", holm([f["p"] for f in filas])))
    return resultado.with_columns(
        ((pl.col("p_holm") < ALFA) & (pl.col("v_cramer") >= CRAMER_V_MIN)).alias("asociacion_relevante"),
        (pl.col("pct_celdas_esperada_menor_5") > 20).alias("chi2_poco_confiable"),
    )


def panel_entidad_anio() -> pl.DataFrame:
    """Arma el panel entidad y año con las tasas de violencia y el contexto.

    Una tasa anual queda nula si a la entidad le falta algún mes de ese año
    (por ejemplo el feminicidio antes de que se registrara, o el RNPDNO
    después de agosto de 2023).

    Returns:
        pl.DataFrame: una fila por entidad y año, con las tasas por 100 mil
            mujeres (el homicidio, por 100 mil habitantes) y los indicadores de contexto.
    """
    contexto = pl.read_parquet(RUTA_LIMPIO_CONTEXTO).with_columns(pl.col("anio").cast(pl.Int32))
    violencia = pl.read_parquet(RUTA_LIMPIO_VIOLENCIA).with_columns(pl.col("fecha").dt.year().cast(pl.Int32).alias("anio"))
    completo = pl.col("fecha").n_unique() == 12

    def anual(valor: str) -> pl.Expr:
        return pl.when(completo & (pl.col(valor).null_count() == 0)).then(pl.col(valor).sum()).otherwise(None)

    anuales = (
        violencia.group_by("cve_ent", "anio", "tipo_lgamvlv")
        .agg(
            pl.when(pl.col("tipo_lgamvlv").first() == "desaparición")
            .then(anual("victimas_mujeres")).otherwise(anual("carpetas")).alias("conteo"),
            anual("victimas_mujeres").alias("victimas"),
        )
        .join(contexto.select("cve_ent", "anio", "poblacion_mujeres", "poblacion_total"), on=["cve_ent", "anio"])
        .with_columns(
            pl.when(pl.col("tipo_lgamvlv") == "control").then(pl.col("poblacion_total"))
            .otherwise(pl.col("poblacion_mujeres")).alias("base")
        )
        .with_columns(
            (pl.col("conteo") / pl.col("base") * POR_CADA).alias("tasa"),
            (pl.col("victimas") / pl.col("poblacion_mujeres") * POR_CADA).alias("tasa_victimas"),
            pl.col("tipo_lgamvlv").replace_strict(TASAS).alias("variable"),
        )
    )
    tasas = anuales.pivot(on="variable", index=["cve_ent", "anio"], values="tasa")
    victimas = (
        anuales.filter(pl.col("tipo_lgamvlv").is_in(["feminicida", "física"]))
        .with_columns(pl.col("variable").str.replace("tasa_", "tasa_victimas_"))
        .pivot(on="variable", index=["cve_ent", "anio"], values="tasa_victimas")
    )
    return contexto.join(tasas, on=["cve_ent", "anio"], how="left").join(victimas, on=["cve_ent", "anio"], how="left")


def correlaciones(datos: pl.DataFrame, columnas: list[str]) -> pl.DataFrame:
    """Calcula la correlación de Spearman de cada par de variables.

    Cada par usa las filas donde ambas variables tienen dato.

    Args:
        datos (pl.DataFrame): tabla con las variables numéricas.
        columnas (list[str]): variables a correlacionar.

    Returns:
        pl.DataFrame: una fila por par, con `variable_1`, `variable_2`, `n` y `rho`.
    """
    filas = []
    for a, b in combinations(columnas, 2):
        par = datos.select(a, b).drop_nulls()
        rho = stats.spearmanr(par[a], par[b]).statistic if par.height > 2 else np.nan
        filas.append({"variable_1": a, "variable_2": b, "n": par.height, "rho": float(rho)})
    return pl.DataFrame(filas)


def redundantes(pares: pl.DataFrame, columnas: list[str]) -> dict[str, str]:
    """Decide qué variable sobra en cada par con |rho| mayor o igual al umbral.

    Se conserva la variable que aparece primero en `columnas` (la más cercana
    a las preguntas de E0); una variable ya descartada no descarta a otras.

    Args:
        pares (pl.DataFrame): resultado de `correlaciones`.
        columnas (list[str]): variables en orden de prioridad.

    Returns:
        dict[str, str]: variable descartada y la variable con la que es redundante.
    """
    prioridad = {c: i for i, c in enumerate(columnas)}
    descartadas: dict[str, str] = {}
    altos = pares.filter(pl.col("rho").abs() >= RHO_REDUNDANCIA).sort(pl.col("rho").abs(), descending=True)
    for a, b in altos.select("variable_1", "variable_2").iter_rows():
        conserva, descarta = sorted([a, b], key=prioridad.get)
        if conserva not in descartadas and descarta not in descartadas:
            descartadas[descarta] = conserva
    return descartadas


def ajustar_pca(datos: pl.DataFrame, columnas: list[str]) -> tuple[StandardScaler, PCA]:
    """Estandariza las variables y ajusta un PCA con todas las componentes.

    Args:
        datos (pl.DataFrame): tabla con la que se ajusta; en el modelado debe
            ser solo la partición de entrenamiento.
        columnas (list[str]): variables numéricas sin nulos.

    Returns:
        tuple[StandardScaler, PCA]: el estandarizador y el PCA ajustados.
    """
    x = datos.select(columnas).to_numpy()
    escalador = StandardScaler().fit(x)
    return escalador, PCA().fit(escalador.transform(x))


def pca_contexto(panel: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, PCA]:
    """Aplica PCA a los indicadores de contexto, con el promedio de cada entidad.

    Se usa una fila por entidad (promedio de la ventana) para no contar 11
    veces a cada estado como si fueran observaciones independientes.

    Args:
        panel (pl.DataFrame): resultado de `panel_entidad_anio`.

    Returns:
        tuple[pl.DataFrame, pl.DataFrame, PCA]: varianza explicada por
            componente, cargas de cada variable y el PCA ajustado.
    """
    entidades = (
        panel.with_columns(pl.col("poblacion_total").log10().alias("log_poblacion"))
        .group_by("cve_ent").agg(pl.col(VARIABLES_PCA).mean())
    )
    _, pca = ajustar_pca(entidades, VARIABLES_PCA)
    componentes = [f"PC{i + 1}" for i in range(pca.n_components_)]
    varianza = pl.DataFrame(
        {
            "componente": componentes,
            "valor_propio": pca.explained_variance_,
            "varianza_explicada": pca.explained_variance_ratio_,
            "varianza_acumulada": np.cumsum(pca.explained_variance_ratio_),
        }
    )
    cargas = pl.DataFrame(pca.components_.T, schema=componentes, orient="row").insert_column(0, pl.Series("variable", VARIABLES_PCA))
    return varianza, cargas, pca


def figura_correlacion(pares: pl.DataFrame, columnas: list[str]) -> None:
    """Dibuja la matriz de correlaciones de Spearman del panel entidad y año.

    Args:
        pares (pl.DataFrame): resultado de `correlaciones`.
        columnas (list[str]): variables en el orden en que se muestran.
    """
    k = len(columnas)
    matriz = np.eye(k)
    posicion = {c: i for i, c in enumerate(columnas)}
    for a, b, rho in pares.select("variable_1", "variable_2", "rho").iter_rows():
        matriz[posicion[a], posicion[b]] = matriz[posicion[b], posicion[a]] = rho
    fig, ax = plt.subplots(figsize=(10, 8.5))
    imagen = ax.imshow(np.where(np.tril(np.ones((k, k)), -1) == 1, matriz, np.nan), cmap=MAPA_DIVERGENTE, vmin=-1, vmax=1)
    for i in range(k):
        for j in range(i):
            if abs(matriz[i, j]) >= RHO_REDUNDANCIA:
                ax.text(j, i, f"{matriz[i, j]:.2f}", ha="center", va="center", fontsize=7, color="white")
    ax.set_xticks(range(k), columnas, rotation=60, ha="right", fontsize=8)
    ax.set_yticks(range(k), columnas, fontsize=8)
    ax.grid(False)
    fig.colorbar(imagen, ax=ax, shrink=0.7, label="ρ de Spearman")
    ax.set_title(f"Las cuatro poblaciones, las dos medidas de feminicidio y vivienda con internet frente a "
                 f"pobreza son redundantes\n(se muestra el valor cuando |ρ| ≥ {RHO_REDUNDANCIA})", fontsize=11)
    guardar(fig, "reduccion_correlacion")


def figura_pca(varianza: pl.DataFrame, cargas: pl.DataFrame) -> None:
    """Dibuja el gráfico de sedimentación y las cargas de las dos primeras componentes.

    Args:
        varianza (pl.DataFrame): varianza explicada por componente.
        cargas (pl.DataFrame): cargas de cada variable.
    """
    fig, (izq, der) = plt.subplots(1, 2, figsize=(11, 4.2))
    x = np.arange(1, varianza.height + 1)
    izq.bar(x, varianza["varianza_explicada"], color=COLORES[0], width=0.6, label="por componente")
    izq.plot(x, varianza["varianza_acumulada"], **estilo_serie(1), label="acumulada")
    izq.axhline(PCA_VARIANZA_OBJETIVO, color=TINTA_SECUNDARIA, linestyle=":", linewidth=1)
    izq.text(x[-1] + 0.3, PCA_VARIANZA_OBJETIVO - 0.05, f"objetivo {PCA_VARIANZA_OBJETIVO:.0%}", ha="right", fontsize=8,
             color=TINTA_SECUNDARIA)
    izq.set_xticks(x, varianza["componente"])
    izq.set_ylim(0, 1.05)
    izq.set_ylabel("Proporción de la varianza")
    izq.legend(loc="center right")
    izq.set_title("Sedimentación", fontsize=10)

    y = np.arange(cargas.height)
    der.barh(y - 0.18, cargas["PC1"], height=0.34, color=COLORES[0], label="PC1")
    der.barh(y + 0.18, cargas["PC2"], height=0.34, color=COLORES[1], hatch="//", edgecolor="white", label="PC2")
    der.axvline(0, color=TINTA_SECUNDARIA, linewidth=0.8)
    der.set_yticks(y, cargas["variable"])
    der.invert_yaxis()
    der.set_xlabel("Carga")
    der.legend(loc="upper right")
    der.set_title("Cargas", fontsize=10)

    pc1 = varianza["varianza_explicada"][0]
    fig.suptitle(f"PC1 resume {pc1:.0%} del contexto (más internet, menos pobreza) y PC2 es casi solo la población",
                 x=0.01, ha="left",
                 fontsize=12, fontweight="bold")
    guardar(fig, "reduccion_pca")


def tabla_decision(
    pruebas: pl.DataFrame,
    descartadas: dict[str, str],
    pares: pl.DataFrame,
    varianza: pl.DataFrame,
) -> pl.DataFrame:
    """Arma la tabla de decisión con una fila por variable del dataset limpio.

    Args:
        pruebas (pl.DataFrame): resultado de `pruebas_categoricas`.
        descartadas (dict[str, str]): resultado de `redundantes` para el panel y el consumo.
        pares (pl.DataFrame): correlaciones del panel y del consumo juntas.
        varianza (pl.DataFrame): varianza explicada del PCA.

    Returns:
        pl.DataFrame: columnas variable, tabla, tipo, tecnica, evidencia,
            decision y justificacion.
    """
    def rho(a: str, b: str) -> str:
        fila = pares.filter(((pl.col("variable_1") == a) & (pl.col("variable_2") == b))
                            | ((pl.col("variable_1") == b) & (pl.col("variable_2") == a)))
        return f"ρ({a}, {b}) = {fila['rho'][0]:.2f}"

    def chi(nombre: str) -> str:
        f = pruebas.filter(pl.col("prueba").str.starts_with(nombre)).row(0, named=True)
        def valor_p(p: float) -> str:
            return "< 0.001" if p < 0.001 else f"= {p:.3f}"
        fisher = f", p Fisher {valor_p(f['p_fisher'])}" if f["p_fisher"] is not None else ""
        p_holm = valor_p(f["p_holm"])
        return f"V = {f['v_cramer']:.2f}, p Holm {p_holm}{fisher}, n = {f['n']:,.0f}"

    pc1 = varianza["varianza_explicada"][0]
    pc12 = varianza["varianza_acumulada"][1]
    canciones = pl.read_parquet(RUTA_LIMPIO_CANCIONES)
    en_corpus = canciones["en_corpus_misongyny"].sum()
    con_tarea_2 = canciones["etiqueta_tarea_2"].is_not_null().sum()
    llave = ("ninguna (llave)", "", "conservar", "identifica el grano de E1")
    filas = {
        # Violencia
        "cve_ent": ("violencia, contexto", "categórica nominal", *llave),
        "fecha": ("violencia, consumo", "fecha", *llave),
        "origen": ("violencia", "categórica nominal", "ninguna", "SESNSP o RNPDNO", "conservar",
                   "separa la fuente principal de la de contraste"),
        "bien_juridico_afectado": ("violencia", "categórica nominal", "jerarquía", "determinado por subtipo",
                                   "combinar en dim_tipo_violencia", "es un nivel superior de la misma jerarquía de delito"),
        "tipo_de_delito": ("violencia", "categórica nominal", "jerarquía", "determinado por subtipo",
                           "combinar en dim_tipo_violencia", "es un nivel superior de la misma jerarquía de delito"),
        "subtipo_de_delito": ("violencia", "categórica nominal", "jerarquía", "define tipo_lgamvlv",
                              "combinar en dim_tipo_violencia", "se conserva como atributo para auditar el mapeo"),
        "modalidad": ("violencia", "categórica nominal", "jerarquía", "parte de la llave del delito",
                      "combinar en dim_tipo_violencia", "P4 no la usa, pero sin ella se duplicarían carpetas"),
        "tipo_lgamvlv": ("violencia", "categórica nominal", "χ²", chi("tipo de violencia × entidad"), "conservar",
                         "es la variable de P4; la mezcla de tipos cambia entre estados"),
        "carpetas": ("violencia", "numérica (conteo)", "correlación", rho("tasa_sexual", "tasa_familiar_genero"),
                     "conservar y transformar a tasa", "medida principal de P2; las tasas de cada tipo no son redundantes entre sí"),
        "victimas_mujeres": ("violencia", "numérica (conteo)", "correlación",
                             rho("tasa_feminicidio", "tasa_victimas_feminicidio"), "conservar en el hecho, no en el análisis",
                             "donde existe es redundante con carpetas; se queda porque el RNPDNO solo tiene víctimas"),
        "victimas_mujeres_menores": ("violencia", "numérica (conteo)", "ninguna", "ver registro de limpieza (edad no especificada)",
                                     "conservar", "P2 la pide; se aclara que es un mínimo"),
        "victimas_hombres": ("violencia", "numérica (conteo)", "ninguna", "solo para comparar", "conservar",
                             "referencia de E0 para ver si la violencia es específica contra mujeres"),
        # Consumo
        "track_id": ("consumo, canciones", "identificador", *llave),
        "streams": ("consumo", "numérica", "correlación", rho("streams", "semanas_en_chart"), "conservar y usar log10",
                    "medida principal de P1 y P3"),
        "semanas_en_chart": ("consumo", "numérica (conteo)", "correlación", rho("streams", "semanas_en_chart"),
                             "descartar" if "semanas_en_chart" in descartadas else "conservar",
                             "redundante con streams" if "semanas_en_chart" in descartadas
                             else "el DataMart la usa como medida alternativa de P1"),
        "mejor_posicion": ("consumo", "numérica (ordinal)", "correlación", rho("streams", "mejor_posicion"),
                           "descartar" if "mejor_posicion" in descartadas else "conservar",
                           "redundante con streams" if "mejor_posicion" in descartadas
                           else "P3 la usa; su relación con streams no llega al umbral"),
        # Canciones
        "artista": ("canciones", "categórica nominal", "ninguna", f"{canciones.height} canciones, {canciones['artista'].n_unique()} artistas", "conservar",
                    "atributo descriptivo de dim_cancion; demasiadas categorías para χ²"),
        "titulo": ("canciones", "texto", "ninguna", "único por canción", "conservar", "atributo descriptivo"),
        "pais_artista": ("canciones", "categórica nominal", "χ² + V de Cramér",
                         f"con macrogénero: {chi('macrogénero × país')}; con es_misogina: {chi('es_misogina × país')}",
                         "descartar del análisis, conservar en la dimensión",
                         "lo explica en buena parte el macrogénero (el regional mexicano es mexicano y el urbano "
                         "latino casi todo extranjero), así que su asociación con la misoginia repite la del género; "
                         f"{canciones['pais_artista'].null_count()} canciones no tienen país"),
        "macrogenero": ("canciones", "categórica nominal", "χ²", chi("es_misogina × urbano"),
                        "conservar", "es la variable de P1 que se cruza con la misoginia"),
        "origen_genero": ("canciones", "categórica nominal", "ninguna", "auditoría", "conservar",
                          "marca qué géneros vienen de MusicBrainz, de corrección o no tienen dato"),
        "etiquetas_fuente": ("canciones", "texto", "ninguna", "lista de etiquetas", "conservar",
                             "permite rehacer el macrogénero"),
        "etiqueta_tarea_1": ("canciones", "categórica nominal", "ninguna", "define es_misogina", "combinar en es_misogina",
                             "M y NM se convierten en verdadero y falso"),
        "etiqueta_tarea_2": ("canciones", "categórica nominal", "ninguna", f"{con_tarea_2} canciones", "conservar como categoria_misoginia",
                             "da el tipo de violencia que P3 y P4 comparan"),
        "en_corpus_misongyny": ("canciones", "binaria", "ninguna", f"{en_corpus} de {canciones.height}", "conservar", "mide la cobertura"),
        "es_misogina": ("canciones", "binaria", "χ²", chi("es_misogina × urbano"), "conservar",
                        "variable objetivo de P1"),
        "origen_etiqueta": ("canciones", "categórica nominal", "ninguna", "un solo valor por ahora", "conservar",
                            "en el modelado distinguirá la etiqueta humana de la del clasificador"),
    }
    poblaciones = ["poblacion_total", "poblacion_mujeres_15_mas", "poblacion_hombres"]
    for variable in poblaciones + ["poblacion_mujeres"]:
        se_va = variable in descartadas
        filas[variable] = ("contexto", "numérica", "correlación", rho("poblacion_mujeres", variable) if se_va
                           else rho("poblacion_mujeres", "poblacion_total"),
                           "descartar del análisis, conservar en la dimensión" if se_va else "conservar",
                           f"redundante con {descartadas[variable]}" if se_va
                           else "denominador de todas las tasas")
    pca = f"PC1 explica {pc1:.0%}, PC1 y PC2 {pc12:.0%}"
    for variable in ["pct_usuarios_internet", "pct_pobreza", "pct_viviendas_internet"]:
        if variable in descartadas:
            filas[variable] = ("contexto", "numérica", "correlación + PCA", f"{rho(descartadas[variable], variable)}; {pca}",
                               "descartar del análisis, conservar en la dimensión",
                               f"redundante con {descartadas[variable]}, que se mide en más años")
        else:
            filas[variable] = ("contexto", "numérica", "correlación + PCA", pca, "conservar originales",
                               "con 4 variables las componentes no ahorran casi nada y PC2 es casi solo la población; "
                               "los originales se interpretan directo")
    filas["indice_impunidad"] = ("contexto", "numérica", "correlación", "9 de 32 entidades sin dato",
                                 "conservar fuera del PCA", "el PCA no admite nulos; queda como atributo con faltante")
    filas["poblacion_mujeres"] = (*filas["poblacion_mujeres"][:4], "conservar", "denominador de todas las tasas")
    filas["poblacion_6_mas"] = ("contexto", "numérica", "ninguna", "universo de la ENDUTIH", "conservar como auxiliar",
                                "solo sirve para pasar los usuarios de internet a porcentaje")
    filas["usuarios_internet"] = ("contexto", "numérica", "ninguna", "ENDUTIH 2015–2025, 2019 interpolado",
                                  "conservar", "es el factor de usuarios del reparto del consumo de E1")
    for variable in ["prevalencia_total", "prevalencia_psicologica", "prevalencia_fisica", "prevalencia_sexual",
                     "prevalencia_economica_patrimonial"]:
        filas[variable] = ("contexto", "numérica (%)", "ninguna", "solo 2016 y 2021 (ENDIREH)", "conservar",
                           "es la referencia oficial de P4 por tipo de violencia; con dos levantamientos no entra "
                           "al panel anual ni al PCA")

    return pl.DataFrame(
        [{"variable": v, "tabla": t[0], "tipo": t[1], "tecnica": t[2], "evidencia": t[3], "decision": t[4],
          "justificacion": t[5]} for v, t in filas.items()]
    )


def ejecutar_reduccion() -> pl.DataFrame:
    """Corre las tres técnicas, dibuja las figuras y guarda la tabla de decisión.

    Returns:
        pl.DataFrame: la tabla de decisión por variable.
    """
    aplicar_estilo()
    RUTA_TABLAS_REDUCCION.mkdir(parents=True, exist_ok=True)

    seccion("Chi-cuadrada y V de Cramér")
    pruebas = pruebas_categoricas()
    print(pruebas.select("prueba", "n", "v_cramer", "p_holm", "p_fisher", "esperada_minima", "asociacion_relevante"))

    seccion("Correlación de Spearman")
    panel = panel_entidad_anio()
    pares_panel = correlaciones(panel, NUMERICAS_PANEL)
    consumo = pl.read_parquet(RUTA_LIMPIO_CONSUMO)
    pares_consumo = correlaciones(consumo, NUMERICAS_CONSUMO)
    pares = pl.concat([pares_panel, pares_consumo])
    descartadas = redundantes(pares_panel, NUMERICAS_PANEL) | redundantes(pares_consumo, NUMERICAS_CONSUMO)
    print(pares.filter(pl.col("rho").abs() >= RHO_REDUNDANCIA).sort(pl.col("rho").abs(), descending=True))
    print("Redundantes:", descartadas)
    figura_correlacion(pares_panel, NUMERICAS_PANEL)

    seccion("PCA del contexto")
    varianza, cargas, _ = pca_contexto(panel)
    print(varianza)
    print(cargas)
    figura_pca(varianza, cargas)

    decision = tabla_decision(pruebas, descartadas, pares, varianza)
    for nombre, tabla in {"pruebas_chi2": pruebas, "correlaciones": pares, "pca_varianza": varianza,
                          "pca_cargas": cargas, "panel_entidad_anio": panel, "tabla_decision": decision}.items():
        tabla.write_parquet(RUTA_TABLAS_REDUCCION / f"{nombre}.parquet")

    seccion("Tabla de decisión")
    with pl.Config(tbl_rows=-1, fmt_str_lengths=60):
        print(decision.select("variable", "tecnica", "evidencia", "decision"))
    return decision


if __name__ == "__main__":
    ejecutar_reduccion()
