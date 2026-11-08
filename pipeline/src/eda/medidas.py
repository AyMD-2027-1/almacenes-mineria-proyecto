"""
Medidas descriptivas que usa el análisis exploratorio: localización,
variabilidad y forma de una variable numérica, concentración (Gini, curva de
Lorenz, CR_k y HHI), heterogeneidad de una variable categórica (entropía,
Gini–Simpson y números de Hill) y heterogeneidad de una numérica entre grupos
(eta y omega cuadradas).

Las fórmulas siguen los capítulos de medidas de concentración y de
heterogeneidad del curso. La entropía se calcula en bits (logaritmo base 2),
como en la definición de la entrega E2.
"""

import numpy as np
import polars as pl
from scipy import stats

# Proporción que se recorta de cada extremo para la media recortada.
RECORTE_MEDIA = 0.10

# Fracción de unidades más grandes cuya participación se reporta junto al Gini.
FRACCION_TOP = 0.10


def resumen_numerico(valores: pl.Series) -> dict[str, float]:
    """Calcula las medidas de localización, variabilidad y forma de una variable.

    Los nulos se ignoran.

    Args:
        valores (pl.Series): variable numérica.

    Returns:
        dict[str, float]: n, media, mediana, moda, media recortada al 10 %,
            percentiles 10, 25, 75 y 90, mínimo, máximo, rango, varianza,
            desviación estándar, IQR, MAD, coeficiente de variación, asimetría
            y curtosis en exceso (0 en una normal).
    """
    x = valores.drop_nulls().cast(pl.Float64).to_numpy()
    q10, q25, q50, q75, q90 = np.percentile(x, [10, 25, 50, 75, 90])
    media = x.mean()
    desviacion = x.std(ddof=1)
    return {
        "n": float(x.size),
        "media": media,
        "mediana": q50,
        "moda": float(stats.mode(x, keepdims=False).mode),
        "media_recortada_10": stats.trim_mean(x, RECORTE_MEDIA),
        "p10": q10,
        "q1": q25,
        "q3": q75,
        "p90": q90,
        "minimo": x.min(),
        "maximo": x.max(),
        "rango": x.max() - x.min(),
        "varianza": x.var(ddof=1),
        "desv_estandar": desviacion,
        "iqr": q75 - q25,
        "mad": stats.median_abs_deviation(x),
        "coef_variacion": desviacion / media if media else np.nan,
        "asimetria": stats.skew(x),
        "curtosis": stats.kurtosis(x),
    }


def validar_masa(x: np.ndarray) -> np.ndarray:
    """Revisa que un vector sirva para medir concentración.

    Args:
        x (np.ndarray): masa de cada unidad (streams, carpetas, población...).

    Returns:
        np.ndarray: el mismo vector como flotantes.

    Raises:
        ValueError: si hay menos de dos unidades, valores no finitos,
            negativos o la suma no es positiva.
    """
    x = np.asarray(x, dtype=float)
    if x.ndim != 1 or x.size < 2:
        raise ValueError("se necesitan al menos dos unidades")
    if not np.all(np.isfinite(x)):
        raise ValueError("solo se aceptan valores finitos")
    if np.any(x < 0) or x.sum() <= 0:
        raise ValueError("se requieren valores no negativos con suma positiva")
    return x


def indices_concentracion(x: np.ndarray, k: int = 10) -> dict[str, float]:
    """Calcula los índices de concentración de una masa repartida entre unidades.

    El Gini usa la fórmula de rangos con los valores en orden ascendente; el
    Gini normalizado lo divide entre su máximo posible con n unidades.

    Args:
        x (np.ndarray): masa no negativa de cada unidad; los ceros cuentan
            como unidades.
        k (int): número de unidades más grandes para CR_k.

    Returns:
        dict[str, float]: unidades, Gini, Gini normalizado, participación del
            10 % de unidades más grandes, CR_k, HHI y unidades efectivas (1/HHI).
    """
    x = validar_masa(x)
    n = x.size
    total = x.sum()
    x_asc = np.sort(x)
    participaciones_desc = x_asc[::-1] / total
    gini = 2 * np.sum(np.arange(1, n + 1) * x_asc) / (n * total) - (n + 1) / n
    hhi = np.square(participaciones_desc).sum()
    top = max(1, int(np.ceil(n * FRACCION_TOP)))
    return {
        "unidades": float(n),
        "gini": gini,
        "gini_normalizado": n * gini / (n - 1),
        "participacion_top_10pct": participaciones_desc[:top].sum(),
        f"cr_{k}": participaciones_desc[: min(k, n)].sum(),
        "hhi": hhi,
        "unidades_efectivas": 1 / hhi,
    }


def curva_lorenz(x: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Calcula los puntos de la curva de Lorenz, acumulando desde la unidad menor.

    Args:
        x (np.ndarray): masa no negativa de cada unidad.

    Returns:
        tuple[np.ndarray, np.ndarray]: fracción acumulada de unidades y
            fracción acumulada de la masa, ambas empezando en 0.
    """
    x_asc = np.sort(validar_masa(x))
    unidades = np.arange(x_asc.size + 1) / x_asc.size
    masa = np.r_[0, np.cumsum(x_asc) / x_asc.sum()]
    return unidades, masa


def indices_categoricos(conteos: np.ndarray) -> dict[str, float]:
    """Calcula la heterogeneidad de una variable categórica a partir de sus conteos.

    Las categorías con conteo cero no cuentan para K ni para la normalización.

    Args:
        conteos (np.ndarray): frecuencia (o masa) de cada categoría.

    Returns:
        dict[str, float]: K observado, proporción de la moda, Gini–Simpson y su
            versión normalizada, entropía en bits y normalizada, y números de
            Hill de orden 1 y 2 (categorías efectivas).

    Raises:
        ValueError: si hay conteos negativos o todos son cero.
    """
    conteos = np.asarray(conteos, dtype=float)
    if np.any(conteos < 0) or conteos.sum() == 0:
        raise ValueError("los conteos deben ser no negativos y sumar más de cero")
    p = conteos / conteos.sum()
    p = p[p > 0]
    k = p.size
    entropia = -(p * np.log2(p)).sum()
    gini_simpson = 1 - np.square(p).sum()
    return {
        "k": float(k),
        "proporcion_moda": p.max(),
        "gini_simpson": gini_simpson,
        "gini_simpson_normalizado": gini_simpson / (1 - 1 / k) if k > 1 else 0.0,
        "entropia_bits": entropia,
        "entropia_normalizada": entropia / np.log2(k) if k > 1 else 0.0,
        "hill_1": 2**entropia,
        "hill_2": 1 / np.square(p).sum(),
    }


def descomposicion_anova(valores: pl.Series, grupos: pl.Series) -> dict[str, float]:
    """Mide qué parte de la variación de una numérica se explica por los grupos.

    Args:
        valores (pl.Series): variable numérica.
        grupos (pl.Series): grupo de cada observación, de la misma longitud.

    Returns:
        dict[str, float]: suma de cuadrados total, entre grupos y dentro de
            grupos, eta cuadrada y omega cuadrada.
    """
    datos = pl.DataFrame({"y": valores.cast(pl.Float64), "grupo": grupos}).drop_nulls()
    datos = datos.with_columns(pl.col("y").mean().over("grupo").alias("media_grupo"))
    media_global = datos["y"].mean()
    ss_t = ((datos["y"] - media_global) ** 2).sum()
    ss_w = ((datos["y"] - datos["media_grupo"]) ** 2).sum()
    ss_b = ss_t - ss_w
    n, g = datos.height, datos["grupo"].n_unique()
    ms_w = ss_w / (n - g)
    return {
        "ss_total": ss_t,
        "ss_entre": ss_b,
        "ss_dentro": ss_w,
        "eta2": ss_b / ss_t if ss_t > 0 else np.nan,
        "omega2": (ss_b - (g - 1) * ms_w) / (ss_t + ms_w) if ss_t + ms_w > 0 else np.nan,
    }
