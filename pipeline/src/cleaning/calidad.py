"""
Herramientas de diagnóstico de calidad que usa la limpieza: detección de
atípicos con la regla del IQR y con el puntaje z robusto, ceros iniciales de
una serie y el registro de limpieza.

El registro de limpieza guarda una fila por problema tratado: en qué tabla y
variable, cuántas filas o celdas afecta, qué se decidió y por qué. Se escribe
completo en cada corrida, así que siempre corresponde a la última limpieza.
"""

import sys
from pathlib import Path

import polars as pl

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from config.rutas import RUTA_REGISTRO_LIMPIEZA  # noqa: E402

# Constante que vuelve a la MAD comparable con la desviación estándar en una normal.
CONSTANTE_MAD = 0.6745

COLUMNAS_REGISTRO = {
    "tabla": pl.String,
    "variable": pl.String,
    "problema": pl.String,
    "afectadas": pl.Int64,
    "total": pl.Int64,
    "decision": pl.String,
    "justificacion": pl.String,
}


def anotar(
    registro: list[dict],
    tabla: str,
    variable: str,
    problema: str,
    afectadas: int,
    total: int,
    decision: str,
    justificacion: str,
) -> None:
    """Agrega un problema tratado al registro de limpieza y lo muestra en consola.

    Args:
        registro (list[dict]): lista donde se acumulan las entradas.
        tabla (str): tabla del dataset analítico o fuente de staging.
        variable (str): columna afectada, o "fila" si el problema es de la fila completa.
        problema (str): qué se encontró.
        afectadas (int): filas o celdas con el problema.
        total (int): filas o celdas revisadas, para poder dar el porcentaje.
        decision (str): qué se hizo (eliminar, imputar, conservar, marcar...).
        justificacion (str): por qué, en una o dos frases.
    """
    registro.append(
        {
            "tabla": tabla,
            "variable": variable,
            "problema": problema,
            "afectadas": int(afectadas),
            "total": int(total),
            "decision": decision,
            "justificacion": justificacion,
        }
    )
    porcentaje = 100 * afectadas / total if total else 0.0
    print(f"[{tabla}.{variable}] {problema}: {afectadas:,} de {total:,} ({porcentaje:.1f} %). Decisión: {decision}")


def guardar_registro(registro: list[dict]) -> pl.DataFrame:
    """Escribe el registro de limpieza en logs/registro_limpieza.csv.

    Args:
        registro (list[dict]): entradas acumuladas con `anotar`.

    Returns:
        pl.DataFrame: el registro con la columna `pct_afectadas` agregada.
    """
    tabla = pl.DataFrame(registro, schema=COLUMNAS_REGISTRO).with_columns(
        (100 * pl.col("afectadas") / pl.col("total")).round(2).alias("pct_afectadas")
    )
    RUTA_REGISTRO_LIMPIEZA.parent.mkdir(parents=True, exist_ok=True)
    tabla.write_csv(RUTA_REGISTRO_LIMPIEZA)
    return tabla


def z_robusto(valor: pl.Expr) -> pl.Expr:
    """Calcula el puntaje z robusto con la mediana y la MAD.

    Cuando la MAD es cero (más de la mitad de los valores son iguales) el
    puntaje no está definido y queda nulo. Se puede combinar con `.over()`
    para calcularlo dentro de cada grupo.

    Args:
        valor (pl.Expr): variable numérica.

    Returns:
        pl.Expr: expresión con el puntaje de cada valor (float).
    """
    mediana = valor.median()
    mad = (valor - mediana).abs().median()
    return pl.when(mad > 0).then(CONSTANTE_MAD * (valor - mediana) / mad).otherwise(None)


def fuera_de_iqr(valor: pl.Expr, factor: float) -> pl.Expr:
    """Marca los valores fuera de las cercas de Tukey.

    Args:
        valor (pl.Expr): variable numérica.
        factor (float): múltiplo del IQR para las cercas (1.5 es el usual).

    Returns:
        pl.Expr: expresión booleana, verdadera fuera de [Q1 - f·IQR, Q3 + f·IQR].
    """
    q1 = valor.quantile(0.25)
    q3 = valor.quantile(0.75)
    iqr = q3 - q1
    return (valor < q1 - factor * iqr) | (valor > q3 + factor * iqr)


def ceros_iniciales(serie: pl.DataFrame, grupo: str, orden: str, valor: str) -> pl.DataFrame:
    """Mide cuántos periodos lleva cada grupo en cero antes de su primer valor positivo.

    Args:
        serie (pl.DataFrame): una fila por grupo y periodo.
        grupo (str): columna del grupo, por ejemplo la entidad.
        orden (str): columna del periodo, por ejemplo la fecha.
        valor (str): columna con el conteo.

    Returns:
        pl.DataFrame: una fila por grupo con `primer_positivo` (mismo tipo que
            `orden`, nulo si el grupo nunca pasa de cero) y `ceros_iniciales`
            (int, periodos anteriores a ese primer positivo).
    """
    return (
        serie.sort(grupo, orden)
        .group_by(grupo, maintain_order=True)
        .agg(
            pl.col(orden).filter(pl.col(valor) > 0).first().alias("primer_positivo"),
            (pl.col(valor).cum_sum() == 0).sum().cast(pl.Int64).alias("ceros_iniciales"),
        )
    )
