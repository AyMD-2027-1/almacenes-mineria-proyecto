"""
Extrae del tabulado de la ENDIREH 2021 (INEGI, cuadro 21.2) la prevalencia de
la violencia contra las mujeres de 15 años y más en los últimos 12 meses, por
entidad, tipo de persona agresora y tipo de violencia.

El cuadro viene en Excel con formato de presentación: cuatro bloques
(estimación, coeficiente de variación, error estándar y observaciones
muestrales), el nombre de la entidad en una fila y debajo una fila por tipo
de persona agresora, y columnas para los levantamientos 2021 y 2016. Aquí
solo se pasa a formato largo, sin cambiar ningún valor; el resultado es el
CSV que se sube a staging.

Este script se ejecuta desde pipeline/ con:
    python -m src.ingestion.endireh
"""

import sys
from pathlib import Path

import polars as pl

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from config.parametros import ARCHIVOS_STAGING, ENDIREH  # noqa: E402
from config.rutas import RUTA_DESCARGAS  # noqa: E402
from src.registro import seccion  # noqa: E402

# Bloques del cuadro, en el orden en que aparecen, con el nombre corto de la medida.
BLOQUES = {
    "Estimaciones puntuales": "estimacion",
    "Coeficientes de variación (%)": "coeficiente_variacion",
    "Errores estándar": "error_estandar",
    "Observaciones muestrales": "observaciones",
}

# Filas de persona agresora debajo de cada entidad (el 3 es la llamada a nota al pie).
AGRESORES = {"Total": "total", "Pareja": "pareja", "Otros agresores3": "otros"}

# Columna del Excel de cada levantamiento y tipo de violencia.
COLUMNAS_VALOR = {
    ("2021", "total"): "column_1",
    ("2021", "psicologica"): "column_3",
    ("2021", "fisica"): "column_4",
    ("2021", "sexual"): "column_5",
    ("2021", "economica_patrimonial"): "column_6",
    ("2016", "total"): "column_8",
    ("2016", "psicologica"): "column_10",
    ("2016", "fisica"): "column_11",
    ("2016", "sexual"): "column_12",
    ("2016", "economica_patrimonial"): "column_13",
}

ENTIDADES = 33


def leer_cuadro() -> pl.DataFrame:
    """Lee la hoja del cuadro tal como viene, todo como texto.

    Returns:
        pl.DataFrame: las celdas de la hoja con columnas `column_0`, `column_1`...
    """
    return pl.read_excel(RUTA_DESCARGAS / ENDIREH["archivo"], sheet_name=ENDIREH["hoja"],
                         has_header=False, infer_schema_length=0)


def a_formato_largo(cuadro: pl.DataFrame) -> pl.DataFrame:
    """Recorre el cuadro y arma una fila por entidad, agresor, medida, levantamiento y tipo.

    Una fila con el nombre de un bloque abre ese bloque; una fila con texto en
    la primera columna y sin valores es una entidad; las filas de agresor
    llevan los valores.

    Args:
        cuadro (pl.DataFrame): resultado de `leer_cuadro`.

    Returns:
        pl.DataFrame: columnas `entidad`, `persona_agresora`, `medida`,
            `levantamiento`, `tipo_violencia` y `valor`, todas como texto.
    """
    filas = []
    bloque = entidad = None
    vistos = 0
    for fila in cuadro.iter_rows(named=True):
        texto = (fila["column_0"] or "").strip()
        if texto in BLOQUES:
            vistos += 1
            # Las cuatro primeras menciones son el índice de la portada.
            bloque = BLOQUES[texto] if vistos > len(BLOQUES) else None
            continue
        if bloque is None or not texto:
            continue
        if texto in AGRESORES and entidad:
            for (levantamiento, tipo), columna in COLUMNAS_VALOR.items():
                filas.append({"entidad": entidad, "persona_agresora": AGRESORES[texto], "medida": bloque,
                              "levantamiento": levantamiento, "tipo_violencia": tipo, "valor": fila[columna]})
        elif fila["column_1"] is None and not texto.startswith(("Entidad", "INEGI", "Fuente")) and not texto[0].isdigit():
            entidad = texto
    return pl.DataFrame(filas)


def ejecutar_endireh() -> pl.DataFrame:
    """Convierte el cuadro a CSV en formato largo y revisa que esté completo.

    Returns:
        pl.DataFrame: la tabla que se sube a staging.
    """
    seccion("ENDIREH 2021, prevalencia en los últimos 12 meses")
    tabla = a_formato_largo(leer_cuadro())
    esperadas = ENTIDADES * len(AGRESORES) * len(BLOQUES) * len(COLUMNAS_VALOR)
    assert tabla["entidad"].n_unique() == ENTIDADES, f"se esperaban {ENTIDADES} entidades: {tabla['entidad'].unique().to_list()}"
    assert tabla.height == esperadas, f"{tabla.height} filas, se esperaban {esperadas}"
    assert tabla.filter(pl.col("medida") == "estimacion")["valor"].null_count() == 0, "hay estimaciones vacías"

    destino = RUTA_DESCARGAS / ARCHIVOS_STAGING["stg_endireh_2021_prevalencias"]
    destino.parent.mkdir(parents=True, exist_ok=True)
    tabla.write_csv(destino)
    nacional = tabla.filter((pl.col("entidad") == "Estados Unidos Mexicanos") & (pl.col("persona_agresora") == "total")
                            & (pl.col("medida") == "estimacion") & (pl.col("levantamiento") == "2021"))
    print(nacional.select("tipo_violencia", "valor"))
    print(f"{destino.name}: {tabla.height:,} filas")
    return tabla


if __name__ == "__main__":
    ejecutar_endireh()
