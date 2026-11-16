"""
Inspección de las tablas de staging tal como llegan de BigQuery, antes de
cualquier limpieza. Reporta tamaño, valores faltantes o reservados,
duplicados, columnas numéricas que no se pueden convertir, cobertura temporal
y nombres de entidad que no coinciden entre fuentes.

No modifica nada: lo que imprime es el diagnóstico que justifica las
decisiones de limpieza.

Este script se ejecuta desde pipeline/ con:
    python -m src.eda.inspeccion
"""

import sys
from pathlib import Path

import polars as pl

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from config.parametros import TABLAS_STAGING  # noqa: E402
from config.rutas import ruta_raw  # noqa: E402
from src.ingestion.entidades import normalizar_nombre  # noqa: E402
from src.registro import seccion  # noqa: E402

pl.Config.set_tbl_rows(40)
pl.Config.set_tbl_hide_dataframe_shape(True)
pl.Config.set_fmt_str_lengths(50)

# Fuente del catálogo de E1 a la que pertenece cada tabla.
FUENTES = {
    "stg_sesnsp_delitos": "F1 · SESNSP, carpetas por delito",
    "stg_sesnsp_victimas": "F1 · SESNSP, víctimas por delito",
    "stg_rnpdno": "F13 · RNPDNO, personas desaparecidas",
    "stg_kworb_mx_semanal": "F4 · Kworb, historial semanal de México",
    "stg_kworb_catalogo_canciones": "F4 · Kworb, catálogo de canciones",
    "stg_kaggle_charts_mx": "F5 · Kaggle, charts diarios de México",
    "stg_misongyny": "F6 · MiSonGyny, etiquetas de misoginia",
    "stg_musicbrainz_tags": "F8 · MusicBrainz, búsquedas por artista",
    "stg_trends_regueton_tema_2021": "F11 · Google Trends, tema reguetón 2021",
    "stg_conapo_poblacion": "F3 · CONAPO, población a mitad de año",
    "stg_endutih_usuarios": "F16 · ENDUTIH 2024, usuarios",
    "stg_coneval_pobreza": "F15 · CONEVAL, pobreza por entidad",
    "stg_mexico_evalua_12_indices": "F17 · México Evalúa, impunidad 2023",
    "stg_naturalearth_mx": "F18 · Natural Earth, geometrías",
    "stg_censo_iter": "F14 · Censo 2020 ITER, totales por entidad",
    "stg_trends_macrogenero_anio": "F11 · Google Trends, macrogénero por año (E2)",
    "stg_trends_artista_anio": "F11 · Google Trends, artista por año (E2)",
    "stg_trends_artista_anio_completo": "F11 · Google Trends, artistas nuevos del top 50 (E2)",
    "stg_endireh_2021_prevalencias": "F2 · ENDIREH 2021, prevalencias tabuladas (E2)",
    "stg_kworb_catalogo_completo": "F4 · Kworb, catálogo del Top 200 semanal completo (E2)",
    "stg_kworb_mx_semanal_completo": "F4 · Kworb, historial semanal completo (E2)",
    "stg_musicbrainz_tags_completo": "F8 · MusicBrainz, búsquedas de los artistas nuevos (E2)",
}

# Llave natural de cada tabla: una combinación que debería ser única. En la
# ENDUTIH la llave (vivienda, hogar y persona) no está entre las columnas que se
# leen, y en el RNPDNO se quitaron las columnas de nombre al cargar staging, así que
# no queda un identificador de persona (consecutivo_registro solo va de 1 a 8): dos
# filas iguales pueden ser dos personas distintas.
LLAVES = {
    "stg_sesnsp_delitos": ["ano", "clave_ent", "bien_juridico_afectado", "tipo_de_delito",
                           "subtipo_de_delito", "modalidad"],
    "stg_sesnsp_victimas": ["ano", "clave_ent", "bien_juridico_afectado", "tipo_de_delito",
                            "subtipo_de_delito", "modalidad", "sexo", "rango_de_edad"],
    "stg_rnpdno": None,
    "stg_kworb_mx_semanal": ["track_id", "semana"],
    "stg_kworb_catalogo_canciones": ["track_id"],
    "stg_kaggle_charts_mx": ["chart", "date", "rank"],
    "stg_misongyny": ["archivo_origen", "id"],
    "stg_musicbrainz_tags": ["artista_buscado"],
    "stg_trends_regueton_tema_2021": ["region"],
    "stg_conapo_poblacion": ["anio", "cve_geo", "edad", "sexo"],
    "stg_endutih_usuarios": None,
    "stg_coneval_pobreza": ["hoja", "fila"],
    "stg_trends_macrogenero_anio": ["macrogenero", "anio", "geo_code"],
    "stg_trends_artista_anio": ["artista", "anio", "geo_code"],
    "stg_trends_artista_anio_completo": ["artista", "anio", "geo_code"],
    "stg_endireh_2021_prevalencias": ["entidad", "persona_agresora", "medida", "levantamiento", "tipo_violencia"],
    "stg_kworb_catalogo_completo": ["track_id"],
    "stg_kworb_mx_semanal_completo": ["track_id", "semana"],
    "stg_musicbrainz_tags_completo": ["artista_buscado"],
    "stg_mexico_evalua_12_indices": ["delito", "entidad", "anio"],
    "stg_naturalearth_mx": ["iso_3166_2"],
    "stg_censo_iter": ["ENTIDAD"],
}

MESES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto",
         "septiembre", "octubre", "noviembre", "diciembre"]

# Columnas que deberían poder convertirse a número.
NUMERICAS = {
    "stg_sesnsp_delitos": ["ano", "clave_ent", *MESES],
    "stg_sesnsp_victimas": ["ano", "clave_ent", *MESES],
    "stg_rnpdno": ["edad"],
    "stg_kaggle_charts_mx": ["rank", "streams"],
    "stg_trends_regueton_tema_2021": ["valor"],
    "stg_conapo_poblacion": ["anio", "cve_geo", "edad", "poblacion"],
    "stg_endutih_usuarios": ["cve_ent", "edad", "fac_per", "p7_1"],
    "stg_coneval_pobreza": ["c04", "c05", "c06", "c07"],
    "stg_mexico_evalua_12_indices": ["anio", "indice_impunidad"],
    "stg_censo_iter": ["ENTIDAD", "POBTOT", "POBFEM", "POBMAS", "TVIVPARHAB", "VPH_INTER"],
}

# Valores de texto que no son un dato: vacíos, reservados por la fuente o sin identificar.
PATRON_MARCADOR = r"^(|--|\*|NA|N/A|SE DESCONOCE|INDETERMINADO|No identificado|No especificado|ELIMINADO \d+)$"

# Columna con el nombre o la clave de la entidad en cada fuente.
COLUMNA_ENTIDAD = {
    "stg_sesnsp_delitos": "entidad",
    "stg_sesnsp_victimas": "entidad",
    "stg_rnpdno": "entidad_de_desaparicion",
    "stg_conapo_poblacion": "entidad",
    "stg_censo_iter": "NOM_ENT",
    "stg_coneval_pobreza": "hoja",
    "stg_mexico_evalua_12_indices": "entidad",
    "stg_naturalearth_mx": "name_es",
    "stg_trends_regueton_tema_2021": "region",
}

# Las columnas de texto largo no se imprimen.
COLUMNAS_LARGAS = ["geometry_wkt", "respuesta_json"]


def cargar(tabla: str) -> pl.DataFrame:
    """Lee la copia local de una tabla de staging.

    Args:
        tabla (str): nombre de la tabla, por ejemplo "stg_rnpdno".

    Returns:
        pl.DataFrame: la tabla con todas sus columnas como texto.
    """
    return pl.read_parquet(ruta_raw(tabla))


def tamano(df: pl.DataFrame, tabla: str) -> None:
    """Imprime la fuente, las filas y las columnas de una tabla.

    Args:
        df (pl.DataFrame): tabla a describir.
        tabla (str): nombre de la tabla.
    """
    print(f"Fuente: {FUENTES.get(tabla, '—')}")
    print(f"Filas: {df.height:,}   Columnas: {df.width}")


def faltantes(df: pl.DataFrame) -> pl.DataFrame:
    """Cuenta, por columna, los nulos y los valores de texto que no son un dato.

    Args:
        df (pl.DataFrame): tabla con columnas de texto.

    Returns:
        pl.DataFrame: una fila por columna con algún problema, con el número
            de nulos, de marcadores, el porcentaje sobre el total de filas y
            cuáles marcadores aparecen. Vacío si la tabla está completa.
    """
    filas = []
    for col in df.columns:
        if col in COLUMNAS_LARGAS:
            continue
        serie = df[col]
        es_marcador = serie.str.contains(PATRON_MARCADOR).fill_null(False)
        nulos, marcadores = serie.null_count(), int(es_marcador.sum())
        if nulos + marcadores == 0:
            continue
        cuales = serie.filter(es_marcador).str.replace(r"\d+$", "N").unique().sort().to_list()
        filas.append(
            {
                "columna": col,
                "nulos": nulos,
                "marcadores": marcadores,
                "pct": round((nulos + marcadores) / df.height * 100, 1),
                "cuales": ", ".join(repr(c) for c in cuales),
            }
        )
    return pl.DataFrame(filas)


def duplicados(df: pl.DataFrame, llave: list[str]) -> tuple[int, int]:
    """Cuenta filas repetidas completas y filas que repiten la llave natural.

    Args:
        df (pl.DataFrame): tabla a revisar.
        llave (list[str]): columnas que deberían identificar cada fila.

    Returns:
        tuple[int, int]: filas duplicadas completas y filas con la llave repetida.
    """
    completas = int(df.is_duplicated().sum())
    por_llave = int(df.select(pl.struct(llave).is_duplicated()).to_series().sum())
    return completas, por_llave


def no_convertibles(df: pl.DataFrame, columnas: list[str]) -> dict[str, int]:
    """Cuenta los valores que no se pueden convertir a número en cada columna.

    No se cuentan los nulos ni los marcadores, que ya se reportan como faltantes.

    Args:
        df (pl.DataFrame): tabla con columnas de texto.
        columnas (list[str]): columnas que deberían ser numéricas.

    Returns:
        dict[str, int]: columnas con al menos un valor no convertible y cuántos tiene.
    """
    conteos = {}
    for col in columnas:
        texto = pl.col(col).str.replace_all(",", "")
        malos = df.select(
            (
                pl.col(col).is_not_null()
                & ~pl.col(col).str.contains(PATRON_MARCADOR)
                & texto.cast(pl.Float64, strict=False).is_null()
            ).sum()
        ).item()
        if malos:
            conteos[col] = malos
    return conteos


def inspeccionar_tabla(tabla: str) -> None:
    """Imprime el diagnóstico general de una tabla.

    Args:
        tabla (str): nombre de la tabla de staging.
    """
    seccion(tabla)
    df = cargar(tabla)
    tamano(df, tabla)

    llave = LLAVES.get(tabla)
    if llave is None:
        print(f"Duplicados: no se pueden determinar, no hay llave natural entre las columnas; "
              f"filas idénticas en todas las columnas: {df.is_duplicated().sum():,}")
    else:
        completas, por_llave = duplicados(df, llave)
        print(f"Filas idénticas: {completas:,}   Llave repetida ({', '.join(llave)}): {por_llave:,}")

    malos = no_convertibles(df, NUMERICAS.get(tabla, []))
    if malos:
        print(f"Valores no numéricos en columnas numéricas: {malos}")

    tabla_faltantes = faltantes(df)
    if tabla_faltantes.is_empty():
        print("Sin nulos ni marcadores.")
    else:
        print("Nulos y marcadores por columna:")
        print(tabla_faltantes)


def cobertura_temporal() -> None:
    """Imprime qué periodo cubre cada fuente con fechas y cómo se reparte en el tiempo."""
    seccion("Cobertura temporal")

    for tabla in ("stg_sesnsp_delitos", "stg_sesnsp_victimas"):
        anios = cargar(tabla)["ano"].unique().sort()
        print(f"{tabla}: {anios.first()} a {anios.last()} ({anios.len()} años)")

    kaggle = cargar("stg_kaggle_charts_mx")
    print("\nKaggle por tipo de chart (el viral50 no publica streams):")
    print(
        kaggle.group_by("chart")
        .agg(
            pl.len().alias("filas"),
            pl.col("streams").null_count().alias("sin_streams"),
            pl.col("date").min().alias("desde"),
            pl.col("date").max().alias("hasta"),
        )
        .sort("chart")
    )

    kworb = cargar("stg_kworb_mx_semanal").filter(pl.col("semana").str.contains(r"^\d{4}/"))
    print("\nKworb por año ('--' es una semana en que la canción no estuvo en el chart):")
    print(
        kworb.group_by(pl.col("semana").str.slice(0, 4).alias("anio"))
        .agg(
            pl.col("track_id").n_unique().alias("canciones"),
            pl.len().alias("semanas_canción"),
            (pl.col("valor") != "--").sum().alias("en_chart"),
        )
        .sort("anio")
    )

    rnpdno = cargar("stg_rnpdno").with_columns(
        pl.col("fecha_de_desaparicion").str.to_date("%d/%m/%Y", strict=False).alias("fecha")
    )
    validas = rnpdno.filter(pl.col("fecha").is_not_null())
    print(
        f"\nRNPDNO: {validas.height:,} de {rnpdno.height:,} registros con fecha válida "
        f"({validas.height / rnpdno.height:.1%}), del {validas['fecha'].min()} al {validas['fecha'].max()}"
    )
    print("Registros de mujeres con fecha válida, por año de desaparición dentro de la ventana:")
    print(
        validas.filter((pl.col("sexo") == "MUJER") & (pl.col("fecha").dt.year() >= 2015))
        .group_by(pl.col("fecha").dt.year().alias("anio"))
        .agg(pl.len().alias("registros"))
        .sort("anio")
    )


def nombres_de_entidad() -> None:
    """Compara los nombres de entidad de cada fuente contra los del SESNSP.

    Imprime, por fuente, los nombres que no coinciden después de normalizar
    mayúsculas y acentos. Son los que necesitan una regla en el catálogo de
    equivalencias.
    """
    seccion("Nombres de entidad que no coinciden con el SESNSP")
    referencia = set(
        cargar("stg_sesnsp_delitos").select(normalizar_nombre(pl.col("entidad"))).to_series().unique()
    )
    print(f"Referencia: {len(referencia)} nombres del SESNSP")

    for tabla, col in COLUMNA_ENTIDAD.items():
        nombres = cargar(tabla).select(normalizar_nombre(pl.col(col)).alias("nombre")).drop_nulls()
        sin_pareja = (
            nombres.filter(~pl.col("nombre").is_in(list(referencia)))
            .group_by("nombre")
            .agg(pl.len().alias("filas"))
            .sort("filas", descending=True)
        )
        distintos = nombres["nombre"].n_unique()
        if sin_pareja.is_empty():
            print(f"{tabla}: {distintos} nombres, todos coinciden")
        else:
            lista = ", ".join(f"{n} ({f:,})" for n, f in sin_pareja.iter_rows())
            print(f"{tabla}: {distintos} nombres, sin pareja: {lista}")

    claves = cargar("stg_endutih_usuarios")["cve_ent"].n_unique()
    print(f"stg_endutih_usuarios: usa clave, no nombre; {claves} claves distintas")


def casos_particulares() -> None:
    """Imprime problemas puntuales que no se ven en el diagnóstico general."""
    seccion("Casos particulares")

    misongyny = cargar("stg_misongyny")
    print("MiSonGyny por archivo de origen:")
    print(
        misongyny.group_by("archivo_origen")
        .agg(
            pl.len().alias("filas"),
            pl.col("label").null_count().alias("sin_etiqueta"),
            pl.col("label").drop_nulls().unique().sort().str.join(",").alias("etiquetas"),
        )
        .sort("archivo_origen")
    )

    natural_earth = cargar("stg_naturalearth_mx").drop("geometry_wkt")
    print("\nNatural Earth, filas sin nombre:")
    print(natural_earth.filter(pl.col("name").is_null()))

    rnpdno = cargar("stg_rnpdno")
    print("\nRNPDNO, registros por sexo:")
    print(rnpdno["sexo"].value_counts().sort("count", descending=True))
    reservado = pl.col(["sexo", "entidad_de_desaparicion", "autoridad_que_reporto"]).str.starts_with("ELIMINADO")
    filas_reservadas = rnpdno.select(pl.all_horizontal(reservado).sum()).item()
    print(f"RNPDNO, registros con sexo, entidad y autoridad reservados a la vez: {filas_reservadas:,}")
    edad = rnpdno["edad"].cast(pl.Int64, strict=False).drop_nulls()
    print(f"RNPDNO, edades mayores a 110 años: {(edad > 110).sum():,} (máxima: {edad.max()})")

    victimas = cargar("stg_sesnsp_victimas")
    print("\nSESNSP víctimas, filas por sexo y rango de edad:")
    print(victimas.group_by("sexo", "rango_de_edad").agg(pl.len().alias("filas")).sort("sexo", "rango_de_edad"))


def ejecutar_inspeccion() -> None:
    """Imprime el diagnóstico completo de todas las tablas de staging."""
    for tabla in TABLAS_STAGING:
        if not ruta_raw(tabla).exists():
            seccion(tabla)
            print("Sin copia local todavía; se omite (correr la ingesta o la carga de archivos)")
            continue
        inspeccionar_tabla(tabla)
    cobertura_temporal()
    nombres_de_entidad()
    casos_particulares()


if __name__ == "__main__":
    ejecutar_inspeccion()
