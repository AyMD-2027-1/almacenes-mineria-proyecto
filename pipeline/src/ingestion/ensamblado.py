"""
Ensamblado del dataset analítico al grano declarado en E1, a partir de las
copias locales de staging. Solo se convierten tipos y fechas y se agrega lo
que la fuente trae más fino que el grano; no se limpia nada todavía, porque
este dataset es la entrada del análisis exploratorio.

Produce tres tablas en data/data-processed/:
    analitico_violencia: entidad, delito o registro y mes (SESNSP y RNPDNO).
    analitico_consumo: canción y mes, a nivel nacional (Kworb).
    analitico_contexto: entidad y año (CONAPO, Censo, ENDUTIH, CONEVAL,
        México Evalúa y ENDIREH), cada indicador solo en el año en que se midió.

Este script se ejecuta desde pipeline/ con:
    python -m src.ingestion.ensamblado
"""

import sys
from pathlib import Path

import polars as pl

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from config.parametros import (  # noqa: E402
    DELITO_IMPUNIDAD,
    TABLA_KWORB_CATALOGO,
    TABLA_KWORB_SEMANAL,
    MAPEO_LGAMVLV,
    VENTANA_FIN,
    VENTANA_INICIO,
)
from config.rutas import (  # noqa: E402
    RUTA_ANALITICO_CONSUMO,
    RUTA_ANALITICO_CONTEXTO,
    RUTA_ANALITICO_VIOLENCIA,
    RUTA_CATALOGO_ENTIDADES,
    RUTA_DATA_PROCESSED,
    ruta_raw,
)
from src.ingestion.entidades import asignar_clave, construir_catalogo  # noqa: E402
from src.ingestion.generos import generos_por_artista  # noqa: E402
from src.registro import registrar_etapa, seccion  # noqa: E402

pl.Config.set_tbl_rows(20)

MESES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto",
         "septiembre", "octubre", "noviembre", "diciembre"]

LLAVE_DELITO = ["bien_juridico_afectado", "tipo_de_delito", "subtipo_de_delito", "modalidad"]

# Cada semana del chart de Kworb termina en la fecha publicada y cubre los 7 días anteriores.
DIAS_POR_SEMANA = 7

# Fila del anexo de CONEVAL con el porcentaje de población en pobreza, y año de cada columna.
FILA_POBREZA = "9"
ANIOS_CONEVAL = {"c04": 2016, "c05": 2018, "c06": 2020, "c07": 2022}

ANIO_CENSO = 2020
ANIO_ENDUTIH = 2024
# La ENDUTIH cuenta a las personas de 6 años y más.
EDAD_MINIMA_ENDUTIH = 6

# Tipos de violencia de la ENDIREH y su columna en dim_contexto_entidad.
PREVALENCIAS = {
    "total": "prevalencia_total",
    "psicologica": "prevalencia_psicologica",
    "fisica": "prevalencia_fisica",
    "sexual": "prevalencia_sexual",
    "economica_patrimonial": "prevalencia_economica_patrimonial",
}


def leer(tabla: str) -> pl.DataFrame:
    """Lee la copia local de una tabla de staging.

    Args:
        tabla (str): nombre de la tabla, por ejemplo "stg_rnpdno".

    Returns:
        pl.DataFrame: la tabla con sus columnas como texto.
    """
    return pl.read_parquet(ruta_raw(tabla))


def en_ventana(fecha: pl.Expr) -> pl.Expr:
    """Indica si una fecha cae dentro de la ventana de análisis de E0.

    Args:
        fecha (pl.Expr): expresión de tipo fecha, el primer día de un mes.

    Returns:
        pl.Expr: expresión booleana.
    """
    return fecha.is_between(VENTANA_INICIO, VENTANA_FIN)


def sesnsp_a_meses(df: pl.DataFrame, columnas_extra: list[str], valor: str) -> pl.DataFrame:
    """Pasa una tabla del SESNSP de una columna por mes a una fila por mes.

    Args:
        df (pl.DataFrame): archivo de delitos o de víctimas, tal como viene.
        columnas_extra (list[str]): columnas que se conservan además de la
            entidad, el año y el delito (por ejemplo sexo y rango de edad).
        valor (str): nombre de la columna con el conteo del mes.

    Returns:
        pl.DataFrame: una fila por entidad, delito y mes, con `cve_ent` (int),
            `fecha` (date, primer día del mes) y el conteo (int).
    """
    return (
        df.unpivot(index=["ano", "clave_ent", *LLAVE_DELITO, *columnas_extra], on=MESES,
                   variable_name="mes", value_name=valor)
        .with_columns(
            pl.col("clave_ent").cast(pl.Int64).alias("cve_ent"),
            pl.date(
                pl.col("ano").cast(pl.Int32),
                pl.col("mes").replace_strict(MESES, list(range(1, 13)), return_dtype=pl.Int8),
                1,
            ).alias("fecha"),
            pl.col(valor).cast(pl.Int64),
        )
        .drop("ano", "clave_ent", "mes")
    )


def ensamblar_sesnsp() -> pl.DataFrame:
    """Une carpetas y víctimas del SESNSP al grano de entidad, delito y mes.

    Las víctimas se resumen en mujeres, mujeres menores de edad y hombres;
    las de sexo no identificado no entran en ninguna de las tres.

    Returns:
        pl.DataFrame: una fila por entidad, delito (bien jurídico, tipo,
            subtipo y modalidad) y mes, con carpetas y víctimas.
    """
    carpetas = sesnsp_a_meses(leer("stg_sesnsp_delitos"), [], "carpetas")
    victimas = (
        sesnsp_a_meses(leer("stg_sesnsp_victimas"), ["sexo", "rango_de_edad"], "victimas")
        .group_by("cve_ent", "fecha", *LLAVE_DELITO)
        .agg(
            pl.col("victimas").filter(pl.col("sexo") == "Mujer").sum().alias("victimas_mujeres"),
            pl.col("victimas")
            .filter((pl.col("sexo") == "Mujer") & (pl.col("rango_de_edad") == "Menores de edad (0-17)"))
            .sum()
            .alias("victimas_mujeres_menores"),
            pl.col("victimas").filter(pl.col("sexo") == "Hombre").sum().alias("victimas_hombres"),
        )
    )
    llave = ["cve_ent", "fecha", *LLAVE_DELITO]
    sin_pareja = victimas.join(carpetas, on=llave, how="anti").height
    assert sin_pareja == 0, f"{sin_pareja} filas de víctimas no tienen delito en el archivo de carpetas"

    return carpetas.join(victimas, on=llave, how="left").with_columns(pl.lit("SESNSP").alias("origen"))


def ensamblar_rnpdno(catalogo: pl.DataFrame, registros: pl.DataFrame | None = None) -> tuple[pl.DataFrame, int]:
    """Cuenta mujeres desaparecidas por entidad y mes de desaparición.

    Solo se pueden ubicar en el grano los registros de mujeres con fecha
    válida y entidad identificable; los demás se cuentan y se reportan.

    Args:
        catalogo (pl.DataFrame): catálogo de entidades.
        registros (pl.DataFrame | None): registros del RNPDNO; si es None se
            usa la copia de staging tal como viene. La limpieza pasa aquí los
            registros ya sin duplicados.

    Returns:
        tuple[pl.DataFrame, int]: la tabla al grano de entidad y mes, con las
            mismas columnas que el SESNSP, y el número de registros de
            mujeres que no se pudieron ubicar.
    """
    if registros is None:
        registros = leer("stg_rnpdno")
    mujeres = asignar_clave(registros, "entidad_de_desaparicion", catalogo).filter(
        pl.col("sexo") == "MUJER"
    ).with_columns(
        pl.col("fecha_de_desaparicion").str.to_date("%d/%m/%Y", strict=False).dt.month_start().alias("fecha"),
        pl.col("edad").cast(pl.Int64, strict=False).alias("edad"),
    )
    ubicables = mujeres.filter(pl.col("fecha").is_not_null() & pl.col("cve_ent").is_not_null())

    tabla = (
        ubicables.group_by("cve_ent", "fecha")
        .agg(
            pl.len().cast(pl.Int64).alias("victimas_mujeres"),
            (pl.col("edad") < 18).sum().cast(pl.Int64).alias("victimas_mujeres_menores"),
        )
        .with_columns(
            pl.lit("Desaparición").alias("bien_juridico_afectado"),
            pl.lit("Desaparición").alias("tipo_de_delito"),
            pl.lit("Mujer desaparecida").alias("subtipo_de_delito"),
            pl.lit("Mujer desaparecida").alias("modalidad"),
            pl.lit(None, dtype=pl.Int64).alias("carpetas"),
            pl.lit(None, dtype=pl.Int64).alias("victimas_hombres"),
            pl.lit("RNPDNO").alias("origen"),
        )
    )
    return tabla, mujeres.height - ubicables.height


def ensamblar_violencia(catalogo: pl.DataFrame) -> pl.DataFrame:
    """Arma el dataset analítico de violencia con el SESNSP y el RNPDNO.

    Args:
        catalogo (pl.DataFrame): catálogo de entidades.

    Returns:
        pl.DataFrame: una fila por entidad, delito o registro y mes dentro de
            la ventana, con el tipo de la LGAMVLV (nulo si el delito no se mapea).
    """
    seccion("Violencia: entidad, delito o registro y mes")
    sesnsp = ensamblar_sesnsp()
    rnpdno, sin_ubicar = ensamblar_rnpdno(catalogo)
    print(f"SESNSP: {sesnsp.height:,} filas   RNPDNO: {rnpdno.height:,} filas")
    print(f"Registros de mujeres del RNPDNO sin fecha válida o sin entidad: {sin_ubicar:,}")

    columnas = ["cve_ent", "fecha", "origen", *LLAVE_DELITO, "tipo_lgamvlv",
                "carpetas", "victimas_mujeres", "victimas_mujeres_menores", "victimas_hombres"]
    violencia = (
        pl.concat([sesnsp, rnpdno], how="diagonal")
        .with_columns(
            pl.when(pl.col("origen") == "RNPDNO")
            .then(pl.lit("desaparición"))
            .otherwise(pl.col("subtipo_de_delito").replace_strict(MAPEO_LGAMVLV, default=None))
            .alias("tipo_lgamvlv")
        )
        .select(columnas)
    )
    total = violencia.height
    violencia = violencia.filter(en_ventana(pl.col("fecha"))).sort("cve_ent", "fecha", "origen", *LLAVE_DELITO)
    print(f"Fuera de la ventana {VENTANA_INICIO:%Y-%m} a {VENTANA_FIN:%Y-%m}: {total - violencia.height:,} filas")

    llave = ["cve_ent", "fecha", "origen", *LLAVE_DELITO]
    assert violencia.select(pl.struct(llave).is_unique().all()).item(), "el grano de violencia tiene filas repetidas"
    registrar_etapa("ensamblado", "analitico_violencia", total, violencia.height,
                    f"SESNSP {sesnsp.height:,} y RNPDNO {rnpdno.height:,} filas; {sin_ubicar:,} registros sin ubicar")
    return violencia


def separar_celda_kworb(df: pl.DataFrame) -> pl.DataFrame:
    """Separa la celda `posición (streams)` de Kworb en dos columnas numéricas.

    Una celda `--` indica una semana en que la canción no estuvo en el chart:
    se registran 0 streams y posición nula.

    Args:
        df (pl.DataFrame): filas semanales de Kworb con las columnas
            `track_id`, `semana` y `valor`.

    Returns:
        pl.DataFrame: columnas `track_id` (str), `fin_semana` (date),
            `posicion` (int, nula fuera del chart) y `streams` (int).
    """
    partes = pl.col("valor").str.extract_groups(r"^(\d+) \(([\d,]+)\)$")
    return df.select(
        "track_id",
        pl.col("semana").str.to_date("%Y/%m/%d").alias("fin_semana"),
        partes.struct.field("1").cast(pl.Int64).alias("posicion"),
        partes.struct.field("2").str.replace_all(",", "").cast(pl.Int64).fill_null(0).alias("streams"),
    )


def ensamblar_consumo() -> pl.DataFrame:
    """Arma el dataset analítico de consumo: canción y mes, a nivel nacional.

    Los streams de cada semana se reparten entre los meses según cuántos de
    sus 7 días caen en cada uno, con el supuesto de que el consumo es parejo
    dentro de la semana. Las semanas en el chart y la mejor posición se
    cuentan en el mes en que termina cada semana.

    A cada canción se le agrega el país y el macrogénero de su artista.

    Returns:
        pl.DataFrame: una fila por canción y mes dentro de la ventana, con
            artista, título, país, macrogénero, streams, semanas en el chart y
            mejor posición.
    """
    seccion("Consumo: canción y mes, nacional")
    crudo = leer(TABLA_KWORB_SEMANAL)
    semanal = separar_celda_kworb(crudo.filter(pl.col("semana").str.contains(r"^\d{4}/")))
    print(f"Filas semanales: {semanal.height:,} (se omiten {crudo.height - semanal.height} filas de Total y Peak)")

    por_dia = (
        semanal.with_columns(
            pl.date_ranges(pl.col("fin_semana").dt.offset_by(f"-{DIAS_POR_SEMANA - 1}d"), pl.col("fin_semana")).alias("dia")
        )
        .explode("dia")
        .with_columns(pl.col("dia").dt.month_start().alias("fecha"))
    )
    streams = por_dia.group_by("track_id", "fecha").agg(
        (pl.col("streams").sum() / DIAS_POR_SEMANA).alias("streams")
    )
    chart = (
        semanal.filter(pl.col("posicion").is_not_null())
        .group_by("track_id", pl.col("fin_semana").dt.month_start().alias("fecha"))
        .agg(
            pl.len().cast(pl.Int64).alias("semanas_en_chart"),
            pl.col("posicion").min().alias("mejor_posicion"),
        )
    )
    catalogo = leer(TABLA_KWORB_CATALOGO).join(
        generos_por_artista().select("artista", "pais_artista", "macrogenero", "origen_genero", "etiquetas_fuente"),
        on="artista",
        how="left",
    )

    consumo = (
        streams.join(chart, on=["track_id", "fecha"], how="left")
        .with_columns(pl.col("semanas_en_chart").fill_null(0))
        .join(catalogo, on="track_id", how="left")
        .select("track_id", "artista", "titulo", "pais_artista", "macrogenero", "origen_genero", "etiquetas_fuente",
                "fecha", "streams", "semanas_en_chart", "mejor_posicion")
    )
    total = consumo.height
    consumo = consumo.filter(en_ventana(pl.col("fecha"))).sort("track_id", "fecha")
    print(f"Canción y mes: {total:,} filas; fuera de la ventana: {total - consumo.height:,}")

    assert consumo["artista"].null_count() == 0, "hay canciones del historial que no están en el catálogo"
    assert consumo["macrogenero"].null_count() == 0, "hay artistas del catálogo sin búsqueda en MusicBrainz"
    assert consumo.select(pl.struct("track_id", "fecha").is_unique().all()).item(), "el grano de consumo tiene filas repetidas"
    registrar_etapa("ensamblado", "analitico_consumo", semanal.height, consumo.height,
                    "semanas repartidas a meses por días; nacional")
    return consumo


def ensamblar_contexto(catalogo: pl.DataFrame) -> pl.DataFrame:
    """Arma el dataset analítico de contexto por entidad y año.

    La base son las 32 entidades por año de CONAPO. Los indicadores de las
    demás fuentes se colocan solo en el año en que se midieron; los años sin
    medición quedan nulos y la decisión de rellenarlos o no se toma en la
    limpieza.

    Args:
        catalogo (pl.DataFrame): catálogo de entidades.

    Returns:
        pl.DataFrame: una fila por entidad y año de la ventana.
    """
    seccion("Contexto: entidad y año")
    conapo = leer("stg_conapo_poblacion").with_columns(
        pl.col("anio").cast(pl.Int32),
        pl.col("cve_geo").cast(pl.Int64).alias("cve_ent"),
        pl.col("edad").cast(pl.Int64),
        pl.col("poblacion").cast(pl.Int64),
    )
    base = conapo.group_by("cve_ent", "anio").agg(
        pl.col("poblacion").sum().alias("poblacion_total"),
        pl.col("poblacion").filter(pl.col("sexo") == "Mujeres").sum().alias("poblacion_mujeres"),
        pl.col("poblacion").filter((pl.col("sexo") == "Mujeres") & (pl.col("edad") >= 15)).sum().alias("poblacion_mujeres_15_mas"),
        pl.col("poblacion").filter(pl.col("sexo") == "Hombres").sum().alias("poblacion_hombres"),
        pl.col("poblacion").filter(pl.col("edad") >= EDAD_MINIMA_ENDUTIH).sum().alias("poblacion_6_mas"),
    )

    censo = leer("stg_censo_iter").select(
        pl.col("ENTIDAD").cast(pl.Int64).alias("cve_ent"),
        pl.lit(ANIO_CENSO, dtype=pl.Int32).alias("anio"),
        (pl.col("VPH_INTER").cast(pl.Float64) / pl.col("TVIVPARHAB").cast(pl.Float64) * 100).alias("pct_viviendas_internet"),
    ).filter(pl.col("cve_ent") > 0)

    # Usuarios de internet por entidad y año de los tabulados de INEGI; el
    # porcentaje se calcula sobre la población de 6 años y más de CONAPO.
    endutih = (
        asignar_clave(leer("stg_endutih_usuarios_entidad_anio"), "entidad", catalogo)
        .filter(pl.col("cve_ent") > 0)
        .select("cve_ent", pl.col("anio").cast(pl.Int32), pl.col("usuarios_internet").cast(pl.Int64))
    )
    assert endutih.group_by("anio").len()["len"].unique().to_list() == [32], "un año de la ENDUTIH no trae las 32 entidades"

    # El microdato de 2024 sirve para validar el porcentaje que sale de los tabulados.
    microdato = (
        leer("stg_endutih_usuarios")
        .with_columns(pl.col("cve_ent").cast(pl.Int64), pl.col("fac_per").cast(pl.Float64))
        .group_by("cve_ent")
        .agg((pl.col("fac_per").filter(pl.col("p7_1") == "1").sum() / pl.col("fac_per").sum() * 100).alias("pct_microdato"))
    )

    coneval = (
        asignar_clave(leer("stg_coneval_pobreza").filter(pl.col("fila") == FILA_POBREZA), "hoja", catalogo)
        .filter(pl.col("cve_ent") > 0)
        .unpivot(index="cve_ent", on=list(ANIOS_CONEVAL), variable_name="columna", value_name="pct_pobreza")
        .select(
            "cve_ent",
            pl.col("columna").replace_strict(ANIOS_CONEVAL, return_dtype=pl.Int32).alias("anio"),
            pl.col("pct_pobreza").cast(pl.Float64),
        )
    )

    evalua = leer("stg_mexico_evalua_12_indices").filter(pl.col("delito") == DELITO_IMPUNIDAD)
    evalua = asignar_clave(evalua, "entidad", catalogo).filter(pl.col("cve_ent") > 0).select(
        "cve_ent",
        pl.col("anio").cast(pl.Int32),
        pl.col("indice_impunidad").cast(pl.Float64),
    )

    endireh = (
        asignar_clave(leer("stg_endireh_2021_prevalencias"), "entidad", catalogo)
        .filter(
            (pl.col("cve_ent") > 0)
            & (pl.col("persona_agresora") == "total")
            & (pl.col("medida") == "estimacion")
        )
        .with_columns(
            pl.col("levantamiento").cast(pl.Int32).alias("anio"),
            pl.col("tipo_violencia").replace_strict(PREVALENCIAS),
            pl.col("valor").cast(pl.Float64),
        )
        .pivot(on="tipo_violencia", index=["cve_ent", "anio"], values="valor")
    )
    assert endireh["cve_ent"].n_unique() == 32, "la ENDIREH no trae las 32 entidades"

    contexto = base.join(endutih, on=["cve_ent", "anio"], how="left").with_columns(
        (pl.col("usuarios_internet") / pl.col("poblacion_6_mas") * 100).alias("pct_usuarios_internet")
    )
    validacion = contexto.filter(pl.col("anio") == ANIO_ENDUTIH).join(microdato, on="cve_ent")
    print(f"ENDUTIH {ANIO_ENDUTIH}, % de usuarios: tabulado contra microdato, diferencia media "
          f"{(validacion['pct_usuarios_internet'] - validacion['pct_microdato']).abs().mean():.1f} puntos")
    for tabla in (censo, coneval, evalua, endireh):
        contexto = contexto.join(tabla, on=["cve_ent", "anio"], how="left")
    contexto = contexto.filter(
        pl.col("anio").is_between(VENTANA_INICIO.year, VENTANA_FIN.year)
    ).sort("cve_ent", "anio")

    assert contexto.height == 32 * (VENTANA_FIN.year - VENTANA_INICIO.year + 1), "faltan entidades o años en CONAPO"
    print(f"Entidad y año: {contexto.height:,} filas, {contexto.width} columnas")
    print("Años con dato por indicador:")
    print(
        contexto.unpivot(index=["cve_ent", "anio"], on=["pct_viviendas_internet", "pct_usuarios_internet",
                                                       "pct_pobreza", "indice_impunidad", *PREVALENCIAS.values()])
        .drop_nulls("value")
        .group_by("variable")
        .agg(pl.col("anio").unique().sort().cast(pl.String).str.join(", ").alias("anios"),
             pl.col("cve_ent").n_unique().alias("entidades"))
        .sort("variable")
    )
    registrar_etapa("ensamblado", "analitico_contexto", None, contexto.height,
                    "CONAPO como base; indicadores solo en su año de medición")
    return contexto


def ejecutar_ensamblado() -> dict[str, pl.DataFrame]:
    """Arma y guarda las tres tablas del dataset analítico y el catálogo de entidades.

    Returns:
        dict[str, pl.DataFrame]: las tablas `violencia`, `consumo`,
            `contexto` y `catalogo`.
    """
    RUTA_DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    catalogo = construir_catalogo()
    tablas = {
        "violencia": ensamblar_violencia(catalogo),
        "consumo": ensamblar_consumo(),
        "contexto": ensamblar_contexto(catalogo),
        "catalogo": catalogo,
    }
    rutas = {
        "violencia": RUTA_ANALITICO_VIOLENCIA,
        "consumo": RUTA_ANALITICO_CONSUMO,
        "contexto": RUTA_ANALITICO_CONTEXTO,
        "catalogo": RUTA_CATALOGO_ENTIDADES,
    }
    seccion("Archivos guardados")
    for nombre, df in tablas.items():
        df.write_parquet(rutas[nombre])
        print(f"{rutas[nombre].name:<32} {df.height:>8,} filas  {df.width:>3} columnas")
    return tablas


if __name__ == "__main__":
    ejecutar_ensamblado()
