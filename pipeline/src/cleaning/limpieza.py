"""
Limpieza del dataset analítico. Cada problema se diagnostica primero, se
trata con una regla declarada en config.yaml o en este archivo y queda en el
registro de limpieza (logs/registro_limpieza.csv) con su conteo y su
justificación. Nada se elimina ni se imputa sin pasar por el registro.

Produce en data/data-processed/ las tablas que se cargan al almacén:
    limpio_violencia: entidad, delito o registro y mes, solo tipos de la LGAMVLV.
    limpio_consumo: canción y mes con streams.
    limpio_canciones: una fila por canción, con género y etiqueta de MiSonGyny.
    limpio_contexto: entidad y año, con los indicadores completados.
    limpio_entidades: las 32 entidades con su clave ISO y su geometría.

Este script se ejecuta desde pipeline/ con:
    python -m src.cleaning.limpieza
"""

import sys
from pathlib import Path

import polars as pl

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from config.parametros import (  # noqa: E402
    EDAD_MAXIMA,
    TABLA_KWORB_CATALOGO,
    IQR_FACTOR,
    MAPEO_LGAMVLV,
    MESES_CERO_INICIAL_MIN,
    VENTANA_FIN,
    VENTANA_INICIO,
    Z_ROBUSTO_UMBRAL,
)
from config.rutas import (  # noqa: E402
    RUTA_LIMPIO_CANCIONES,
    RUTA_LIMPIO_CONSUMO,
    RUTA_LIMPIO_CONTEXTO,
    RUTA_LIMPIO_ENTIDADES,
    RUTA_LIMPIO_VIOLENCIA,
)
from src.cleaning.calidad import anotar, ceros_iniciales, fuera_de_iqr, guardar_registro, z_robusto  # noqa: E402
from src.eda.vistas import POR_CADA, consumo, contexto, violencia  # noqa: E402
from src.ingestion.ensamblado import LLAVE_DELITO, ensamblar_rnpdno, leer, sesnsp_a_meses  # noqa: E402
from src.ingestion.entidades import asignar_clave, construir_catalogo  # noqa: E402
from src.ingestion.generos import comparable  # noqa: E402
from src.registro import registrar_etapa, seccion  # noqa: E402

COLUMNAS_VIOLENCIA = ["cve_ent", "fecha", "origen", *LLAVE_DELITO, "tipo_lgamvlv",
                      "carpetas", "victimas_mujeres", "victimas_mujeres_menores", "victimas_hombres"]
CONTEOS_VIOLENCIA = ["carpetas", "victimas_mujeres", "victimas_mujeres_menores", "victimas_hombres"]

# Indicadores de contexto que solo se miden en algunos años.
INDICADORES_CONTEXTO = ["pct_viviendas_internet", "usuarios_internet", "pct_usuarios_internet", "pct_pobreza",
                        "indice_impunidad"]

# Patrón de las claves ISO 3166-2 de las entidades de México (MX-AGU, MX-CMX...).
PATRON_ISO = r"^MX-[A-Z]{3}$"

# En MiSonGyny la tarea 1 dice si la letra es misógina (M o NM) y la tarea 2 en
# qué categoría cae; NR es "no relacionada", así que equivale a NM.
MISOGINA_TAREA_1 = {"M": True, "NM": False}
MISOGINA_TAREA_2 = {"H": True, "S": True, "V": True, "NR": False}


def limpiar_rnpdno(registro: list[dict], catalogo: pl.DataFrame) -> pl.DataFrame:
    """Corrige edades imposibles del RNPDNO y lo vuelve a agregar al grano.

    Las filas idénticas no se eliminan: la versión pública no trae un
    identificador de persona, así que dos registros iguales pueden ser dos
    personas distintas (casi todas son filas con todos sus datos testados).

    Args:
        registro (list[dict]): registro de limpieza.
        catalogo (pl.DataFrame): catálogo de entidades.

    Returns:
        pl.DataFrame: mujeres desaparecidas por entidad y mes dentro de la ventana.
    """
    crudo = leer("stg_rnpdno")
    repetidas = crudo.filter(crudo.is_duplicated())
    testadas = repetidas.filter(pl.col("sexo").str.starts_with("ELIMINADO")).height
    anotar(registro, "stg_rnpdno", "fila", "registros idénticos en las 8 columnas", repetidas.height, crudo.height,
           "conservar",
           f"son personas distintas que se ven iguales: en staging se quitaron las columnas de nombre para proteger "
           f"su identidad, no queda un identificador de persona ({crudo['consecutivo_registro'].n_unique()} valores "
           f"distintos en consecutivo_registro) y {testadas:,} de ellas tienen todos sus datos testados")
    mujeres = crudo.filter(pl.col("sexo") == "MUJER").height

    edad = pl.col("edad").cast(pl.Int64, strict=False)
    imposibles = crudo.filter(edad > EDAD_MAXIMA).height
    limpio = crudo.with_columns(pl.when(edad > EDAD_MAXIMA).then(None).otherwise(pl.col("edad")).alias("edad"))
    anotar(registro, "stg_rnpdno", "edad", f"edad mayor a {EDAD_MAXIMA} años", imposibles, crudo.height,
           "volver nula", "es un error de captura; el registro se conserva porque la fecha y la entidad sí sirven")

    marcadores = crudo.filter(pl.col("sexo").str.starts_with("ELIMINADO")).height
    anotar(registro, "stg_rnpdno", "sexo, entidad", "datos testados con la leyenda ELIMINADO", marcadores,
           crudo.height, "excluir del conteo de mujeres",
           "la fuente los oculta por protección de datos; no se puede saber si son mujeres ni dónde desaparecieron")

    agregado, sin_ubicar = ensamblar_rnpdno(catalogo, limpio)
    anotar(registro, "stg_rnpdno", "fecha_de_desaparicion", "mujeres sin fecha válida o sin entidad",
           sin_ubicar, mujeres, "excluir y reportar como límite",
           "sin mes o sin entidad no se pueden ubicar en el grano; por eso el RNPDNO es solo fuente de contraste para P2")

    ultimo_mes = agregado["fecha"].max()
    meses_sin_dato = len(pl.date_range(ultimo_mes, VENTANA_FIN, "1mo", eager=True)) - 1
    anotar(registro, "analitico_violencia", "victimas_mujeres (RNPDNO)", f"la fuente termina en {ultimo_mes:%Y-%m}",
           meses_sin_dato, len(pl.date_range(VENTANA_INICIO, VENTANA_FIN, "1mo", eager=True)),
           "dejar sin filas, no rellenar con cero",
           "son meses que la versión descargada no cubre; un cero diría que no hubo desapariciones")
    return agregado.filter(pl.col("fecha").is_between(VENTANA_INICIO, VENTANA_FIN))


def limpiar_violencia(registro: list[dict], catalogo: pl.DataFrame) -> pl.DataFrame:
    """Limpia el dataset analítico de violencia.

    Args:
        registro (list[dict]): registro de limpieza.
        catalogo (pl.DataFrame): catálogo de entidades.

    Returns:
        pl.DataFrame: SESNSP y RNPDNO limpios, solo con delitos que tienen
            tipo de la LGAMVLV o que sirven de control.
    """
    seccion("Limpieza de violencia")
    analitico = violencia().drop("anio")
    sesnsp = analitico.filter(pl.col("origen") == "SESNSP")

    sin_tipo = sesnsp.filter(pl.col("tipo_lgamvlv").is_null())
    anotar(registro, "analitico_violencia", "tipo_lgamvlv", "delito sin tipo de violencia de la LGAMVLV",
           sin_tipo.height, sesnsp.height, "excluir",
           f"son delitos como robo o fraude que no responden P2; el mapeo de E1 conserva {len(MAPEO_LGAMVLV)} subtipos")
    sesnsp = sesnsp.filter(pl.col("tipo_lgamvlv").is_not_null())

    sin_victimas = sesnsp.filter(pl.col("victimas_mujeres").is_null()).height
    anotar(registro, "analitico_violencia", "victimas_mujeres", "delito que el SESNSP no reporta por víctima",
           sin_victimas, sesnsp.height, "dejar nulo",
           "es un faltante estructural: el archivo de víctimas del SESNSP no cubre delitos sexuales ni violencia "
           "familiar, solo carpetas; no es un cero")

    victimas = sesnsp_a_meses(leer("stg_sesnsp_victimas"), ["sexo", "rango_de_edad"], "victimas").filter(
        pl.col("subtipo_de_delito").is_in(list(MAPEO_LGAMVLV)) & pl.col("fecha").is_between(VENTANA_INICIO, VENTANA_FIN)
    )
    total_victimas = victimas["victimas"].sum()
    sexo_desconocido = victimas.filter(pl.col("sexo") == "No identificado")["victimas"].sum()
    anotar(registro, "stg_sesnsp_victimas", "sexo", "víctimas con sexo no identificado", sexo_desconocido,
           total_victimas, "no asignarlas a mujeres ni a hombres",
           "repartirlas inventaría víctimas; el conteo de mujeres es un mínimo y así se aclara en decisiones")
    mujeres = victimas.filter(pl.col("sexo") == "Mujer")
    edad_desconocida = mujeres.filter(pl.col("rango_de_edad") == "No especificado")["victimas"].sum()
    anotar(registro, "stg_sesnsp_victimas", "rango_de_edad", "mujeres víctimas sin edad especificada",
           edad_desconocida, mujeres["victimas"].sum(), "no imputar; victimas_mujeres_menores es un mínimo",
           "la falta de edad depende de la fiscalía de cada estado, así que imputarla movería las diferencias entre estados")

    feminicidio = (
        sesnsp.filter(pl.col("tipo_lgamvlv") == "feminicida").group_by("cve_ent", "fecha").agg(pl.col("carpetas").sum())
    )
    arranque = ceros_iniciales(feminicidio, "cve_ent", "fecha", "carpetas").filter(
        pl.col("ceros_iniciales") >= MESES_CERO_INICIAL_MIN
    )
    no_registrado = (
        pl.col("tipo_lgamvlv") == "feminicida"
    ) & (pl.col("fecha") < pl.col("primer_positivo"))
    sesnsp = sesnsp.join(arranque.select("cve_ent", "primer_positivo"), on="cve_ent", how="left")
    celdas = sesnsp.filter(no_registrado.fill_null(False)).height
    sesnsp = sesnsp.with_columns(
        [pl.when(no_registrado.fill_null(False)).then(None).otherwise(pl.col(c)).alias(c) for c in CONTEOS_VIOLENCIA]
    ).drop("primer_positivo")
    estados = ", ".join(f"{r['cve_ent']} hasta {r['primer_positivo']:%Y-%m}" for r in arranque.sort("cve_ent").iter_rows(named=True))
    anotar(registro, "analitico_violencia", "carpetas (feminicidio)",
           f"estados que empiezan la serie con {MESES_CERO_INICIAL_MIN} meses o más en cero",
           celdas, sesnsp.filter(pl.col("tipo_lgamvlv") == "feminicida").height, "volver nulo, no cero",
           f"la fiscalía todavía no clasificaba el delito como feminicidio, no es que no hubiera ({estados})")

    tasas = (
        sesnsp.drop_nulls("carpetas")
        .group_by("cve_ent", "fecha", "tipo_lgamvlv")
        .agg(pl.col("carpetas").sum())
        .with_columns(pl.col("fecha").dt.year().alias("anio"))
        .join(contexto().select("cve_ent", "anio", "poblacion_mujeres"), on=["cve_ent", "anio"])
        .with_columns((pl.col("carpetas") / pl.col("poblacion_mujeres") * POR_CADA).alias("tasa"))
        .with_columns(
            z_robusto(pl.col("tasa")).over("cve_ent", "tipo_lgamvlv").alias("z"),
            fuera_de_iqr(pl.col("tasa"), IQR_FACTOR).over("tipo_lgamvlv").alias("fuera_iqr"),
        )
    )
    atipicos = tasas.filter(pl.col("z").abs() > Z_ROBUSTO_UMBRAL)
    anotar(registro, "analitico_violencia", "tasa mensual por entidad",
           f"mes atípico dentro de la serie de su estado (|z robusto| > {Z_ROBUSTO_UMBRAL})",
           atipicos.height, tasas.height, "conservar",
           "son picos reales de denuncias (por ejemplo tras cambios de registro o campañas); quitarlos borraría el fenómeno de P2")
    anotar(registro, "analitico_violencia", "tasa mensual por entidad", f"fuera de las cercas de Tukey ({IQR_FACTOR}·IQR) entre estados",
           tasas["fuera_iqr"].sum(), tasas.height, "conservar y analizar en tasas, no en conteos",
           "son estados con tasas altas de forma sostenida, que es justo la diferencia que P2 quiere ver")

    rnpdno = limpiar_rnpdno(registro, catalogo).with_columns(pl.lit("desaparición").alias("tipo_lgamvlv"))
    limpio = pl.concat([sesnsp.select(COLUMNAS_VIOLENCIA), rnpdno.select(COLUMNAS_VIOLENCIA)]).sort(
        "cve_ent", "fecha", "origen", *LLAVE_DELITO
    )
    assert limpio.select(pl.struct("cve_ent", "fecha", "origen", *LLAVE_DELITO).is_unique().all()).item()
    assert limpio["tipo_lgamvlv"].null_count() == 0
    registrar_etapa("limpieza", "limpio_violencia", analitico.height, limpio.height,
                    "solo delitos con tipo LGAMVLV; RNPDNO sin duplicados")
    return limpio


def limpiar_consumo(registro: list[dict]) -> pl.DataFrame:
    """Limpia el dataset analítico de consumo.

    Args:
        registro (list[dict]): registro de limpieza.

    Returns:
        pl.DataFrame: una fila por canción y mes con streams mayores a cero.
    """
    seccion("Limpieza de consumo")
    analitico = consumo().drop("anio")

    ceros = analitico.filter(pl.col("streams") == 0)
    anotar(registro, "analitico_consumo", "streams", "canción y mes con 0 streams (celdas -- de Kworb)",
           ceros.height, analitico.height, "eliminar la fila",
           "es un cero estructural: la canción estaba fuera del chart; el hecho de consumo solo guarda lo que se escuchó")
    limpio = analitico.filter(pl.col("streams") > 0)

    sin_genero = limpio.filter(pl.col("origen_genero") == "sin dato")
    anotar(registro, "analitico_consumo", "macrogenero", "artista sin género en MusicBrainz", sin_genero.height,
           limpio.height, "dejar como categoría 'sin etiqueta'",
           f"son {sin_genero['streams'].sum() / limpio['streams'].sum():.0%} de los streams; asignarles un género "
           "inventaría la composición que mide P1")

    log_streams = pl.col("streams").log10()
    atipicos = limpio.filter(fuera_de_iqr(pl.col("streams"), IQR_FACTOR))
    atipicos_log = limpio.filter(fuera_de_iqr(log_streams, IQR_FACTOR))
    anotar(registro, "analitico_consumo", "streams", f"fuera de las cercas de Tukey ({IQR_FACTOR}·IQR)",
           atipicos.height, limpio.height, "conservar y usar log10 en las comparaciones",
           f"son los éxitos del año, el centro de P1; en escala log solo quedan {atipicos_log.height} fuera de las cercas")

    assert limpio.select(pl.struct("track_id", "fecha").is_unique().all()).item()
    registrar_etapa("limpieza", "limpio_consumo", analitico.height, limpio.height, "sin meses en cero")
    return limpio


def etiquetas_misongyny(registro: list[dict]) -> pl.DataFrame:
    """Arma una fila por canción del corpus MiSonGyny con sus etiquetas.

    Args:
        registro (list[dict]): registro de limpieza.

    Returns:
        pl.DataFrame: columnas `titulo_comparable`, `artista_comparable`,
            `etiqueta_tarea_1` y `etiqueta_tarea_2` (str, nulas si la canción no
            está en esa tarea).
    """
    crudo = leer("stg_misongyny")
    encabezado = crudo.filter(pl.col("id") == "id")
    anotar(registro, "stg_misongyny", "fila", "la fila de encabezados se cargó como dato", encabezado.height,
           crudo.height, "eliminar", "no es una canción")
    crudo = crudo.filter(pl.col("id") != "id")

    respuestas = crudo.filter(pl.col("archivo_origen").str.starts_with("TEST_LABELS")).select("id", pl.col("label").alias("respuesta"))
    canciones = (
        crudo.filter(~pl.col("archivo_origen").str.starts_with("TEST_LABELS"))
        .join(respuestas, on="id", how="left")
        .with_columns(
            pl.coalesce("label", "respuesta").alias("label"),
            pl.col("archivo_origen").str.extract(r"Task_(\d)").alias("tarea"),
        )
    )
    anotar(registro, "stg_misongyny", "label", "canciones de prueba con la etiqueta en otro archivo",
           respuestas.height, canciones.height, "unir por id con TEST_LABELS",
           "el corpus publica las respuestas de la partición de prueba por separado")
    assert canciones["label"].null_count() == 0, "quedaron canciones de MiSonGyny sin etiqueta"

    return (
        canciones.with_columns(
            comparable(pl.col("song_title").str.replace_all(r"\(.*?\)|\[.*?\]", "")).alias("titulo_comparable"),
            comparable(pl.col("song_artist")).alias("artista_comparable"),
        )
        .group_by("titulo_comparable", "artista_comparable")
        .agg(
            pl.col("label").filter(pl.col("tarea") == "1").first().alias("etiqueta_tarea_1"),
            pl.col("label").filter(pl.col("tarea") == "2").first().alias("etiqueta_tarea_2"),
        )
    )


def limpiar_canciones(registro: list[dict], consumo_limpio: pl.DataFrame) -> pl.DataFrame:
    """Arma la tabla de canciones con género y etiqueta de misoginia de MiSonGyny.

    Una canción del chart se empareja con el corpus cuando el título
    simplificado es igual y el nombre de un artista contiene al otro (el
    corpus suele listar a todos los artistas y Kworb solo al principal).

    Args:
        registro (list[dict]): registro de limpieza.
        consumo_limpio (pl.DataFrame): resultado de `limpiar_consumo`, para
            medir la cobertura en streams.

    Returns:
        pl.DataFrame: una fila por canción del catálogo de Kworb.
    """
    seccion("Limpieza de canciones")
    generos = (
        consumo_limpio.select("track_id", "pais_artista", "macrogenero", "origen_genero", "etiquetas_fuente").unique("track_id")
    )
    catalogo = leer(TABLA_KWORB_CATALOGO).join(generos, on="track_id", how="left").with_columns(
        comparable(pl.col("titulo").str.replace_all(r"\(.*?\)|\[.*?\]", "")).alias("titulo_comparable"),
        comparable(pl.col("artista")).alias("artista_comparable"),
    )
    assert catalogo["track_id"].is_unique().all(), "el catálogo de Kworb repite canciones"

    corpus = etiquetas_misongyny(registro)
    pares = catalogo.select("track_id", "titulo_comparable", "artista_comparable").join(
        corpus, on="titulo_comparable", suffix="_corpus"
    )
    mismo_artista = pl.col("artista_comparable_corpus").str.contains(pl.col("artista_comparable"), literal=True) | pl.col(
        "artista_comparable"
    ).str.contains(pl.col("artista_comparable_corpus"), literal=True)
    descartados = pares.filter(~mismo_artista)["track_id"].n_unique()
    pares = pares.filter(mismo_artista).group_by("track_id").agg(
        pl.col("etiqueta_tarea_1").drop_nulls().first(), pl.col("etiqueta_tarea_2").drop_nulls().first()
    )

    canciones = (
        catalogo.join(pares, on="track_id", how="left")
        .with_columns(
            pl.col("etiqueta_tarea_1").is_not_null().or_(pl.col("etiqueta_tarea_2").is_not_null()).alias("en_corpus_misongyny"),
            pl.coalesce(
                pl.col("etiqueta_tarea_1").replace_strict(MISOGINA_TAREA_1, default=None, return_dtype=pl.Boolean),
                pl.col("etiqueta_tarea_2").replace_strict(MISOGINA_TAREA_2, default=None, return_dtype=pl.Boolean),
            ).alias("es_misogina"),
        )
        .with_columns(
            pl.when(pl.col("es_misogina").is_not_null()).then(pl.lit("misongyny")).alias("origen_etiqueta"),
            pl.col("macrogenero").fill_null("sin etiqueta"),
        )
        .drop("titulo_comparable", "artista_comparable")
        .sort("track_id")
    )
    clasificadas = canciones.filter(pl.col("es_misogina").is_not_null())
    streams = consumo_limpio.group_by("track_id").agg(pl.col("streams").sum())
    cobertura = streams.filter(pl.col("track_id").is_in(clasificadas["track_id"].implode()))["streams"].sum() / streams["streams"].sum()
    anotar(registro, "dim_cancion", "es_misogina", "canción del chart que no está en MiSonGyny",
           canciones.height - clasificadas.height, canciones.height, "dejar nulo",
           f"solo se usa la etiqueta humana del corpus; las clasificadas suman {cobertura:.0%} de los streams y "
           f"{descartados} títulos iguales de otro artista no se emparejaron")
    registrar_etapa("limpieza", "limpio_canciones", catalogo.height, canciones.height,
                    f"{clasificadas.height} canciones con etiqueta de MiSonGyny")
    return canciones


def limpiar_contexto(registro: list[dict]) -> pl.DataFrame:
    """Completa los indicadores de contexto en los años sin medición.

    Entre dos mediciones se interpola en línea recta; antes de la primera y
    después de la última se repite la medición más cercana. Una entidad que
    nunca se midió queda nula.

    Args:
        registro (list[dict]): registro de limpieza.

    Returns:
        pl.DataFrame: una fila por entidad y año, con los indicadores completos
            salvo las entidades sin ninguna medición.
    """
    seccion("Limpieza de contexto")
    analitico = contexto().sort("cve_ent", "anio")
    limpio = analitico.with_columns(
        [pl.col(c).interpolate().forward_fill().backward_fill().over("cve_ent") for c in INDICADORES_CONTEXTO]
    )
    for columna in INDICADORES_CONTEXTO:
        medidos = analitico.drop_nulls(columna)
        anios = ", ".join(str(a) for a in medidos["anio"].unique().sort())
        rellenados = analitico[columna].null_count() - limpio[columna].null_count()
        nunca = limpio[columna].null_count()
        anotar(registro, "analitico_contexto", columna, f"solo se mide en {anios}", rellenados, analitico.height,
               "interpolar entre mediciones y repetir la más cercana en los extremos",
               "cambian despacio de un año a otro y la dimensión de contexto necesita un valor por año"
               + (f"; {nunca // analitico['anio'].n_unique()} entidades sin ninguna medición quedan nulas" if nunca else ""))
    prevalencias = [c for c in analitico.columns if c.startswith("prevalencia_")]
    anios = ", ".join(str(a) for a in analitico.drop_nulls("prevalencia_total")["anio"].unique().sort())
    anotar(registro, "analitico_contexto", "prevalencia_*", f"la ENDIREH solo se levanta en {anios}",
           sum(analitico[c].null_count() for c in prevalencias), analitico.height * len(prevalencias),
           "dejar nulo fuera de los años de levantamiento",
           "es una encuesta por levantamiento y E1 la define así; P4 la compara en su año, no como serie")
    registrar_etapa("limpieza", "limpio_contexto", analitico.height, limpio.height, "indicadores completados por año")
    return limpio


def limpiar_entidades(registro: list[dict], catalogo: pl.DataFrame) -> pl.DataFrame:
    """Prepara las 32 entidades con su clave ISO y su geometría de Natural Earth.

    Args:
        registro (list[dict]): registro de limpieza.
        catalogo (pl.DataFrame): catálogo de entidades.

    Returns:
        pl.DataFrame: columnas `cve_ent`, `entidad`, `iso_3166_2` y `geometry_wkt`.
    """
    crudo = leer("stg_naturalearth_mx")
    validas = crudo.filter(pl.col("iso_3166_2").str.contains(PATRON_ISO))
    anotar(registro, "stg_naturalearth_mx", "iso_3166_2", "fila que no es una entidad (clave MX-X01~, sin nombre)",
           crudo.height - validas.height, crudo.height, "eliminar",
           "es un polígono menor sin estado asignado en Natural Earth")
    entidades = (
        asignar_clave(validas, "name", catalogo)
        .join(catalogo.select("cve_ent", "entidad").unique(), on="cve_ent", how="left")
        .select("cve_ent", "entidad", "iso_3166_2", "geometry_wkt")
        .sort("cve_ent")
    )
    sin_clave = entidades.filter(pl.col("cve_ent").is_null())
    assert sin_clave.is_empty(), f"entidades de Natural Earth sin clave: {sin_clave['iso_3166_2'].to_list()}"
    assert entidades["cve_ent"].n_unique() == 32
    return entidades


def ejecutar_limpieza() -> pl.DataFrame:
    """Limpia las tablas del dataset analítico, las guarda y escribe el registro.

    Returns:
        pl.DataFrame: el registro de limpieza.
    """
    registro: list[dict] = []
    catalogo = construir_catalogo()
    consumo_limpio = limpiar_consumo(registro)
    tablas = {
        RUTA_LIMPIO_VIOLENCIA: limpiar_violencia(registro, catalogo),
        RUTA_LIMPIO_CONSUMO: consumo_limpio.drop("artista", "titulo", "pais_artista", "macrogenero",
                                                 "origen_genero", "etiquetas_fuente"),
        RUTA_LIMPIO_CANCIONES: limpiar_canciones(registro, consumo_limpio),
        RUTA_LIMPIO_CONTEXTO: limpiar_contexto(registro),
        RUTA_LIMPIO_ENTIDADES: limpiar_entidades(registro, catalogo),
    }
    seccion("Archivos guardados")
    for ruta, df in tablas.items():
        df.write_parquet(ruta)
        print(f"{ruta.name:<28} {df.height:>8,} filas  {df.width:>3} columnas")
    tabla = guardar_registro(registro)
    print(f"\nRegistro de limpieza: {tabla.height} problemas tratados")
    return tabla


if __name__ == "__main__":
    ejecutar_limpieza()
