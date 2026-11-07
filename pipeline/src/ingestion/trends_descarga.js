/*
 * Descarga de Google Trends usada en E2: interés por estado de cada macrogénero y de
 * cada artista del top 50 de cada año, una descarga por año de 2015 a 2025.
 *
 * Se ejecuta en la consola del navegador con una pestaña abierta en
 * https://trends.google.com/trends/explore?geo=MX&hl=es (la sesión del sitio es la
 * que autoriza las consultas). Usa los mismos servicios que el botón "Descargar CSV"
 * de la vista "Interés por subregión" y obtiene los mismos valores.
 *
 * Al terminar, `exportarCSV` descarga un archivo con las columnas de las tablas de
 * staging (stg_trends_macrogenero_anio y stg_trends_artista_anio).
 *
 * Uso:
 *   1. Pegar este archivo en la consola.
 *   2. Para géneros:  await descargarTodo(GENEROS_POR_ANIO, TEMAS_GENERO); exportarCSV("macrogenero", ...)
 *   3. Para artistas: await buscarTemas(Object.keys(ARTISTAS_POR_ANIO)); luego
 *      await descargarTodo(ARTISTAS_POR_ANIO, temas); exportarCSV("artista", ...)
 *      ARTISTAS_POR_ANIO es el top 50 de cada año de la muestra (artista: [años]).
 */

// Tema de Google Trends de cada macrogénero (identificador del Knowledge Graph).
const TEMAS_GENERO = {
  "regional mexicano": "/m/04ggcc9", // Música regional mexicana
  "urbano latino": "/m/0233qs",      // Reguetón
  "hip hop y rap": "/m/0glt670",     // Música hip hop
  "cumbia y tropical": "/m/02ccj9",  // Cumbia
  "rock": "/m/06by7",                // Rock
  "electrónica": "/m/02lkt",         // Música electrónica
  "pop": "/m/064t9",                 // Pop
};

const ANIOS = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025];
const GENEROS_POR_ANIO = Object.fromEntries(Object.keys(TEMAS_GENERO).map((g) => [g, ANIOS]));

// Correcciones manuales del tema de un artista cuando el autocompletado elige otro.
const TEMAS_CORREGIDOS = {
  "Zion & Lennox": { keyword: "/m/01qfzz4", titulo: "Zion y Lennox", tipo: "Banda" },
  "Bobby Pulido": { keyword: "/m/01mgyt0", titulo: "Bobby Pulido", tipo: "Cantante" },
};

// Tipos de resultado del autocompletado que corresponden a un artista.
const TIPO_ARTISTA = /cantante|grupo|banda|rapero|músic|music|artista|dúo|duo|compositor|productor|dj|conjunto|agrupación|cantautor|singer|rapper|band|trío|trio|intérprete/i;

const PAUSA_ENTRE_PETICIONES_MS = 1500;
const PAUSA_ENTRE_DESCARGAS_MS = 2500;
const PAUSA_TRAS_ERROR_MS = 30000;

const esperar = (ms) => new Promise((resolver) => setTimeout(resolver, ms));

// Las respuestas de Trends empiezan con caracteres de protección antes del JSON.
async function pedirJSON(url) {
  const respuesta = await fetch(url);
  const texto = await respuesta.text();
  if (!respuesta.ok) throw new Error(String(respuesta.status));
  return JSON.parse(texto.slice(texto.indexOf("{")));
}

// Busca el tema de cada artista con el autocompletado; si no hay tema de tipo artista,
// usa el nombre como término de búsqueda.
async function buscarTemas(artistas) {
  const temas = {};
  for (const nombre of artistas) {
    const consulta = nombre
      .replace(/ de (Rene Camacho|Sergio Lizárraga)$/i, "")
      .replace(/ y su Norteño Banda$/i, "")
      .replace(/ Y Su Esencia$/i, "");
    const datos = await pedirJSON(`/trends/api/autocomplete/${encodeURIComponent(consulta)}?hl=es&tz=360`);
    const tema = datos.default.topics.find((t) => TIPO_ARTISTA.test(t.type));
    temas[nombre] = TEMAS_CORREGIDOS[nombre]
      || (tema ? { keyword: tema.mid, titulo: tema.title, tipo: tema.type } : { keyword: consulta, titulo: consulta, tipo: "término" });
    await esperar(900);
  }
  return temas;
}

// Interés por estado de un tema en un año: primero se pide la exploración, que
// devuelve el token del mapa, y después los datos del mapa.
async function descargarGeo(keyword, anio) {
  const peticion = { comparisonItem: [{ keyword, geo: "MX", time: `${anio}-01-01 ${anio}-12-31` }], category: 0, property: "" };
  const exploracion = await pedirJSON(`/trends/api/explore?hl=es&tz=360&req=${encodeURIComponent(JSON.stringify(peticion))}`);
  const mapa = exploracion.widgets.find((w) => w.id === "GEO_MAP");
  await esperar(PAUSA_ENTRE_PETICIONES_MS);
  const datos = await pedirJSON(
    `/trends/api/widgetdata/comparedgeo?hl=es&tz=360&req=${encodeURIComponent(JSON.stringify(mapa.request))}&token=${mapa.token}`
  );
  return datos.default.geoMapData.map((x) => [x.geoCode, x.geoName, x.value[0], x.hasData[0]]);
}

// Descarga cada tema en cada año de su lista, con hasta tres intentos por descarga.
async function descargarTodo(aniosPorTema, temas) {
  const resultados = {};
  for (const [nombre, anios] of Object.entries(aniosPorTema)) {
    const keyword = typeof temas[nombre] === "string" ? temas[nombre] : temas[nombre].keyword;
    for (const anio of anios) {
      for (let intento = 0; intento < 3; intento++) {
        try {
          resultados[`${nombre}|${anio}`] = await descargarGeo(keyword, anio);
          break;
        } catch (error) {
          if (intento === 2) console.log(`Falló ${nombre} ${anio}: ${error.message}`);
          await esperar(PAUSA_TRAS_ERROR_MS);
        }
      }
      await esperar(PAUSA_ENTRE_DESCARGAS_MS);
    }
  }
  return resultados;
}

// Arma el CSV con las columnas de staging y lo descarga.
function exportarCSV(nivel, resultados, temas, archivo) {
  const hoy = new Date().toISOString().slice(0, 10);
  const comillas = (s) => `"${String(s).replace(/"/g, '""')}"`;
  const encabezado = nivel === "artista"
    ? "artista,tema_trends,titulo_tema,tipo_tema,anio,geo_code,region_trends,interes,tiene_dato,fecha_descarga"
    : "macrogenero,tema_mid,anio,geo_code,region_trends,interes,tiene_dato,fecha_descarga";
  const lineas = [encabezado];
  for (const [clave, filas] of Object.entries(resultados)) {
    const corte = clave.lastIndexOf("|");
    const nombre = clave.slice(0, corte);
    const anio = clave.slice(corte + 1);
    for (const [codigo, region, interes, tieneDato] of filas) {
      if (nivel === "artista") {
        const t = temas[nombre];
        lineas.push([comillas(nombre), comillas(t.keyword), comillas(t.titulo), comillas(t.tipo), anio, codigo,
          comillas(region), interes, tieneDato, hoy].join(","));
      } else {
        lineas.push([nombre, temas[nombre], anio, codigo, comillas(region), interes, tieneDato, hoy].join(","));
      }
    }
  }
  const enlace = document.createElement("a");
  enlace.href = URL.createObjectURL(new Blob([lineas.join("\n") + "\n"], { type: "text/csv;charset=utf-8" }));
  enlace.download = archivo;
  document.body.appendChild(enlace);
  enlace.click();
  enlace.remove();
}
