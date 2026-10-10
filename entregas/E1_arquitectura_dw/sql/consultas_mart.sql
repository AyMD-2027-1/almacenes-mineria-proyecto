-- =====================================================================
-- E1 · Arquitectura del DW — Consultas del DataMart
-- Una vista por pregunta de E0, construidas sobre el esquema estrella.
-- Operaciones OLAP: ROLLUP, GROUPING, funciones de ventana y CORR.
-- =====================================================================

-- ---------------------------------------------------------------------
-- P1a. Proporción de streams misóginos por año y género musical,
--      con subtotales por año y total general (ROLLUP).
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW misoginia_mart.p1_proporcion_misogina_genero AS
SELECT
  d.anio,
  c.genero,
  GROUPING(d.anio)   AS es_total_anios,
  GROUPING(c.genero) AS es_total_generos,
  SUM(f.streams) AS streams_totales,
  SUM(IF(c.es_misogina, f.streams, 0)) AS streams_misoginos,
  SAFE_DIVIDE(SUM(IF(c.es_misogina, f.streams, 0)), SUM(f.streams)) AS proporcion_misogina,
  -- El Top 73 (F12) no trae streams: para sus filas se usa la permanencia en el chart
  SAFE_DIVIDE(SUM(IF(c.es_misogina, f.dias_en_chart, 0)), SUM(f.dias_en_chart)) AS proporcion_misogina_por_dias,
  AVG(IF(c.es_misogina, f.popularidad_promedio, NULL)) AS popularidad_promedio_misoginas
FROM misoginia_dw.fact_consumo_musical AS f
JOIN misoginia_dw.dim_fecha AS d ON d.fecha_key = f.fecha_key
JOIN misoginia_dw.dim_contenido_musical AS c ON c.contenido_key = f.contenido_key
JOIN misoginia_dw.dim_entidad AS e ON e.entidad_key = f.entidad_key
WHERE c.tipo_contenido = 'cancion'
  AND c.es_actual
  AND e.cve_ent = 0
GROUP BY ROLLUP (d.anio, c.genero);

-- ---------------------------------------------------------------------
-- P1b. Exposición a contenido misógino por entidad y año:
--      interés de búsqueda por género (Google Trends), ponderado por la
--      proporción misógina de cada género en el consumo nacional.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW misoginia_mart.p1_exposicion_misogina_entidad AS
WITH misoginia_por_genero AS (
  SELECT
    d.anio,
    c.genero,
    SAFE_DIVIDE(SUM(IF(c.es_misogina, f.streams, 0)), SUM(f.streams)) AS proporcion_misogina
  FROM misoginia_dw.fact_consumo_musical AS f
  JOIN misoginia_dw.dim_fecha AS d ON d.fecha_key = f.fecha_key
  JOIN misoginia_dw.dim_contenido_musical AS c ON c.contenido_key = f.contenido_key
  WHERE c.tipo_contenido = 'cancion' AND c.es_actual
  GROUP BY d.anio, c.genero
),
interes_por_entidad AS (
  SELECT
    d.anio,
    e.cve_ent,
    e.entidad,
    c.genero,
    AVG(SAFE_DIVIDE(f.interes_busqueda, NULLIF(f.interes_ancla, 0))) AS interes_relativo
  FROM misoginia_dw.fact_consumo_musical AS f
  JOIN misoginia_dw.dim_fecha AS d ON d.fecha_key = f.fecha_key
  JOIN misoginia_dw.dim_entidad AS e ON e.entidad_key = f.entidad_key
  JOIN misoginia_dw.dim_contenido_musical AS c ON c.contenido_key = f.contenido_key
  WHERE c.tipo_contenido = 'termino_genero'
    AND e.cve_ent BETWEEN 1 AND 32
  GROUP BY d.anio, e.cve_ent, e.entidad, c.genero
)
SELECT
  i.anio,
  i.cve_ent,
  i.entidad,
  SAFE_DIVIDE(SUM(i.interes_relativo * m.proporcion_misogina), SUM(i.interes_relativo)) AS indice_exposicion_misogina,
  RANK() OVER (PARTITION BY i.anio
               ORDER BY SAFE_DIVIDE(SUM(i.interes_relativo * m.proporcion_misogina), SUM(i.interes_relativo)) DESC) AS lugar_en_el_anio
FROM interes_por_entidad AS i
JOIN misoginia_por_genero AS m ON m.anio = i.anio AND m.genero = i.genero
GROUP BY i.anio, i.cve_ent, i.entidad;

-- ---------------------------------------------------------------------
-- P2a. Serie mensual nacional: proporción misógina del consumo frente a
--      la tasa de delitos de violencia de género por 100 mil mujeres.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW misoginia_mart.p2_serie_mensual_nacional AS
WITH musica AS (
  SELECT
    f.fecha_key,
    SAFE_DIVIDE(SUM(IF(c.es_misogina, f.streams, 0)), SUM(f.streams)) AS proporcion_misogina
  FROM misoginia_dw.fact_consumo_musical AS f
  JOIN misoginia_dw.dim_contenido_musical AS c ON c.contenido_key = f.contenido_key
  JOIN misoginia_dw.dim_entidad AS e ON e.entidad_key = f.entidad_key
  WHERE c.tipo_contenido = 'cancion' AND c.es_actual AND e.cve_ent = 0
  GROUP BY f.fecha_key
),
violencia AS (
  SELECT
    v.fecha_key,
    t.tipo_lgamvlv,
    SUM(v.conteo) AS conteo
  FROM misoginia_dw.fact_violencia AS v
  JOIN misoginia_dw.dim_tipo_violencia AS t ON t.tipo_violencia_key = v.tipo_violencia_key
  JOIN misoginia_dw.dim_fuente AS s ON s.fuente_key = v.fuente_key
  WHERE s.codigo_catalogo = 'F1'
  GROUP BY v.fecha_key, t.tipo_lgamvlv
),
poblacion AS (
  SELECT anio, SUM(poblacion_mujeres) AS mujeres
  FROM misoginia_dw.dim_entidad
  WHERE cve_ent BETWEEN 1 AND 32
  GROUP BY anio
)
SELECT
  d.fecha,
  d.anio,
  vi.tipo_lgamvlv,
  mu.proporcion_misogina,
  vi.conteo,
  SAFE_DIVIDE(vi.conteo, p.mujeres) * 100000 AS tasa_100k_mujeres
FROM violencia AS vi
JOIN musica AS mu ON mu.fecha_key = vi.fecha_key
JOIN misoginia_dw.dim_fecha AS d ON d.fecha_key = vi.fecha_key
JOIN poblacion AS p ON p.anio = d.anio;

-- ---------------------------------------------------------------------
-- P2b. Coeficiente de correlación por tipo de violencia (serie nacional).
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW misoginia_mart.p2_correlacion_nacional AS
SELECT
  tipo_lgamvlv,
  COUNT(*) AS n_meses,
  CORR(proporcion_misogina, tasa_100k_mujeres) AS correlacion_pearson
FROM misoginia_mart.p2_serie_mensual_nacional
GROUP BY tipo_lgamvlv;

-- ---------------------------------------------------------------------
-- P2c. Panel entidad-año: exposición misógina (proxy de Google Trends)
--      frente a tasas oficiales, con variables de control y contraste
--      del subregistro; correlación entre estados para cada año.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW misoginia_mart.p2_panel_entidad_anio AS
WITH violencia AS (
  SELECT
    d.anio,
    e.cve_ent,
    SUM(IF(s.codigo_catalogo = 'F1' AND t.tipo_lgamvlv = 'feminicida', v.conteo, 0)) AS feminicidios_sesnsp,
    SUM(IF(s.codigo_catalogo = 'F1', v.conteo, 0)) AS delitos_violencia_genero,
    SUM(IF(s.codigo_catalogo = 'F13', v.conteo_mujeres, 0)) AS mujeres_desaparecidas
  FROM misoginia_dw.fact_violencia AS v
  JOIN misoginia_dw.dim_fecha AS d ON d.fecha_key = v.fecha_key
  JOIN misoginia_dw.dim_entidad AS e ON e.entidad_key = v.entidad_key
  JOIN misoginia_dw.dim_tipo_violencia AS t ON t.tipo_violencia_key = v.tipo_violencia_key
  JOIN misoginia_dw.dim_fuente AS s ON s.fuente_key = v.fuente_key
  GROUP BY d.anio, e.cve_ent
),
panel AS (
  SELECT
    x.anio,
    x.cve_ent,
    x.entidad,
    x.indice_exposicion_misogina,
    SAFE_DIVIDE(vi.delitos_violencia_genero, e.poblacion_mujeres) * 100000 AS tasa_delitos_100k,
    SAFE_DIVIDE(vi.feminicidios_sesnsp, e.poblacion_mujeres) * 100000 AS tasa_feminicidio_100k,
    SAFE_DIVIDE(vi.mujeres_desaparecidas, e.poblacion_mujeres) * 100000 AS tasa_desaparicion_100k,
    SAFE_DIVIDE(e.feminicidios_prensa, NULLIF(vi.feminicidios_sesnsp, 0)) AS razon_prensa_vs_oficial,
    e.pct_pobreza,
    e.indice_impunidad,
    e.pct_usuarios_internet,
    e.pct_viviendas_internet
  FROM misoginia_mart.p1_exposicion_misogina_entidad AS x
  JOIN misoginia_dw.dim_entidad AS e ON e.cve_ent = x.cve_ent AND e.anio = x.anio
  LEFT JOIN violencia AS vi ON vi.cve_ent = x.cve_ent AND vi.anio = x.anio
)
SELECT
  *,
  CORR(indice_exposicion_misogina, tasa_delitos_100k) OVER (PARTITION BY anio) AS correlacion_entre_estados
FROM panel;

-- ---------------------------------------------------------------------
-- P3. Evolución mensual de cada categoría de contenido misógino:
--     participación en el consumo, media móvil de 12 meses y cambio
--     contra el mismo mes del año anterior (funciones de ventana).
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW misoginia_mart.p3_tendencia_categoria AS
WITH mensual AS (
  SELECT
    d.fecha,
    c.categoria_misoginia,
    SUM(f.streams) AS streams,
    AVG(f.mejor_posicion) AS posicion_promedio
  FROM misoginia_dw.fact_consumo_musical AS f
  JOIN misoginia_dw.dim_fecha AS d ON d.fecha_key = f.fecha_key
  JOIN misoginia_dw.dim_contenido_musical AS c ON c.contenido_key = f.contenido_key
  JOIN misoginia_dw.dim_entidad AS e ON e.entidad_key = f.entidad_key
  WHERE c.tipo_contenido = 'cancion' AND c.es_actual AND e.cve_ent = 0
  GROUP BY d.fecha, c.categoria_misoginia
),
participacion AS (
  SELECT
    fecha,
    categoria_misoginia,
    streams,
    posicion_promedio,
    SAFE_DIVIDE(streams, SUM(streams) OVER (PARTITION BY fecha)) AS participacion
  FROM mensual
)
SELECT
  fecha,
  categoria_misoginia,
  streams,
  posicion_promedio,
  participacion,
  AVG(participacion) OVER (PARTITION BY categoria_misoginia ORDER BY fecha
                           ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS media_movil_12m,
  participacion - LAG(participacion, 12) OVER (PARTITION BY categoria_misoginia ORDER BY fecha) AS cambio_anual
FROM participacion;

-- ---------------------------------------------------------------------
-- P4. Tipos de violencia: participación en la música misógina frente a
--     los delitos del SESNSP y la prevalencia de la ENDIREH 2021,
--     con su lugar en cada fuente (RANK).
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW misoginia_mart.p4_tipos_musica_vs_oficial AS
WITH musica AS (
  SELECT t.tipo_lgamvlv, SUM(f.streams) AS valor
  FROM misoginia_dw.fact_consumo_musical AS f
  JOIN misoginia_dw.dim_tipo_violencia AS t ON t.tipo_violencia_key = f.tipo_violencia_key
  JOIN misoginia_dw.dim_contenido_musical AS c ON c.contenido_key = f.contenido_key
  WHERE c.tipo_contenido = 'cancion' AND c.es_actual AND c.es_misogina
  GROUP BY t.tipo_lgamvlv
),
oficial AS (
  SELECT t.tipo_lgamvlv, SUM(v.conteo) AS valor
  FROM misoginia_dw.fact_violencia AS v
  JOIN misoginia_dw.dim_tipo_violencia AS t ON t.tipo_violencia_key = v.tipo_violencia_key
  JOIN misoginia_dw.dim_fuente AS s ON s.fuente_key = v.fuente_key
  WHERE s.codigo_catalogo = 'F1'
  GROUP BY t.tipo_lgamvlv
),
endireh AS (
  SELECT tipo_lgamvlv, prevalencia
  FROM misoginia_dw.dim_entidad
  UNPIVOT (prevalencia FOR tipo_lgamvlv IN (
    prevalencia_psicologica AS 'psicológica',
    prevalencia_fisica AS 'física',
    prevalencia_sexual AS 'sexual',
    prevalencia_economica_patrimonial AS 'económica'))
  WHERE cve_ent = 0 AND anio = 2021
)
SELECT
  COALESCE(m.tipo_lgamvlv, o.tipo_lgamvlv, n.tipo_lgamvlv) AS tipo_lgamvlv,
  SAFE_DIVIDE(m.valor, SUM(m.valor) OVER ()) AS participacion_musica,
  SAFE_DIVIDE(o.valor, SUM(o.valor) OVER ()) AS participacion_sesnsp,
  n.prevalencia AS prevalencia_endireh_2021,
  RANK() OVER (ORDER BY m.valor DESC) AS lugar_musica,
  RANK() OVER (ORDER BY o.valor DESC) AS lugar_sesnsp,
  RANK() OVER (ORDER BY n.prevalencia DESC) AS lugar_endireh
FROM musica AS m
FULL OUTER JOIN oficial AS o ON o.tipo_lgamvlv = m.tipo_lgamvlv
FULL OUTER JOIN endireh AS n ON n.tipo_lgamvlv = COALESCE(m.tipo_lgamvlv, o.tipo_lgamvlv);
