# Proyecto Integrador — Almacenes y Minería de Datos

Facultad de Ciencias, UNAM. Proyecto Integrador construido de forma incremental sobre la metodología
**CRISP-DM**, entregado como sitio web con [Quarto](https://quarto.org/) y desplegado con GitHub
Pages.

**Sitio publicado:** <https://aymd-2027-1.github.io/almacenes-mineria-proyecto/>

## Integrantes

- Flores Morán Julieta Melina
- García Landa Brenda Yareli
- Jiménez Rivera Emiliano Kaleb
- Zarco Romero José Antonio

## Entregas

| Entrega | Fase CRISP-DM | Contenido |
|---|---|---|
| **E0 — Comprensión del negocio** | 1. Comprensión del negocio | Bitácora del stakeholder, Business Understanding Canvas, preguntas de investigación, criterio de éxito, riesgos y supuestos, uso de IA. |
| **E1 — Arquitectura del DW** | 2. Comprensión de los datos | **Fuentes de datos:** criterios de selección, búsqueda documentada, 27 fuentes evaluadas (16 aceptadas, de 12 tipos distintos, y 11 descartadas) y catálogo con una página por fuente. **Arquitectura** de tres capas con enfoque medallion en BigQuery, evidencia de las cargas y consultas del DataMart. **Esquema estrella** con dos hechos y cinco dimensiones, diagrama Mermaid y DDL. Matriz de trazabilidad, pivote (con E0 v2), glosario, uso de IA y referencias. |

## Estructura del repositorio

```
.
├── entregas/
│   ├── E0_comprension_negocio/   # entrega anterior
│   │   ├── bitacora.qmd          # bitácora de la reunión con el stakeholder
│   │   ├── canvas.qmd            # Business Understanding Canvas
│   │   ├── preguntas.qmd         # preguntas de investigación priorizadas
│   │   ├── criterio_exito.qmd    # criterio de éxito en términos de negocio
│   │   ├── riesgos_supuestos.qmd # supuestos y riesgos
│   │   ├── uso_ia.qmd            # documentación de uso de LLMs
│   │   └── e0_v2.qmd             # E0 v2: resumen de lo ajustado, con enlace a pivote
│   └── E1_arquitectura_dw/
│       ├── fuentes.qmd           # búsqueda documentada, catálogo e integración
│       ├── fuentes/              # una ficha por fuente (F1–F19)
│       ├── arquitectura.qmd      # 3 capas, evidencia BigQuery, consultas del mart
│       ├── estrella.qmd          # grano, diagrama Mermaid y DDL
│       ├── trazabilidad.qmd      # matriz pregunta-métrica-dimensión-fuente
│       ├── pivote.qmd            # solo si hubo pivote; incluye E0 v2
│       ├── uso_ia.qmd            # documentación de uso de LLMs
│       ├── glosario.qmd          # siglas y términos
│       ├── referencias.qmd       # referencias citadas en E1
│       ├── sql/                  # ddl_dw.sql, consultas_mart.sql
│       ├── datos/                # scripts de extracción y preparación de staging (los CSV no se suben)
│       └── img/                  # capturas de pantalla de BigQuery
├── assets/                       # logo y scripts del sitio
├── _quarto.yml                   # agregar E1 a la navegación
├── index.qmd                     # página de inicio del sitio
├── theme.scss                    # estilos del sitio
├── docs/                         # sitio compilado (GitHub Pages)
└── README.md                     # URL del sitio desplegado
```

Este repositorio y su sitio Quarto se reutilizan durante todo el semestre: cada entrega agrega una
nueva carpeta en `entregas/` y sus páginas correspondientes al sitio.

## Cómo previsualizar el sitio localmente

Requiere tener [Quarto](https://quarto.org/docs/get-started/) instalado.

```bash
quarto preview
```

## Cómo compilar el sitio

```bash
quarto render
```

Esto genera el sitio estático en `docs/`, que es la carpeta que sirve GitHub Pages.

## Despliegue en GitHub Pages

1. Hacer `quarto render` y confirmar que `docs/` se generó correctamente.
2. Subir el repositorio a GitHub (`main` como rama por defecto).
3. En GitHub: **Settings -> Pages -> Branch: `main` / `docs`**.
4. Pegar la URL pública resultante en este README, en la sección "Sitio publicado" de arriba.
