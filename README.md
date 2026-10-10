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
| **E1 — Arquitectura del DW** *(en curso)* | 2. Comprensión de los datos | **Fuentes de datos:** criterios de selección, proceso de búsqueda, 10 fuentes evaluadas (8 aceptadas de 6 tipos distintos) y catálogo con una página por fuente. Uso de IA y referencias. *Pendiente:* arquitectura de 3 capas en BigQuery, esquema estrella y matriz de trazabilidad. |

## Estructura del repositorio

```
.
├── entregas/
│   ├── E0_comprension_negocio/
│   │   ├── bitacora.qmd          # bitácora de la reunión con el stakeholder
│   │   ├── canvas.qmd            # Business Understanding Canvas
│   │   ├── preguntas.qmd         # preguntas de investigación priorizadas
│   │   ├── criterio_exito.qmd    # criterio de éxito en términos de negocio
│   │   ├── riesgos_supuestos.qmd # supuestos y riesgos
│   │   └── uso_ia.qmd            # documentación de uso de LLMs
│   └── E1_arquitectura_dw/
│       ├── fuentes.qmd           # criterios, búsqueda documentada y fuentes evaluadas
│       ├── fuentes/              # catálogo: una página por fuente (F1–F10)
│       ├── uso_ia.qmd            # documentación de uso de LLMs
│       ├── referencias.qmd       # referencias citadas en E1
│       └── img/                  # capturas de pantalla y vistas previas de los datos
├── assets/                       # logo y scripts del sitio
├── _quarto.yml                   # configuración y navegación del sitio (output-dir: docs)
├── index.qmd                     # página de inicio del sitio
├── theme.scss                    # estilos del sitio
├── docs/                         # sitio compilado (servido por GitHub Pages)
└── README.md
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
