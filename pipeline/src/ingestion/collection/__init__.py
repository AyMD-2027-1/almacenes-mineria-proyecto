"""Scripts con los que se recopilaron los datos que E2 agregó a staging.

No forman parte de cada corrida del pipeline: se ejecutaron una vez para
descargar Kworb, MusicBrainz, la ENDIREH, la ENDUTIH y Google Trends, y
`carga_staging` subió los archivos resultantes a BigQuery.
"""
