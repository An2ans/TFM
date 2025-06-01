import duckdb
from pathlib import Path
from prefect import task, get_run_logger

@task
def connect_local_duckdb(ruta: str) -> duckdb.DuckDBPyConnection:
    """
    Conecta (o crea si no existe) un archivo DuckDB en la ruta proporcionada.
    Devuelve la conexión para que pueda ser reutilizada en tareas posteriores.

    Parámetros:
    - ruta: Ruta completa (incluido nombre de archivo) donde se ubicará altadis_local.db.
      Ejemplo: "C:/Users/anton/Documents/TFM/altadis_local.db"

    Comportamiento:
    - Si el archivo no existe, duckdb.connect lo creará automáticamente.
    - Si hay un error al conectarse o crear el archivo, se lanza RuntimeError con mensaje claro.
    """
    logger = get_run_logger()
    try:
        db_path = Path(ruta)
        # Asegurarse de que la carpeta existe
        db_path.parent.mkdir(parents=True, exist_ok=True)
        # Conectar (o crear) el archivo DuckDB
        conn = duckdb.connect(str(db_path))
        logger.info(f"✅ Conectado a DuckDB en: {db_path}")
        return conn
    except Exception as e:
        msg = f"❌ No se pudo conectar (o crear) al archivo DuckDB en '{ruta}': {e}"
        logger.error(msg)
        raise RuntimeError(msg)
