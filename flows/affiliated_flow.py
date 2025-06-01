# flows/affiliated_flow.py

import os
from pathlib import Path

from prefect import flow, task, get_run_logger

# Importar tareas custom
from tasks.create_workqueue import create_work_queue
from tasks.import_csv import import_csv
from tasks.check_nulls import check_nulls
from tasks.check_unique import check_unique
from tasks.force_unique import force_unique
from tasks.connect_local_duckdb import connect_local_duckdb
from tasks.create_local_table import create_local_table

# Constantes para rutas y nombres
AFFILIATED_PATH = Path(
    r"C:\Users\anton\OneDrive - UNIR\Equipo\TFM2\DATA\Affiliated_Outlets.csv"
)
PK_COLUMN = "Affiliated_Code"
DB_PATH = Path.cwd() / "altadis_local.db"  # Archivo DuckDB en la raíz del proyecto
TABLE_NAME = "Affiliated_Outlets"

@flow(name="affiliated_flow")
def affiliated_flow():
    logger = get_run_logger()

    # 1) Crear o verificar Work Queue "default"
    queue_name = create_work_queue("default")

    # 2) Leer CSV de afiliados
    #    import_csv lanzará FileNotFoundError si AFFILIATED_PATH no existe, deteniendo el flow.
    df_aff = import_csv(str(AFFILIATED_PATH), ";")
    logger.info(f"✅ CSV leído: {df_aff.shape[0]} filas, {df_aff.shape[1]} columnas")

    # 3) Comprobar nulos
    nulls = check_nulls(df_aff)
    if nulls:
        logger.warning(f"⚠️ Columnas con nulos: {nulls}")
    else:
        logger.info("✅ No se encontraron nulos")

    # 4) Comprobar unicidad de la columna PK_COLUMN
    is_unique = check_unique(df_aff, PK_COLUMN)
    if not is_unique:
        logger.warning(f"⚠️ Valores duplicados detectados en '{PK_COLUMN}', forzando unicidad")
        df_aff = force_unique(df_aff, PK_COLUMN)
        logger.info(f"✅ Unicidad forzada en '{PK_COLUMN}'")
    else:
        logger.info(f"✅ Todos los valores de '{PK_COLUMN}' son únicos")

    # 5) Conectar a DuckDB local
    con = connect_local_duckdb(str(DB_PATH))

    # 6) Crear y poblar tabla en DuckDB
    create_local_table(df_aff, TABLE_NAME, con)
    logger.info(f"🎉 Tabla '{TABLE_NAME}' lista en {DB_PATH}")

if __name__ == "__main__":
    affiliated_flow()
