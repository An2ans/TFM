# flows/sales_flow.py

from typing import Tuple
from pathlib import Path
import pandas as pd
from prefect import flow, get_run_logger

# -------- Importar tareas y ajustes ----------
from tasks.Extract.extract_csv import extract_csv
from tasks.Quality.check_nulls import check_nulls
from tasks.Transform.create_new_index import create_new_index
from tasks.Transform.transform_date import transform_date
from tasks.Transform.sort_dates import sort_dates
from tasks.Quality.check_unique import check_unique
from tasks.Quality.error_handling import error_handling
from tasks.Load.connect_local_duckdb import connect_local_duckdb
from tasks.Load.create_local_table import create_local_table
from tasks.Load.update_cloud_summary import update_cloud_summary
from tasks.Load.load_table_to_cloud import load_table_to_cloud
from tasks.Load.connect_cloud_db import connect_cloud_db


@flow(name="sales_flow")
def sales_flow(settings: dict, LOCAL_DB_PATH: str) -> Tuple[int, str]:
    """
    Sales Flow siguiendo el patrón de control por pasos:
      - Mientras task_code == 0:
          Ejecuta cada tarea en orden, actualiza (task_code, task_msg, df)
      - Post-bucle: si task_code != 0 → error_handling, si no → éxito.
    """

    logger = get_run_logger()

    # 0) Parámetros de configuración
    SOURCE_PATH = Path(settings["SOURCE_PATH"])
    TABLE_PK    = settings["TABLE_PK"]
    TABLE_ID  = settings["TABLE_ID"]
    TABLE_NAME= settings["TABLE_NAME"]

    # Variables de control
    task_code, task_msg = 0, ""
    df = pd.DataFrame()

    # Ejecución secuencial de tareas
    while task_code == 0:
        # 1) Extract CSV → (code, msg, df)
        code_01, msg_01, df = extract_csv(str(SOURCE_PATH), ";")
        task_code, task_msg = code_01, msg_01
        logger.info(msg_01)
        if task_code != 0:
            break

        # 2) Check nulls → (code, msg)
        code_02, msg_02 = check_nulls(df)
        task_code, task_msg = code_02, msg_02
        logger.info(msg_02)
        if task_code != 0:
            break

        # 3) Create new index on "Sales_DAY" → (code, msg, df)
        code_03, msg_03, df = create_new_index(df, "Sales_DAY", TABLE_PK)
        task_code, task_msg = code_03, msg_03
        logger.info(msg_03)
        if task_code != 0:
            break

        # 4) Transform date "Sales_DAY" from YYYYMMDD → (code, msg, df)
        code_04, msg_04, df = transform_date(df, "Sales_DAY", "YYYYMMDD")
        task_code, task_msg = code_04, msg_04
        logger.info(msg_04)
        if task_code != 0:
            break

        # 5) Sort dates ascending → (code, msg, df)
        code_05, msg_05, df = sort_dates(df, "Sales_DAY", "ASC")
        task_code, task_msg = code_05, msg_05
        logger.info(msg_05)
        if task_code != 0:
            break

        # 6) Check unique on TABLE_PK → (code, msg)
        code_06, msg_06 = check_unique(df, TABLE_PK)
        task_code, task_msg = code_06, msg_06
        logger.info(msg_06)

        # 7) Load: conectar a DuckDB local
        code_07, msg_07, con = connect_cloud_db()
        task_code, task_msg = code_07, msg_07
        logger.info(msg_07)
        if task_code != 0:
            break

        # 8) Load: crear tabla en DuckDB
        code_08, msg_08 = load_table_to_cloud(df, TABLE_NAME, con)
        task_code, task_msg = code_08, msg_08
        logger.info(msg_08)
        if task_code != 0:
            break

        # 9) Load: actualizar resumen
        code_09, msg_09= update_cloud_summary(df, TABLE_ID, TABLE_NAME, con)
        task_code, task_msg = code_09, msg_09
        logger.info(msg_09)
        # romper tras paso 9
        break   

    # Post-bucle: manejo de errores o éxito
    if task_code != 0:
        error_handling(task_code, task_msg, df)
        raise RuntimeError(f"Abortado sales_flow")

    else:
        return (0, f"✅ sales_flow completado! Tabla {TABLE_ID} - {TABLE_NAME} cargada con éxito en Local ")
