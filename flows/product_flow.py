# flows/product_flow.py

from pathlib import Path
import pandas as pd
from prefect import flow, get_run_logger

# Tareas
from tasks.Extract.extract_csv import extract_csv
from tasks.Quality.check_nulls import check_nulls
from tasks.Quality.check_unique import check_unique
from tasks.Transform.transform_col_unique import transform_col_unique
from tasks.Quality.check_datatypes import check_datatypes
from tasks.Load.connect_local_duckdb import connect_local_duckdb
from tasks.Load.create_local_table import create_local_table
from tasks.Load.update_local_table import update_local_table
from tasks.Load.update_cloud_summary import update_cloud_summary
from tasks.Quality.error_handling import error_handling
from tasks.Load.load_table_to_cloud import load_table_to_cloud
from tasks.Load.connect_cloud_db import connect_cloud_db


@flow(name="product_flow")
def product_flow(settings: dict, LOCAL_DB_PATH: str) -> pd.DataFrame:
    """
    ET + Load para productos:
      - Mientras task_code == 0:
          Ejecuta cada tarea, guarda outputs en code_##, msg_##, df_## (si aplica)
      - Post-bucle: si task_code != 0 → error_handling + RuntimeError; si no → éxito.
    """

    logger = get_run_logger()

    # Parámetros
    SOURCE_PATH = Path(settings["SOURCE_PATH"])
    TABLE_NAME  = settings["TABLE_NAME"]
    TABLE_ID    = settings["TABLE_ID"]
    TABLE_PK    = settings["TABLE_PK"]
    QUALITY     = settings.get("Quality", {})

    # Control
    task_code, task_msg = 0, ""
    df: pd.DataFrame = pd.DataFrame()
    con = None

    # Secuencia de pasos
    while task_code == 0:
        # 1) Extract CSV
        code_01, msg_01, df = extract_csv(str(SOURCE_PATH), ";")
        task_code, task_msg = code_01, msg_01
        logger.info(msg_01)
        if task_code != 0:
            break

        # 2) Check nulls
        code_02, msg_02 = check_nulls(df)
        task_code, task_msg = code_02, msg_02
        logger.info(msg_02)
        if task_code != 0:
            break

        # 3) Check unique + transform if dup
        code_03, msg_03 = check_unique(df, TABLE_PK)
        task_code, task_msg = code_03, msg_03
        logger.info(msg_03)
        if task_code != 0:
            break

        # si hay duplicados, fuerza renombrado
        if "dup" in msg_03.lower():  # tu lógica para detectar necesidad
            code_04, msg_04, df = transform_col_unique(df, TABLE_PK)
            task_code, task_msg = code_04, msg_04
            logger.info(msg_04)
            if task_code != 0:
                break

        # 4) Check datatypes si corresponde
        if QUALITY:
            code_05, msg_05, df = check_datatypes(df, QUALITY)
            task_code, task_msg = code_05, msg_05
            logger.info(msg_05)
            if task_code != 0:
                break
        else:
            logger.warning("⚠️ No hay 'Quality' en settings; omitiendo check_datatypes.")

        # 5) Conectar DuckDB
        code_06, msg_06, con = connect_cloud_db()
        task_code, task_msg = code_06, msg_06
        logger.info(msg_06)
        if task_code != 0 or con is None:
            break

        # 6) Crear o actualizar tabla
        code_07, msg_07 = load_table_to_cloud(df, TABLE_NAME, con)
        task_code, task_msg = code_07, msg_07
        if task_code != 0:
            break

        # 7) Actualizar summary
        code_09, msg_09 = update_cloud_summary(df, TABLE_ID, TABLE_NAME, con)
        task_code, task_msg = code_09, msg_09
        logger.info(msg_09)
        if task_code != 0:
            break

        # todo OK: salir del bucle
        break

    # Manejo final
    if task_code != 0:
        # registra y falla
        error_handling(task_code, task_msg, df)
        raise RuntimeError(f"Aborting product_flow: {task_msg}")

    else:
        return (0, f"✅ product_flow completado! Tabla {TABLE_ID} - {TABLE_NAME} cargada con éxito en Local ")
