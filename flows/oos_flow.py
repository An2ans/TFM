# flows/oos_flow.py

from pathlib import Path
import pandas as pd
from prefect import flow, get_run_logger
from typing import Tuple

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


@flow(name="oos_flow")
def oos_flow(settings: dict, LOCAL_DB_PATH:str) -> Tuple[int, str]:
    """
    oos_flow: idéntico a delivery_flow, pero para Out-Of-Stock data.
    """
    logger = get_run_logger()

    # 0) Parámetros
    SOURCE_PATH = Path(settings["SOURCE_PATH"])
    TABLE_PK    = settings["TABLE_PK"]
    TABLE_ID    = settings["TABLE_ID"]
    TABLE_NAME  = settings["TABLE_NAME"]

    # Estado inicial
    task_code, task_msg = 0, ""
    df: pd.DataFrame = pd.DataFrame()

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

        # 3) Create new index on "OoS_DAY"
        code_03, msg_03, df = create_new_index(df, "OoS_DAY", TABLE_PK)
        task_code, task_msg = code_03, msg_03
        logger.info(msg_03)
        if task_code != 0:
            break

        # 4) Transform date "OoS_DAY" from YYYYMMDD
        code_04, msg_04, df = transform_date(df, "OoS_DAY", "YYYYMMDD")
        task_code, task_msg = code_04, msg_04
        logger.info(msg_04)
        if task_code != 0:
            break

        # 5) Sort dates ascending
        code_05, msg_05, df = sort_dates(df, "OoS_DAY", "ASC")
        task_code, task_msg = code_05, msg_05
        logger.info(msg_05)
        if task_code != 0:
            break

        # 6) Check unique on TABLE_PK
        code_06, msg_06 = check_unique(df, TABLE_PK)
        task_code, task_msg = code_06, msg_06
        logger.info(msg_06)

        # 7) Conectar DuckDB
        code_07, msg_07, con = connect_cloud_db()
        task_code, task_msg = code_07, msg_07
        logger.info(msg_07)
        if task_code != 0:
            break

        # 8) Crear tabla
        code_08, msg_08 = load_table_to_cloud(df, TABLE_NAME, con)
        task_code, task_msg = code_08, msg_08
        logger.info(msg_08)
        if task_code != 0:
            break

        # 9) Update summary
        code_09, msg_09 = update_cloud_summary(df, TABLE_ID, TABLE_NAME, con)
        task_code, task_msg = code_09, msg_09
        logger.info(msg_09)
        break


    if task_code != 0:
        error_handling(task_code, task_msg, df)
        raise RuntimeError(f"Abortado oos_flow")
    else:
        return (0, f"✅ oos_flow completado! Tabla {TABLE_ID} - {TABLE_NAME} cargada con éxito en Local ")
