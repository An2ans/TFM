# flows/delivery_flow.py

from pathlib import Path
import pandas as pd
from prefect import flow, get_run_logger
from typing import Tuple, Any

# --- Importamos las mismas tareas que en sales_flow ---
from tasks.Extract.extract_csv import extract_csv
from tasks.Quality.check_nulls import check_nulls
from tasks.Transform.create_new_index import create_new_index
from tasks.Transform.transform_date import transform_date
from tasks.Transform.sort_dates import sort_dates
from tasks.Quality.check_unique import check_unique
from tasks.Quality.error_handling import error_handling

@flow(name="delivery_flow")
def delivery_flow(settings: dict) -> Tuple[int, str, pd.DataFrame]:
    """
    delivery_flow: idéntico a sales_flow, pero para los datos de delivery.
    Retorna (TABLE_ID, TABLE_NAME, df) en caso de éxito, 
    o dispara error_handling y no retorna df en caso de fallo.
    """
    logger = get_run_logger()

    # 0) Parámetros
    SOURCE_PATH = Path(settings["SOURCE_PATH"])
    TABLE_PK    = settings["TABLE_PK"]
    TABLE_ID    = settings["TABLE_ID"]
    TABLE_NAME  = settings["TABLE_NAME"]

    # Control de errores y df
    task_code, task_msg = 0, ""
    df: pd.DataFrame = pd.DataFrame()

    # 1–6) Mismo patrón que sales_flow
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

        # 3) Create new index on TABLE_PK
        code_03, msg_03, df = create_new_index(df, "Delivery_DAY", TABLE_PK)
        task_code, task_msg = code_03, msg_03
        logger.info(msg_03)
        if task_code != 0:
            break

        # 4) Transform date
        code_04, msg_04, df = transform_date(df, "Delivery_DAY", "YYYYMMDD")
        task_code, task_msg = code_04, msg_04
        logger.info(msg_04)
        if task_code != 0:
            break

        # 5) Sort dates ascending
        code_05, msg_05, df = sort_dates(df, "Delivery_DAY", "ASC")
        task_code, task_msg = code_05, msg_05
        logger.info(msg_05)
        if task_code != 0:
            break

        # 6) Check unique on TABLE_PK
        code_06, msg_06 = check_unique(df, TABLE_PK)
        task_code, task_msg = code_06, msg_06
        logger.info(msg_06)
        # check_unique never aborts if code==0, so we break anyway
        break

    # Post-bucle: error o éxito
    if task_code != 0:
        error_handling(task_code, task_msg, df)
        raise RuntimeError(f"Abortado delivery_flow")
    else:
        logger.info("🎉 delivery_flow completado con éxito.")
        return TABLE_ID, TABLE_NAME, df
