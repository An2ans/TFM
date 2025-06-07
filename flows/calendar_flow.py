# flows/calendar_flow.py

from pathlib import Path
import pandas as pd
from prefect import flow, get_run_logger
from typing import Tuple

# Tareas necesarias
from tasks.Extract.extract_json import extract_json
from tasks.Transform.transform_date import transform_date
from tasks.Transform.create_calendar import create_calendar
from tasks.Transform.join_tables import join_tables
from tasks.Quality.error_handling import error_handling
from tasks.Quality.check_datatypes import check_datatypes

@flow(name="calendar_flow")
def calendar_flow(settings: dict) -> Tuple[int, str, pd.DataFrame]:
    """
    Genera un calendario y lo une con datos de festivos extraídos de un JSON.
    Patrón de ejecución por pasos con:
      - Mientras task_code == 0:
          * Ejecuta cada tarea en orden, actualiza task_code, task_msg, df
          * Hace logger.info(task_msg)
          * Si una tarea falla (task_code != 0) rompe el bucle
      - Tras el bucle, si task_code != 0 llama a error_handling
      - Si task_code == 0, registra éxito y devuelve df_final
    """

    logger = get_run_logger()

    # Parámetros de configuración
    JSON_PATH = Path(settings["SOURCE_PATH"])
    JSON_DF   = settings["JSON_DF"]
    FI        = settings["FECHA_INICIAL"]  # "DD-MM-YYYY"
    FF        = settings["FECHA_FINAL"]    # "DD-MM-YYYY"
    TABLE_PK  = settings["TABLE_PK"]
    TABLE_ID  = settings["TABLE_ID"]
    TABLE_NAME= settings["TABLE_NAME"]
    QUALITY= settings["QUALITY"]


    # Variables de control
    task_code, task_msg = 0, ""
    df, df_cal = pd.DataFrame(), pd.DataFrame()

    # Ejecución secuencial de tareas
    while task_code == 0:
        # 1) Extract JSON
        code_01, msg_01, df_01 = extract_json(str(JSON_PATH), JSON_DF)
        task_code, task_msg, df = code_01, msg_01, df_01
        logger.info(msg_01)
        if task_code != 0:
            break

        # 2) Transform: convertir Day a datetime
        code_02, msg_02, df_02 = transform_date(df, TABLE_PK, "DDMMYYYY")
        task_code, task_msg, df = code_02, msg_02, df_02
        logger.info(msg_02)
        if task_code != 0:
            break

        # 3) Transform: crear calendario de FI a FF
        code_03, msg_03, df_cal = create_calendar(FI, FF)
        task_code, task_msg = code_03, msg_03
        logger.info(msg_03)
        if task_code != 0:
            break

        # 4) Transform: unir ambos DataFrames por "Day"
        code_04, msg_04, df = join_tables(TABLE_PK, "FULL", df_cal, df)
        task_code, task_msg = code_04, msg_04
        logger.info(msg_04)

        #5) Quality check
        code_05, msg_05, df = check_datatypes(df, QUALITY )
        task_code, task_msg = code_05, msg_05
        break

    # Post-bucle: manejo único de errores o éxito
    if task_code != 0:
        error_handling(task_code, task_msg, df)
        raise RuntimeError(f"Abortado calendar_flow")
    else:
        logger.info("🎉 calendar_flow completado con éxito.")
        return (TABLE_ID, TABLE_NAME, df)
