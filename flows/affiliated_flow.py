# flows/affiliated_flow.py

from pathlib import Path
import pandas as pd
from prefect import flow, get_run_logger
from dotenv import load_dotenv

# Importar las tareas usadas
from tasks.Extract.extract_csv import extract_csv
from tasks.Transform.rename_col import rename_col
from tasks.Transform.group_by import group_by
from tasks.Transform.join_tables import join_tables
from tasks.Transform.transform_cat_to_num import transform_cat_to_num
from tasks.Quality.check_nulls import check_nulls
from tasks.Quality.check_unique import check_unique
from tasks.Quality.check_datatypes import check_datatypes
from tasks.Load.connect_local_duckdb import connect_local_duckdb
from tasks.Load.create_local_table import create_local_table
from tasks.Load.update_summary import update_summary
from tasks.Quality.error_handling import error_handling
from tasks.Load.load_table_to_cloud import load_table_to_cloud
from tasks.Load.connect_cloud_db import connect_cloud_db
from tasks.Load.update_cloud_summary import update_cloud_summary

@flow(name="affiliated_flow")
def affiliated_flow(settings: dict):
    """
    Flujo para procesar la tabla de 'affiliated':
    Pasos:
      1) extract_csv(SOURCE_PATH, ";")
      2) rename_col(df, {...})
      3) extract_csv(PC_PATH, ";")
      4) group_by(df_cp, "cp", AGG_MAP)
      5) join_tables("cp", "FULL", df, df_cp)
      6) transform_cat_to_num(df, "Location")
      7) transform_cat_to_num(df, "Tam_m2", Tam_map)
      8) check_nulls(df)
      9) check_unique(df, TABLE_PK)
     10) check_datatypes(df, QUALITY)
     11) connect_local_duckdb(LOCAL_DB_PATH)
     12) create_local_table(df, TABLE_NAME, con)
     13) update_summary(df, TABLE_ID, TABLE_NAME, con)
    Si en algún paso code != 0, se aborta y se llama a error_handling.
    Si todo OK, al final devuelve (TABLE_ID, TABLE_NAME, df).
    """

    # Incorporamos las variables de entorno .env

    logger = get_run_logger()

    # 0) Extraer de settings
    SOURCE_PATH = Path(settings["SOURCE_PATH"])
    PC_PATH     = Path(settings["PC_PATH"])
    TABLE_NAME  = settings["TABLE_NAME"]
    TABLE_ID    = settings["TABLE_ID"]
    TABLE_PK    = settings["TABLE_PK"]
    QUALITY     = settings.get("QUALITY", {})
    AGG_MAP     = settings.get("AGG_MAP", {})   # dict para group_by en df_cp
    
    
    Tam_map     = {              
        "<2m2": 1,
        "2-5m2":2,
        "5-10m2":3,
        "10-20m2":4,
        "20-30m2":5,
        ">20m2":6,
        ">30m2":7,
        "N.D.":0
    } # dict opcional para mapear 'Tam_m2' con los valores que queremos 

    # Variables de control
    task_code, task_msg = 0, ""
    df, df_cp = pd.DataFrame(), pd.DataFrame()
    con = None

    # Ejecución secuencial de tareas
    while task_code == 0:
        # 1) Extract CSV principal
        code_01, msg_01, df = extract_csv(str(SOURCE_PATH), ";")
        task_code, task_msg = code_01, msg_01
        logger.info(msg_01)
        if task_code != 0:
            break

        # 2) Renombrar columnas del df principal
        #    Ajusta el mapeo según lo indicado:
        rename_map = {
            "Affiliated_NAME": "Affiliated_Name",
            "POSTALCODE": "cp",
            "Management_Cluster": "Cluster"
        }
        code_02, msg_02, df = rename_col(df, rename_map)
        task_code, task_msg = code_02, msg_02
        logger.info(msg_02)
        if task_code != 0:
            break

        # 3) Extract CSV de códigos postales
        code_03, msg_03, df_cp = extract_csv(str(PC_PATH), ";")
        task_code, task_msg = code_03, msg_03
        logger.info(msg_03)
        if task_code != 0:
            break

        # 4) Agrupar df_cp por "cp" con AGG_MAP
        code_04, msg_04, df_cp = group_by(df_cp, "cp", AGG_MAP)
        task_code, task_msg = code_04, msg_04
        logger.info(msg_04)
        if task_code != 0:
            break

        # 5) Unir df principal con df_cp agrupado por "cp"
        code_05, msg_05, df = join_tables("cp", "LEFT", df, df_cp)
        task_code, task_msg = code_05, msg_05
        logger.info(msg_05)
        if task_code != 0:
            break

        # 6) transform_cat_to_num sobre "Location"
        code_06, msg_06, df = transform_cat_to_num(df, "Location")
        task_code, task_msg = code_06, msg_06
        logger.info(msg_06)
        if task_code != 0:
            break

        # 7) transform_cat_to_num sobre "Tam_m2", usando Tam_map si existe
        if Tam_map is not None:
            code_07, msg_07, df = transform_cat_to_num(df, "Tam_m2", Tam_map)
        else:
            code_07, msg_07, df = transform_cat_to_num(df, "Tam_m2")
        task_code, task_msg = code_07, msg_07
        logger.info(msg_07)
        if task_code != 0:
            break

        # 8) check_nulls en df
        code_08, msg_08 = check_nulls(df)
        task_code, task_msg = code_08, msg_08
        logger.info(msg_08)
        if task_code != 0:
            break

        # 9) check_unique en la PK
        code_09, msg_09 = check_unique(df, TABLE_PK)
        task_code, task_msg = code_09, msg_09
        logger.info(msg_09)
        if task_code != 0:
            break

        # 10) check_datatypes según QUALITY (si hay QUALITY)
        if QUALITY:
            # según convención: check_datatypes devuelve (code, df_mod, msg)
            code_10, msg_10, df = check_datatypes(df, QUALITY)
            task_code, task_msg = code_10, msg_10
            if task_code != 0:
                # error en datatypes
                logger.info(msg_10)
                break
        else:
            logger.warning("⚠️ No hay diccionario 'Quality' en settings; omitiendo check_datatypes.")
        # Si error en datatypes, task_code ya != 0 y sale
        if task_code != 0:
            break

        # 11) Conectamos con MotherDuck (Cloud DW)
        logger.info("▶️ Intentando conectar a DuckDB Cloud...")
        code_11, msg_11, con = connect_cloud_db()
        task_code, task_msg = code_11, msg_11
        logger.info(msg_11)
        if code_11 != 0 or con is None:
            break

        # 12) Creamos (o actualizamos) la tabla en el cloud        
        logger.info(f"▶️ Intentando cargar tabla '{TABLE_NAME}' al cloud...")
        code_12, msg_12, load_report =load_table_to_cloud(df, TABLE_NAME, con)
        task_code, task_msg = code_12, msg_12
        logger.info(msg_12)
        if task_code != 0:
            break

        # 13) Creamos (o actualizamos) las tablas summary en el cloud        
        logger.info(f"▶️ Intentando cargar tabla '{TABLE_NAME}' al cloud...")
        code_13, msg_13 =update_cloud_summary(load_report, TABLE_ID, TABLE_NAME, con)
        task_code, task_msg = code_13, msg_13
        logger.info(msg_13)
        if task_code != 0:
            break

        # Si hemos llegado aquí, todo OK; salimos del while
        break

    # Post-bucle: manejar error o éxito
    if task_code != 0:
        # Llamar a error_handling con df para contexto
        error_handling(task_code, task_msg, df)
        # No devolvemos nada: el flow termina en estado Failed implícito
        return
    else:
        return (0, f"✅ affiliated_flow completado! Tabla {TABLE_ID} - {TABLE_NAME} cargada con éxito en Local ")


