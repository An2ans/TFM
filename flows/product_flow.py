# flows/product_flow.py

from pathlib import Path
from prefect import flow, get_run_logger

# Tareas actualizadas
from tasks.Extract.extract_csv import extract_csv
from tasks.Quality.check_nulls import check_nulls_ge
from tasks.Quality.check_unique import check_unique_ge
from tasks.Quality.check_datatypes import check_datatypes
from tasks.Transform.transform_col_unique import transform_col_unique
from tasks.Load.connect_local_duckdb import connect_local_duckdb
from tasks.Load.create_local_table import create_local_table
from tasks.Load.update_local_table import update_local_table
from tasks.Load.update_summary import update_summary

@flow(name="product_flow")
def product_flow(settings: dict, LOCAL_DB_PATH: str):

    # 0) Extraer 'settings' 
    SOURCE_PATH = Path(settings["SOURCE_PATH"])
    TABLE_NAME  = settings["TABLE_NAME"]
    TABLE_ID    = settings["TABLE_ID"]
    TABLE_PK    = settings["TABLE_PK"]
    QUALITY     = settings.get("Quality", {})

    logger = get_run_logger()

    # 1) Extraer CSV
    df = extract_csv(str(SOURCE_PATH), ";")

    # 2) Check Nulls
    code_nulls, msg_nulls = check_nulls_ge(df)
    logger.info(msg_nulls)

    # 3) Check Unique
    code_unique, msg_unique = check_unique_ge(df, TABLE_PK)
    logger.info(msg_unique)
    if code_unique == 0:
        code_force, df_mod, msg_force = transform_col_unique(df, TABLE_PK)
        logger.info(msg_force)
        if code_force == 0:
            raise RuntimeError("Aborting product_flow: " + msg_force)
        df = df_mod
    
    
    # 5) Quality check antes de insertar datos
    if QUALITY:
        code_dt, df_corr, msg_dt = check_datatypes(df, QUALITY)
        logger.info(msg_dt)
        if code_dt == 0:
            raise RuntimeError("Aborting product_flow: " + msg_dt)
        df = df_corr
    else:
        logger.warning("⚠️ No hay diccionario 'Quality' en settings; omitiendo check_datatypes.")

    # 6) Conectar a DuckDB
    code_con, msg_con, con = connect_local_duckdb(str(LOCAL_DB_PATH))
    logger.info(msg_con)
    if code_con == 0 or con is None:
        raise RuntimeError("Aborting product_flow: " + msg_con)

    # 7) Crear o actualizar tabla principal
    code_tbl, msg_tbl, head_df = create_local_table(df, TABLE_NAME, con)
    if code_tbl == 2:
        # La tabla ya existe: actualizamos sus datos
        logger.info(msg_tbl)
        code_upd, msg_upd = update_local_table(df, TABLE_NAME, con)
        logger.info(msg_upd)
        if code_upd == 0:
            raise RuntimeError("Aborting product_flow: " + msg_upd)
    elif code_tbl == 1:
        # Se creó por primera vez
        logger.info(msg_tbl)
        # Mostrar primeras 5 filas
        logger.info(f"Primeras 5 filas de '{TABLE_NAME}':\n{head_df}")
    else:
        # code_tbl == 0: error creando tabla
        raise RuntimeError("Aborting product_flow: " + msg_tbl)

    # 8) Actualizar summary
    code_sum, msg_sum = update_summary(df, TABLE_ID, TABLE_NAME, con)
    logger.info(msg_sum)
    if code_sum == 0:
        raise RuntimeError("Aborting product_flow: " + msg_sum)

    logger.info("🎉 product_flow completado con éxito.")
