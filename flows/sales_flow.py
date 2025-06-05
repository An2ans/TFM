# flows/sales_flow.py

from pathlib import Path

from prefect import flow, get_run_logger

# -------- Importar tareas y ajustes ----------
from tasks.Extract.extract_csv import extract_csv
from tasks.Quality.check_nulls import check_nulls_ge
from tasks.Transform.create_new_index import create_new_index
from tasks.Transform.transform_date import transform_date
from tasks.Transform.sort_dates import sort_dates
from tasks.Quality.check_unique import check_unique_ge

@flow(name="sales_flow")
def sales_flow(settings: dict, LOCAL_DB_PATH: str):
    """
    Sales Flow: ET parte (Extract y Transform/Quality) para procesar datos de ventas.
    """

    # 0) Extraer ajustes desde el diccionario `settings`
    SOURCE_PATH = Path(settings["SOURCE_PATH"])
    TABLE_NAME  = settings["TABLE_NAME"]
    TABLE_ID    = settings["TABLE_ID"]
    TABLE_PK    = settings["TABLE_PK"]
    QUALITY     = settings.get("Quality", {})

    logger = get_run_logger()

    # -----------------------------
    # 1) [Extract] Cargar CSV
    # -----------------------------
    df = extract_csv(str(SOURCE_PATH), ";")
    logger.info(f"✅ Etapa Extract: CSV cargado desde '{SOURCE_PATH}' con {len(df)} filas y {len(df.columns)} columnas.")

    # ----------------------------------------------------
    # 2) [Transform / Data Quality] Comprobar nulos
    # ----------------------------------------------------
    code_nulls, msg_nulls = check_nulls_ge(df)
    logger.info(f"✅ Etapa Transform/Data Quality: {msg_nulls}")
    if code_nulls != 1:
        raise RuntimeError(f"Aborting sales_flow: {msg_nulls}")

    # ----------------------------------------------------
    # 3) [Transform] Crear nuevo índice basado en Sales_DAY y PK
    # ----------------------------------------------------
    #    Crea columna "Index" como nuevo índice único
    code_idx, df, msg_idx = create_new_index(df, "Sales_DAY")
    logger.info(f"✅ Etapa Transform: {msg_idx}")
    if code_idx == 0:
        raise RuntimeError(f"Aborting sales_flow: {msg_idx}")

    # ----------------------------------------------------
    # 4) [Transform] Convertir columna de fecha a datetime
    # ----------------------------------------------------
    code_date, df, msg_date = transform_date(df, "Sales_DAY", "YYYYMMDD")
    logger.info(f"✅ Etapa Transform: {msg_date}")
    if code_date == 0:
        raise RuntimeError(f"Aborting sales_flow: {msg_date}")

    # ----------------------------------------------------
    # 5) [Transform] Ordenar por fecha (Sales_DAY) ascendente
    # ----------------------------------------------------
    code_sort, df, msg_sort = sort_dates(df, "Sales_DAY", "ASC")
    logger.info(f"✅ Etapa Transform: {msg_sort}")
    if code_sort == 0:
        raise RuntimeError(f"Aborting sales_flow: {msg_sort}")

    # ----------------------------------------------------
    # 6) [Transform / Data Quality] Comprobar unicidad de PK
    # ----------------------------------------------------
    code_unique, msg_unique = check_unique_ge(df, TABLE_PK)
    logger.info(f"✅ Etapa Transform/Data Quality: {msg_unique}")
    if code_unique == 0:
        raise RuntimeError(f"Aborting sales_flow: {msg_unique}")

    logger.info("🎉 ET (Extract+Transform) de sales_flow completado con éxito.")

    # Aquí terminaría la parte de ET; la carga (Load) vendrá después.

