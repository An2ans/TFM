# ETL.py

import os , json
from dotenv import load_dotenv
from pathlib import Path
from prefect import flow, get_run_logger
from tasks.Load.connect_prefect_workpool import connect_prefect_workpool
from tasks.Load.finish_ETL import finish_ETL
from flows.affiliated_flow import affiliated_flow
from flows.product_flow import product_flow

# Incorporamos las variables de entorno .env
load_dotenv()

# ---------------------------------------------------
#  CARGAR JSON Y EXTRAER LA SECCIÓN "global" y "flows"
# ---------------------------------------------------
BASE_DIR      = Path(__file__).parent
SETTINGS_PATH = BASE_DIR / "ETL_settings.json"

with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
    settings = json.load(f)["settings"]

global_settings = settings["global"]
flow_settings = settings["flows"]

LOCAL_DB_PATH = global_settings["LOCAL_DB_PATH"]

@flow(name="etl_orchestrator")
def etl_orchestrator():
    logger = get_run_logger()

    # 1) Conectar al Work Pool
    code_pool, msg_pool = connect_prefect_workpool()
    logger.info(msg_pool)
    if code_pool == 0:
        raise RuntimeError("Aborting ETL: " + msg_pool)

    # 2) Ejecutar subflow 'affiliated_flow'con sus settings
    logger.info("▶️ Iniciando `affiliated_flow` …")
    affiliated_flow(flow_settings["affiliated"], LOCAL_DB_PATH)
    logger.info("✅ `affiliated_flow` finalizado.")

    # 3) Ejecutar subflow 'product_flow'con sus settings
    logger.info("▶️ Iniciando `product_flow` …")
    product_flow(flow_settings["product"], LOCAL_DB_PATH)
    logger.info("✅ `product_flow` finalizado.")


    # 4) Limpiar caché
    code_fin, msg_fin = finish_ETL()
    logger.info(msg_fin)
    if code_fin == 0:
        raise RuntimeError("Error en finish_ETL: " + msg_fin)

    logger.info("🎉 ETL_orchestrator completado con éxito.")


if __name__ == "__main__":
    etl_orchestrator()
    etl_orchestrator.serve()