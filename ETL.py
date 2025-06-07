# ETL.py

import os , json
from dotenv import load_dotenv
from pathlib import Path
from prefect import flow, get_run_logger, run
from tasks.Load.connect_prefect_workpool import connect_prefect_workpool
from tasks.Load.finish_ETL import finish_ETL
from flows.affiliated_flow import affiliated_flow
from flows.product_flow import product_flow
from flows.sales_flow import sales_flow
from flows.calendar_flow import calendar_flow
from flows.load_flow import load_flow
from flows.delivery_flow import delivery_flow
from flows.oos_flow import oos_flow


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

    load_items = []

    # 1) Conectar al Work Pool
    code_pool, msg_pool = connect_prefect_workpool()
    logger.info(msg_pool)
    if code_pool == 0:
        raise RuntimeError("Aborting ETL: " + msg_pool)

    # 2) Ejecutar subflow 'affiliated_flow'con sus settings
    #logger.info("▶️ Iniciando `affiliated_flow` …")
    #affiliated_flow(flow_settings["affiliated"], LOCAL_DB_PATH)
    #logger.info("✅ `affiliated_flow` finalizado.")

    # 3) Ejecutar subflow 'product_flow'con sus settings
    #logger.info("▶️ Iniciando `product_flow` …")
    #product_flow(flow_settings["product"], LOCAL_DB_PATH)
    #logger.info("✅ `product_flow` finalizado.")

    # 4) Ejecutar subflow 'sales_flow'con sus settings
    #logger.info("▶️ Iniciando `sales_flow` …")
    #sales_table = sales_flow(flow_settings["sales"] )
    #load_items.append(sales_table)
    #logger.info("✅ `sales_flow` finalizado.")
    
    # 5) Ejecutar subflow 'delivery_flow'con sus settings
    logger.info("▶️ Iniciando `delivery_flow` …")
    delivery_table = delivery_flow(flow_settings["delivery"] )
    load_items.append(delivery_table)
    logger.info("✅ `delivery_flow` finalizado.")
    
    # 6) Ejecutar subflow 'oos_flow'con sus settings
    logger.info("▶️ Iniciando `oos_flow` …")
    oos_table = oos_flow(flow_settings["oos"] )
    load_items.append(oos_table)
    logger.info("✅ `oos_flow` finalizado.")


    # ) Ejecutamos subflow calendar_flow 
    logger.info("▶️ Iniciando `calendar_flow` …")
    calendar_table = calendar_flow(flow_settings["calendar"])
    load_items.append(calendar_table)
    logger.info("✅ `calendar_flow` finalizado.")


    # ) Cargar las tablas en la DB
    for table_id, table_name, df in load_items:
        logger.info(f"▶️ Iniciando carga de '{table_name}' (ID={table_id}) …")
        load_flow.run(
            LOCAL_DB_PATH,
            (table_id, table_name, df)
        )
        logger.info(f"✅ Carga de '{table_name}' completada.")    # ) Limpiar caché
    code_fin, msg_fin = finish_ETL()
    logger.info(msg_fin)

    logger.info("🎉 ETL_orchestrator completado con éxito.")


if __name__ == "__main__":
    etl_orchestrator()
    #etl_orchestrator.serve()