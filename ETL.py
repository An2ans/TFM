# ETL.py

import os
from dotenv import load_dotenv
from prefect import flow, get_run_logger

from tasks.Load.connect_prefect_workpool import connect_prefect_workpool
from tasks.Load.finish_ETL import finish_ETL
from flows.affiliated_flow import affiliated_flow

load_dotenv()

@flow(name="etl_orchestrator")
def etl_orchestrator():
    logger = get_run_logger()

    # 1) Conectar al Work Pool
    code_pool, msg_pool = connect_prefect_workpool()
    logger.info(msg_pool)
    if code_pool == 0:
        raise RuntimeError("Aborting ETL: " + msg_pool)

    # 2) Ejecutar subflow 'affiliated_flow'
    logger.info("▶️ Iniciando `affiliated_flow` …")
    affiliated_flow()
    logger.info("✅ `affiliated_flow` finalizado.")

    # 3) Limpiar caché
    code_fin, msg_fin = finish_ETL()
    logger.info(msg_fin)
    if code_fin == 0:
        raise RuntimeError("Error en finish_ETL: " + msg_fin)

    logger.info("🎉 ETL_orchestrator completado con éxito.")


if __name__ == "__main__":
    etl_orchestrator()
    pool_name = os.getenv("PREFECT_DEFAULT_WORK_POOL_NAME")
    etl_orchestrator.deploy(
        name="etl_orchestrator-deployment",
        work_pool_name=pool_name
    )
