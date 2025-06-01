# ETL.py

import os
from dotenv import load_dotenv
from prefect import flow, get_run_logger

# Importar task para validar work pool
from tasks.connect_workpool import connect_work_pool

# Importar subflows
from flows.affiliated_flow import affiliated_flow
# (En el futuro, podrías agregar otros: from flows.otro_flow import otro_flow)

# Cargar variables de entorno desde .env
load_dotenv()
# Asegúrate de que .env contenga al menos:
#   PREFECT_API_KEY=…
#   PREFECT_WORKSPACE_ID=…
#   PREFECT_DEFAULT_WORK_POOL_NAME=default-agent-pool
#   PREFECT_DEFAULT_WORK_POOL_ID=8c879453-d19d-41aa-ae79-e91c1d8b59f4

@flow(name="etl_orchestrator")
def etl(selected_flow: str):
    """
    Orquestador principal sin retries, que:
    1) Verifica el Work Pool.
    2) Ejecuta únicamente el subflow solicitado.
    """
    logger = get_run_logger()

    # 1) Verificar que el Work Pool configurado exista
    pool_id = connect_work_pool()
    logger.info(f"→ Usaremos el Work Pool ID: {pool_id}")

    # 2) Ejecutar el subflow seleccionado
    if selected_flow.lower() == "affiliated":
        logger.info("▶️ Iniciando `affiliated_flow` …")
        affiliated_flow()
        logger.info("✅ `affiliated_flow` completado.")
    else:
        msg = f"❌ Flow desconocido para ejecutar: '{selected_flow}'."
        logger.error(msg)
        raise ValueError(msg)

if __name__ == "__main__":
    # Leer cuál flow queremos ejecutar (por defecto "affiliated")
    selected = os.getenv("ETL_SELECTED_FLOW", "affiliated")
    
    # 1) Ejecutar localmente el flow orquestador
    etl(selected)
    
    # 2) Registrar (deploy) el flow en Prefect Cloud, usando el Work Pool configurado
    #    El nombre del pool se lee de la variable de entorno
    pool_name = os.getenv("PREFECT_DEFAULT_WORK_POOL_NAME")
    etl.deploy(
        name="etl_orchestrator-deployment",
        work_pool_name=pool_name
    )
