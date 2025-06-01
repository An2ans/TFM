# tasks/connect_workpool.py

import subprocess
import os
from prefect import task, get_run_logger

@task
def connect_work_pool() -> str:
    """
    Comprueba que el Work Pool configurado exista en Prefect Cloud.
    Lee de variables de entorno:
      - PREFECT_DEFAULT_WORK_POOL_NAME
      - PREFECT_DEFAULT_WORK_POOL_ID

    Si el pool NO existe, levanta RuntimeError y detiene el flujo.
    Si existe, devuelve el ID (o el nombre) para posteriores usos.
    """
    logger = get_run_logger()

    # 1) Leer nombre y ID del work pool desde .env
    pool_name = os.getenv("PREFECT_DEFAULT_WORK_POOL_NAME")
    pool_id   = os.getenv("PREFECT_DEFAULT_WORK_POOL_ID")

    if not pool_name or not pool_id:
        msg = "❌ Faltan variables de entorno PREFECT_DEFAULT_WORK_POOL_NAME o PREFECT_DEFAULT_WORK_POOL_ID."
        logger.error(msg)
        raise RuntimeError(msg)

    # 2) Invocar CLI para listar work pools existentes
    try:
        result = subprocess.run(
            ["prefect", "work-pool", "ls"],
            capture_output=True, text=True, check=True
        )
        output = result.stdout.lower()
    except subprocess.CalledProcessError as e:
        msg = f"❌ Error al listar Work Pools: {e.stderr.strip()}"
        logger.error(msg)
        raise RuntimeError(msg)

    # 3) Verificar que aparezca el pool_id o pool_name en la salida
    if (pool_id.lower() in output) or (pool_name.lower() in output):
        logger.info(f"✅ Work Pool encontrado: '{pool_name}' (ID={pool_id})")
        return pool_id

    # 4) Si no existe, fallamos
    msg = f"❌ Work Pool '{pool_name}' (ID={pool_id}) NO encontrado en Prefect Cloud."
    logger.error(msg)
    raise RuntimeError(msg)
