# tasks/Load/connect_prefect_workpool.py

# tasks/Load/connect_prefect_workpool.py

import os
from typing import Tuple, Optional, Any
from prefect import task, get_client
from prefect.client.schemas.objects import WorkPool

@task
def connect_prefect_workpool() -> Tuple[int, str, Optional[WorkPool]]:
    """
    Conecta con un Work Pool y su Work Queue en Prefect.

    Códigos de retorno:
    0 - Ejecución exitosa
    1 - No existen las variables de entorno necesarias
    2 - Las variables no son correctas (pool no encontrado)
    3 - Error de conexión
    9 - Otro error inesperado
    """

    try:
        pool_name = os.getenv("PREFECT_DEFAULT_WORK_POOL_NAME")
        pool_id   = os.getenv("PREFECT_DEFAULT_WORK_POOL_ID")

        if not pool_name or not pool_id:
            return 1, "❌ Faltan PREFECT_DEFAULT_WORK_POOL_NAME o PREFECT_DEFAULT_WORK_POOL_ID en .env", None

        with get_client(sync_client=True) as client:
            pool_obj = None

            try:
                pool_obj = client.read_work_pool(work_pool_name=pool_name)
            except Exception:
                try:
                    pool_obj = client.read_work_pool(work_pool_id=pool_id)
                except Exception:
                    return 2, f"❌ No se encontró el work pool '{pool_name}' con ID '{pool_id}'.", None

            parte1 = f"✅ Se ha conectado al work pool '{pool_name}'."

            try:
                queues = client.read_work_queues(work_pool_id=pool_obj.id)
            except Exception as conn_err:
                return 3, f"❌ Error al leer work queues: {conn_err}", None

            if queues:
                queue_id = queues[0].id
                parte2 = f"→ Usando work queue existente: '{queue_id}'"
            else:
                try:
                    new_queue_name = f"{pool_name}-auto-queue"
                    new_queue = client.create_work_queue(
                        work_queue={"name": new_queue_name, "work_pool_id": pool_obj.id}
                    )
                    queue_id = new_queue.id
                    parte2 = f"→ Nueva work queue creada: '{queue_id}'"
                except Exception as queue_err:
                    return 3, f"⚠️ No se pudo crear la work queue: {queue_err}", None

            mensaje = f"{parte1} {parte2}"
            return 0, mensaje, pool_obj

    except Exception as e:
        return 9, f"❌ Error inesperado: {str(e)}", None
