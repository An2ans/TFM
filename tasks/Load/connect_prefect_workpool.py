# tasks/Load/connect_prefect_workpool.py

import os
from typing import Tuple
from prefect import task, get_client

@task
def connect_prefect_workpool() -> Tuple[int, str]:
    """
    1) Lee PREFECT_DEFAULT_WORK_POOL_NAME y PREFECT_DEFAULT_WORK_POOL_ID de .env.
    2) Usa Prefect Client (sync) para buscar el Work Pool por nombre o ID:
       - Si no lo encuentra, devuelve (0, mensaje de error).
       - Si lo encuentra, parte1 = "✅ Se ha conectado al work pool '<pool_name>'".
    3) Lee las Work Queues asociadas a ese pool:
       - Si existe al menos una, parte2 = "→ Usando work queue existente: '<queue_id>'".
       - Si no existe ninguna, crea una nueva con nombre "<pool_name>-auto-queue":
         * Si la creación tiene éxito, parte2 = "→ Nueva work queue creada: '<nueva_queue_id>'".
         * Si falla la creación, parte2 = "⚠️ No se pudo conectar ni crear work queue para '<pool_name>'".
    4) Devuelve (code, mensaje_total), donde code = 1 si encontró el pool; 0 si no.
    """

    pool_name = os.getenv("PREFECT_DEFAULT_WORK_POOL_NAME")
    pool_id   = os.getenv("PREFECT_DEFAULT_WORK_POOL_ID")
    if not pool_name or not pool_id:
        return 0, "❌ Faltan PREFECT_DEFAULT_WORK_POOL_NAME o PREFECT_DEFAULT_WORK_POOL_ID en .env"

    # Usamos el client síncrono
    with get_client(sync_client=True) as client:
        # 1) Intentar leer el Work Pool por nombre
        pool_obj = None
        try:
            pool_obj = client.read_work_pool(work_pool_name=pool_name)
        except Exception:
            # Si no lo encuentra por nombre, probamos por ID
            try:
                pool_obj = client.read_work_pool(work_pool_id=pool_id)
            except Exception:
                pool_obj = None

        if pool_obj is None:
            return 0, f"❌ Incapaz de conectar al work pool '{pool_name}'."

        parte1 = f"✅ Se ha conectado al work pool '{pool_name}'."

        # 2) Listar las Work Queues de ese pool
        try:
            queues = client.read_work_queues(work_pool_id=pool_obj.id)
        except Exception:
            queues = []

        if queues:
            # Tomamos la primera queue existente
            queue_id = queues[0].id
            parte2 = f"→ Usando work queue existente: '{queue_id}'"
        else:
            # 3) No hay queues; creamos una nueva
            new_queue_name = f"{pool_name}-auto-queue"
            try:
                new_queue = client.create_work_queue(
                    work_queue={"name": new_queue_name, "work_pool_id": pool_obj.id}
                )
                queue_id = new_queue.id
                parte2 = f"→ Nueva work queue creada: '{queue_id}'"
            except Exception:
                parte2 = f"⚠️ No se pudo conectar ni crear work queue para '{pool_name}'."

    mensaje_total = f"{parte1} {parte2}"
    return 1, mensaje_total
