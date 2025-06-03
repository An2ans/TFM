# tasks/Load/finish_ETL.py

import shutil
from pathlib import Path
from prefect import task, get_run_logger
from typing import Tuple

@task(cache_key_fn=lambda *_: None)
def finish_ETL() -> Tuple[int, str]:
    """
    1) Elimina la caché local de Prefect (si existe).
    2) Cierra cualquier recurso (DuckDB) que quede abierto: normalmente las tareas ya cierran su conexión.
    3) Devuelve (1, mensaje de limpieza completa). En caso de error, (0, mensaje_error).
    """
    logger = get_run_logger()

    try:
        # 1) Eliminar caché de Prefect (por defecto en ~/.prefect/cache)
        cache_dir = Path.home() / ".prefect" / "cache"
        if cache_dir.exists():
            shutil.rmtree(cache_dir)
            logger.info(f"✅ Caché Prefect eliminada: {cache_dir}")

        # 2) No hay conexiones globales de DuckDB abiertas aquí (se cierran en la tarea create_local_table).
        #    Si existiera alguna referencia global, habría que cerrarla manualmente.

        msg = "🎉 finalización del ETL completada (caché borrada)."
        logger.info(msg)
        return 1, msg

    except Exception as e:
        err = f"❌ Error en finish_ETL: {e}"
        logger.error(err)
        return 0, err
