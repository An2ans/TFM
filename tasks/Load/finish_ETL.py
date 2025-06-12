# tasks/Load/finish_ETL.py

import shutil
import os
from typing import Tuple
from pathlib import Path
from prefect import task, get_run_logger

@task
def finish_ETL() -> Tuple[int, str]:
    """
    Elimina todas las carpetas '__pycache__' recursivamente a partir de la raíz del proyecto.
    Devuelve:
      (1, "Cache eliminado con éxito") si todo fue OK,
      (0, "mensaje de error") si algo falló.
    """
    logger = get_run_logger()
    try:
        # Partimos de la carpeta raíz del proyecto
        root_dir = Path(__file__).parent.parent.parent  

        removed = 0
        # Recorremos todos los subdirectorios
        for dirpath, dirnames, _ in os.walk(root_dir):
            if "__pycache__" in dirnames:
                cache_folder = Path(dirpath) / "__pycache__"
                shutil.rmtree(cache_folder)
                removed += 1

        msg = f"✅ Cache limpios: {removed} carpetas '__pycache__' eliminadas."
        logger.info(msg)
        return 0, msg

    except Exception as e:
        err = f"❌ Error al limpiar cache: {e}"
        logger.error(err)
        return 1, err
