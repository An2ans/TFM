# flows/load_flow.py

from pathlib import Path
import pandas as pd
from typing import Tuple
from prefect import flow, get_run_logger

# Tareas necesarias
from tasks.Load.connect_local_duckdb import connect_local_duckdb
from tasks.Load.create_local_table import create_local_table
from tasks.Load.update_summary import update_summary
from tasks.Quality.error_handling import error_handling

@flow(name="load_flow")
def load_flow(
    LOCAL_DB_PATH: str,
    load_item: Tuple[int, str, pd.DataFrame]
) -> None:
    """
    Unifica la carga en DuckDB de una tabla + actualización de summary.
    
    Parámetros:
    - LOCAL_DB_PATH: ruta al fichero DuckDB.
    - load_item: tupla (table_id, table_name, df) para cargar.

    Patrón de ejecución por pasos con:
      - while task_code == 0:
          Ejecuta cada tarea en orden, actualiza task_code, task_msg, con/df
          Hace logger.info(task_msg)
          Si falla sale del bucle
      - Tras el bucle, si task_code != 0 llama a error_handling
      - Si code == 0, registra éxito y termina.
    """
    logger = get_run_logger()

    table_id, table_name, df = load_item

    task_code, task_msg = 0, ""
    con = None  # la conexión DuckDB

    # Bucle de carga
    while task_code == 0:
        # 1) Conectar a DuckDB
        code_01, msg_01, con = connect_local_duckdb(str(LOCAL_DB_PATH))
        task_code, task_msg = code_01, msg_01
        logger.info(msg_01)
        if task_code != 0:
            break

        # 2) Crear o sustituir la tabla principal
        code_02, msg_02, _ = create_local_table(df, table_name, con)
        task_code, task_msg = code_02, msg_02
        logger.info(msg_02)
        if task_code != 0:
            break

        # 3) Actualizar tabla summary
        code_03, msg_03 = update_summary(df, table_id, table_name, con)
        task_code, task_msg = code_03, msg_03
        logger.info(msg_03)
        # no break aquí, ya hemos terminado correctamente

        break  # salimos exitosamente

    # Post-bucle: fallo o éxito único
    if task_code != 0:
        error_handling(task_code, task_msg, df)
        raise RuntimeError(f"load de tabla '{table_name}' abortada por error")
    else:
        logger.info(f"🎉 load_flow: tabla '{table_name}' cargada y summary actualizado con éxito.")
