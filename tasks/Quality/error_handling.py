# tasks/Utils/error_handling.py

import pandas as pd
from prefect import task, get_run_logger

@task
def error_handling(
    error_code: int,
    error_msg: str,
    df: pd.DataFrame = None
):
    """
    Aborta el flujo mostrando:
      - Código de error y mensaje.
      - head(5) del df (si se pasa).
      - dtypes del df.
    No devuelve nada.
    """
    logger = get_run_logger()

    logger.error(f"Aborting task. Código: {error_code}. Mensaje: {error_msg}")

    if df is not None:
        try:
            head = df.head(5).to_string()
            logger.error(f"DataFrame.head(5):\n{head}")
        except Exception as e:
            logger.error(f"No se pudo mostrar head(): {e}")

        try:
            dtypes = df.dtypes.astype(str).to_string()
            logger.error(f" Dtypes:\n{dtypes}")
        except Exception as e:
            logger.error(f"No se pudo mostrar dtypes: {e}")
