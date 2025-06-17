# tasks/Transform/rename_col.py

import pandas as pd
from typing import Tuple, Dict, Any
from prefect import task, get_run_logger

@task
def rename_col(
    df: pd.DataFrame,
    names_map: Dict[str, str]
) -> Tuple[int, str, pd.DataFrame]:
    """
    Renombra columnas de `df` según `names_map`, donde keys son nombres actuales
    y values son nombres nuevos.

    Parámetros:
    - df: DataFrame a procesar.
    - names_map: dict {col_actual: col_nueva}.

    Códigos de retorno:
      1 → parámetros inválidos (df no es DataFrame o está vacío, names_map no es dict o está vacío).
      2 → columnas faltantes en df.
      3 → valor de names_map no válido (no es texto).
      9 → otro error inesperado.
      0 → éxito.

    Retorna: (code, mensaje, df_modificado)
    """
    logger = get_run_logger()
    try:
        # 1) Validación de parámetros básicos
        if not isinstance(df, pd.DataFrame) or df is None or df.empty:
            return 1, "rename_col ❌ Error: DataFrame inválido o vacío.", df
        if not isinstance(names_map, dict) or len(names_map) == 0:
            return 1, "rename_col ❌ Error: names_map debe ser un dict no vacío {col_actual: col_nueva}.", df

        # 2) Verificar que las columnas a renombrar existen en df
        missing = [col for col in names_map.keys() if col not in df.columns]
        if missing:
            return 2, f"rename_col ❌ Error: columnas no encontradas en DataFrame: {missing}.", df

        # 3) Verificar que los nuevos nombres sean strings válidos
        invalid_new = [new for new in names_map.values() if not isinstance(new, str) or new.strip() == ""]
        if invalid_new:
            return 3, f"rename_col ❌ Error: nombres nuevos inválidos (deben ser strings no vacíos): {invalid_new}.", df

        # 4) Intentar renombrar
        try:
            df_mod = df.copy()
            df_mod = df_mod.rename(columns=names_map)
        except Exception as e:
            # Error inesperado durante rename
            head_str = df.head(5).to_string(index=False)
            dtypes_str = df.dtypes.astype(str).to_string()
            full_msg = (
                f"rename_col ❌ Error al renombrar columnas: {e}\n\n"
                f"DataFrame.head(5):\n{head_str}\n\n"
                f"Estructura de columnas:\n{dtypes_str}"
            )
            logger.error(full_msg)
            return 9, full_msg, df

        msg = f"rename_col ✅ Columnas renombradas correctamente: {list(names_map.keys())} → {list(names_map.values())}."
        logger.info(msg)
        return 0, msg, df_mod

    except Exception as e:
        # Capturar cualquier otro error inesperado
        try:
            head_str = df.head(5).to_string(index=False)
            dtypes_str = df.dtypes.astype(str).to_string()
            full_msg = (
                f"rename_col ❌ Error inesperado: {e}\n\n"
                f"DataFrame.head(5):\n{head_str}\n\n"
                f"Estructura de columnas:\n{dtypes_str}"
            )
        except Exception:
            full_msg = f"rename_col ❌ Error inesperado: {e} (además falló al mostrar head/dtypes)."
        logger.error(full_msg)
        return 9, full_msg, df
