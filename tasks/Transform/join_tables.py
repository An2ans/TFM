# tasks/Transform/join_tables.py

import pandas as pd
from typing import Tuple
from prefect import task, get_run_logger

@task
def join_tables(
    key: str,
    how: str,
    *dfs: pd.DataFrame
) -> Tuple[int, str, pd.DataFrame]:
    """
    Une exactamente dos DataFrames en base a una columna común `key`.
    `how` puede ser:
      - "FULL"  → outer join
      - "INNER" → inner join
      - "LEFT"  → left join (todos de df_left y coincidencias en df_right)
    
    Códigos de retorno:
      0 → éxito.
      1 → clave inválida (no existe o no es string).
      2 → tipo de join inválido.
      3 → número de DataFrames diferente de 2 o alguno no es DataFrame.
      4 → mismatch de tipos en la columna key entre ambos DataFrames.
      9 → otro error desconocido.
    
    Retorna: (code, mensaje, df_merged_o_empty).
    """
    logger = get_run_logger()

    # 1) Validar key
    if not isinstance(key, str) or not key.strip():
        return 1, "join_tables ❌ Error: la clave debe ser un string no vacío.", pd.DataFrame()

    # 2) Validar how
    how_upper = how.upper() if isinstance(how, str) else ""
    if how_upper not in {"FULL", "INNER", "LEFT"}:
        return 2, "join_tables ❌ Error: 'how' debe ser 'FULL', 'INNER' o 'LEFT'.", pd.DataFrame()

    # 3) Validar que haya exactamente dos DataFrames
    if len(dfs) != 2:
        return 3, "join_tables ❌ Error: se requieren exactamente dos DataFrames para hacer join.", pd.DataFrame()
    df_left, df_right = dfs
    if not isinstance(df_left, pd.DataFrame):
        return 3, "join_tables ❌ Error: el tercer parámetro (df_left) no es un DataFrame.", pd.DataFrame()
    if not isinstance(df_right, pd.DataFrame):
        return 3, "join_tables ❌ Error: el cuarto parámetro (df_right) no es un DataFrame.", pd.DataFrame()

    try:
        # 4) Verificar que la clave existe en ambos df y chequear tipos
        if key not in df_left.columns:
            return 1, f"join_tables ❌ Error: la clave '{key}' no existe en el DataFrame izquierdo.", pd.DataFrame()
        if key not in df_right.columns:
            return 1, f"join_tables ❌ Error: la clave '{key}' no existe en el DataFrame derecho.", pd.DataFrame()

        dtype_left = df_left[key].dtype
        dtype_right = df_right[key].dtype
        if dtype_left != dtype_right:
            return 4, (
                f"join_tables ❌ Error: mismatch de tipos en la columna '{key}': "
                f"[izquierdo: {dtype_left}, derecho: {dtype_right}]"
            ), pd.DataFrame()

        # 5) Mapear how a pandas
        how_map = {
            "FULL": "outer",
            "INNER": "inner",
            "LEFT": "left"
        }
        pandas_how = how_map[how_upper]

        # 6) Realizar el merge
        df_merged = pd.merge(df_left.copy(), df_right.copy(), on=key, how=pandas_how)
        msg = f"join_tables ✅ Tablas unidas correctamente usando '{how_upper}' join sobre clave '{key}'."
        return 0, msg, df_merged

    except Exception as e:
        logger.error(f"join_tables ❌ Error inesperado: {e}")
        return 9, f"join_tables ❌ Error inesperado: {e}", pd.DataFrame()
