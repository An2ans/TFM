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
    Une 2 o más DataFrames en base a una columna común `key`.
    `how` puede ser "FULL" (outer join) o "INNER" (inner join).

    Códigos de retorno:
      * 0 → éxito.
      * 1 → clave inválida (no existe o no es string).
      * 2 → tipo de join inválido.
      * 3 → número insuficiente de DataFrames o alguno no es DataFrame.
      * 4 → mismatch de tipos en la columna key.
      * 9 → otro error desconocido.
    """
    logger = get_run_logger()

    # 1) Validar key
    if not isinstance(key, str) or not key:
        return 1, f"join_tables ❌ Error: la clave debe ser un string no vacío.", pd.DataFrame()

    # 2) Validar how
    how_upper = how.upper() if isinstance(how, str) else ""
    if how_upper not in {"FULL", "INNER"}:
        return 2, f"join_tables ❌ Error: 'how' debe ser 'FULL' o 'INNER'.", pd.DataFrame()

    # 3) Validar dfs
    if len(dfs) < 2:
        return 3, "join_tables ❌ Error: se requieren al menos dos DataFrames para hacer join.", pd.DataFrame()
    for i, df in enumerate(dfs, start=1):
        if not isinstance(df, pd.DataFrame):
            return 3, f"join_tables ❌ Error: el parámetro #{i+2} no es un DataFrame.", pd.DataFrame()

    try:
        # Verificar que la clave existe en cada df y chequear tipos
        dtypes = []
        for i, df in enumerate(dfs, start=1):
            if key not in df.columns:
                return 1, f"join_tables ❌ Error: la clave '{key}' no existe en el DataFrame #{i}.", pd.DataFrame()
            dtypes.append(df[key].dtype)

        # 4) Validar que todos los dtypes de la key sean iguales
        if any(dt != dtypes[0] for dt in dtypes[1:]):
            return 4, (
                f"join_tables ❌ Error: mismatch de tipos en la columna '{key}': "
                f"{[str(dt) for dt in dtypes]}"
            ), pd.DataFrame()

        # Mapeo de how a pandas
        how_map = {"FULL": "outer", "INNER": "inner"}
        pandas_how = how_map[how_upper]

        # Comenzar con el primer DataFrame y unir sucesivamente
        df_merged = dfs[0].copy()
        for df_next in dfs[1:]:
            df_merged = pd.merge(df_merged, df_next, on=key, how=pandas_how)

        msg = f"join_tables ✅ Tablas unidas correctamente usando '{how_upper}' join sobre clave '{key}'."
        return 0, msg, df_merged

    except Exception as e:
        logger.error(f"join_tables ❌ Error inesperado: {e}")
        return 9, f"join_tables ❌ Error inesperado: {e}", pd.DataFrame()
