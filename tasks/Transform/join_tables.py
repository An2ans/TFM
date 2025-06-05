# tasks/Transform/join_tables.py

import pandas as pd
from typing import Tuple, Any, List
from prefect import task, get_run_logger

@task
def join_tables(
    key: str,
    how: str,
    *dfs: pd.DataFrame
) -> Tuple[int, pd.DataFrame, str]:
    """
    Une 2 o más DataFrames en base a una columna común `key`.
    `how` puede ser "FULL" (outer join) o "INNER" (inner join).
    
    Retorna:
      * code = 1, df_unido, mensaje si éxito.
      * code = 0, df vacío, mensaje si error.
    """
    try:
        if how.upper() not in {"FULL", "INNER"}:
            raise ValueError("Tipo de join inválido; opciones: FULL, INNER")

        if len(dfs) < 2:
            raise ValueError("Se requieren al menos dos DataFrames para hacer join")

        # Verificar que la clave existe en cada df
        for i, df in enumerate(dfs, start=1):
            if key not in df.columns:
                raise KeyError(f"La clave '{key}' no existe en el DataFrame #{i}")

        # Mapeo de how a pandas
        how_map = {"FULL": "outer", "INNER": "inner"}
        pandas_how = how_map[how.upper()]

        # Comenzar con el primer DataFrame
        df_merged = dfs[0].copy()
        # Iterar uniendo sucesivamente
        for df_next in dfs[1:]:
            df_merged = pd.merge(df_merged, df_next, on=key, how=pandas_how)

        msg = f"✅ Tablas unidas correctamente usando '{how}' join sobre clave '{key}'."
        return 1, df_merged, msg

    except Exception as e:
        full_msg = f"❌ Error en join_tables: {e}"
        # Retornamos un DataFrame vacío para que el flujo pueda detectar el error
        return 0, pd.DataFrame(), full_msg
