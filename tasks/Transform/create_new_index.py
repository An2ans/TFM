# tasks/Transform/create_new_index.py

import pandas as pd
from typing import Tuple
from prefect import task, get_run_logger

@task
def create_new_index(df: pd.DataFrame, col: str, name: str = "index") -> Tuple[int, pd.DataFrame, str]:
    """
    Crea una nueva columna 'Index' basada en la columna `col` de df, garantizando unicidad:
      - Toma el valor original (ej. 123) y agrega un sufijo incremental (0,1,2...) para duplicados.
      - Ordena de modo que cada fila con el mismo valor en `col` reciba sufijos 0,1,2,...
      - Inserta 'Index' como primera columna en el DataFrame resultante.
    Devuelve (code, df_modificado, message):
      * code = 1 si OK, 0 si error.
      * message informa del éxito o del error.
    """

    try:
        if col not in df.columns:
            raise KeyError(f"Columna '{col}' no existe en el DataFrame")

        df_mod = df.copy()

        # Contadores para cada valor en col
        counters = {}

        def make_index(val):
            # Si es NaN, tratamos como cadena "nan" para evitar problemas
            key = val if pd.notna(val) else "NaN"
            cnt = counters.get(key, 0)
            counters[key] = cnt + 1
            # Sufijo es cnt (comienza en 0)
            suffix = cnt
            base = str(val) if pd.notna(val) else "null"
            return f"{base}{suffix}"

        # Crear la columna Index
        df_mod[name] = df_mod[col].apply(make_index)

        # Reordenar columnas para que 'Index' quede primero
        cols = df_mod.columns.tolist()
        cols.remove(name)
        new_order = [name] + cols
        df_mod = df_mod[new_order]

        msg = "✅ Columna '{name}' creada correctamente como primera columna."
        return 1, df_mod, msg

    except Exception as e:
        err = f"❌ Error en create_new_index para columna '{col}': {e}"
        return 0, df, err
