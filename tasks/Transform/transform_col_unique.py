# tasks/Transform/transform_col_unique.py

import pandas as pd
from typing import Tuple, Any
from prefect import task, get_run_logger

@task
def transform_col_unique(df: pd.DataFrame, col: str) -> Tuple[int, pd.DataFrame, str]:
    """
    Garantiza que los valores en df[col] sean únicos.
    Si aparece un duplicado, añade ' - N' donde N es el contador de duplicados.
    Ejemplo: ['A', 'A', 'B', 'A'] → ['A', 'A - 2', 'B', 'A - 3']

    Devuelve:
      * code = 1 si se transformó correctamente.
      * df_modificado = DataFrame con la columna única.
      * message = descripción del resultado o error.
    """
    try:
        if col not in df.columns:
            raise KeyError(f"Columna '{col}' no encontrada en el DataFrame")

        counts = {}
        new_values = []
        for val in df[col].fillna("").astype(str):
            counts[val] = counts.get(val, 0) + 1
            if counts[val] == 1:
                new_values.append(val)
            else:
                new_values.append(f"{val} - {counts[val]}")

        df_mod = df.copy()
        df_mod[col] = new_values

        msg = f"✅ Todos los valores de '{col}' son únicos ahora."
        return 1, df_mod, msg

    except Exception as e:
        err = f"❌ Error en transform_col_unique: {e}"
        # Devolvemos el DataFrame original para que el flujo decida cómo continuar
        return 0, df, err
