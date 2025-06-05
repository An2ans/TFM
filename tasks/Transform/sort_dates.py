# tasks/Transform/sort_dates.py

import pandas as pd
from typing import Tuple, Any
from prefect import task, get_run_logger

@task
def sort_dates(
    df: pd.DataFrame,
    col: str,
    order: str = "ASC"
) -> Tuple[int, pd.DataFrame, str]:
    """
    Ordena `df` por la columna `col`, que debe ser tipo datetime. `order` es "ASC" o "DES".
    
    Retorna:
      * code = 1, df_ordenado, mensaje si todo bien.
      * code = 0, df original, mensaje si error.
    """
    try:
        if col not in df.columns:
            raise KeyError(f"Columna '{col}' no encontrada en el DataFrame")

        if not pd.api.types.is_datetime64_any_dtype(df[col]):
            raise TypeError(f"Columna '{col}' no es tipo datetime")

        ascending = True
        if order.upper() == "ASC":
            ascending = True
        elif order.upper() == "DES":
            ascending = False
        else:
            raise ValueError("El parámetro 'order' debe ser 'ASC' o 'DES'")

        df_mod = df.copy().sort_values(by=col, ascending=ascending).reset_index(drop=True)
        msg = f"✅ DataFrame ordenado por '{col}' en orden {'ascendente' if ascending else 'descendente'}."
        return 1, df_mod, msg

    except Exception as e:
        head_str = df.head(5).to_string(index=False)
        dtypes_str = df.dtypes.astype(str).to_string()
        full_msg = (
            f"❌ Error en sort_dates: {e}\n\n"
            f"DataFrame.head(5):\n{head_str}\n\n"
            f"Estructura de columnas:\n{dtypes_str}"
        )
        return 0, df, full_msg
