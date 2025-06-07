# tasks/Transform/sort_dates.py

import pandas as pd
from typing import Tuple
from prefect import task, get_run_logger

@task
def sort_dates(
    df: pd.DataFrame,
    col: str,
    order: str = "ASC"
) -> Tuple[int, str, pd.DataFrame]:
    """
    Ordena `df` por la columna `col`, que debe ser tipo datetime. `order` es "ASC" o "DES".
    
    Códigos de retorno:
      - 1 → DataFrame vacío o no válido.
      - 2 → Columna `col` no existe.
      - 3 → `order` no es "ASC" ni "DES".
      - 4 → Columna `col` no es tipo datetime/date.
      - 0 → Éxito.
    """
    logger = get_run_logger()
    # 1) Verificar que df no esté vacío y sea un DataFrame
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        msg = "❌ El DataFrame está vacío o no es válido."
        return 1, msg, df

    # 2) Verificar existencia de la columna
    if col not in df.columns:
        msg = f"❌ Columna '{col}' no existe en el DataFrame."
        return 2, msg, df

    # 3) Verificar parámetro order
    if order.upper() not in ("ASC", "DES"):
        msg = "❌ El parámetro 'order' debe ser 'ASC' o 'DES'."
        return 3, msg, df

    # 4) Verificar tipo datetime en la columna
    if not pd.api.types.is_datetime64_any_dtype(df[col]):
        msg = f"❌ Columna '{col}' no es de tipo datetime/date."
        return 4, msg, df

    # 5) Todo correcto: ordenar
    ascending = order.upper() == "ASC"
    try:
        df_mod = df.copy().sort_values(by=col, ascending=ascending).reset_index(drop=True)
        msg = f"✅ DataFrame ordenado por '{col}' en orden {'ascendente' if ascending else 'descendente'}."
        return 0, msg, df_mod
    except Exception as e:
        head_str = df.head(5).to_string(index=False)
        dtypes_str = df.dtypes.astype(str).to_string()
        full_msg = (
            f"❌ Error al ordenar DataFrame en sort_dates: {e}\n\n"
            f"DataFrame.head(5):\n{head_str}\n\n"
            f"Estructura de columnas:\n{dtypes_str}"
        )
        return 9, full_msg, df
