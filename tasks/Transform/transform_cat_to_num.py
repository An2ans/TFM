# tasks/Transform/transform_cat_to_num.py

import pandas as pd
from typing import Dict, Tuple, Any
from prefect import task, get_run_logger

@task
def transform_cat_to_num(df: pd.DataFrame, col: str) -> Tuple[int, pd.DataFrame, Dict[Any, int], str]:
    """
    Crea una nueva columna '<col>_num' mapeando valores únicos de la columna categórica df[col] a números comenzando en 1.
    - Si hay nulos o valores ausentes en df[col], reciben el valor 0 en la columna numérica.
    - Devuelve (code, df_modificado, mapping, message).
      * code = 1 si todo OK, 0 si error.
      * mapping = dict que mapea {valor_categórico: número}.
    """

    try:
        if col not in df.columns:
            raise KeyError(f"Columna '{col}' no existe en el DataFrame")

        df_mod = df.copy()
        new_col = f"{col}_num"

        # Obtiene valores únicos (sin NaN)
        unique_vals = df_mod[col].dropna().unique().tolist()
        mapping: Dict[Any, int] = {val: i + 1 for i, val in enumerate(unique_vals)}

        # Mapear y asignar 0 a NaN o valores no en mapping
        df_mod[new_col] = df_mod[col].map(mapping).fillna(0).astype(int)

        msg = f"✅ Columna '{new_col}' creada con éxito. Mapping: {mapping}"
        return 1, df_mod, mapping, msg

    except Exception as e:
        err = f"❌ Error en transform_cat_to_num para columna '{col}': {e}"
        return 0, df, {}, err
