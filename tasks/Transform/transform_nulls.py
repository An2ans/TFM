# tasks/Transform/transform_nulls.py

import pandas as pd
from typing import List, Tuple, Any
from prefect import task

@task
def transform_nulls(
    df: pd.DataFrame,
    col: str,
    null_values: List[Any] = None
) -> Tuple[int, pd.DataFrame, str]:
    """
    Sustituye nulos en la columna `col` de `df`:
      - Si dtype de col es numérico (int/float), reemplaza nulos por 0.
      - Si dtype es object/string, reemplaza nulos por cadena vacía "".
      - Además, cualquier valor en la lista `null_values` se trata como nulo y se sustituye.
    Devuelve (code, df_modificado, message):
      * code = 1 si OK, 0 si error.
      * message indica cuántos valores fueron reemplazados o el error ocurrido.
    """

    try:
        if col not in df.columns:
            raise KeyError(f"Columna '{col}' no existe en el DataFrame")

        df_mod = df.copy()
        series = df_mod[col]

        # Definir set de valores a tratar como nulos (sin incluir None/np.nan que pandas ya ve)
        extra_nulls = set(null_values) if null_values else set()

        # Contar nulos originales (NaN)
        orig_nulls = series.isna().sum()

        # Marcar como NaN cualquier valor en extra_nulls
        if extra_nulls:
            mask_extra = series.isin(extra_nulls)
            series = series.mask(mask_extra, pd.NA)

        # Contar totales después de marcar extra_nulls
        total_nulls = series.isna().sum()
        replaced = int(total_nulls)

        # Decidir valor de relleno según tipo
        if pd.api.types.is_numeric_dtype(series.dtype):
            fill_val = 0
        else:
            fill_val = ""

        # Reemplazar
        df_mod[col] = series.fillna(fill_val)

        msg = f"✅ Se han reemplazado {replaced} valores nulos/en {extra_nulls} en la columna '{col}'."
        return 1, df_mod, msg

    except Exception as e:
        err = f"❌ Error en transform_nulls para columna '{col}': {e}"
        return 0, df, err
