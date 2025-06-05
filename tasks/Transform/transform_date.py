# tasks/Transform/transform_date.py

import pandas as pd
from typing import Tuple, Any
from prefect import task, get_run_logger

@task
def transform_date(
    df: pd.DataFrame,
    col: str,
    date_format: str = "YYYYMMDD"
) -> Tuple[int, pd.DataFrame, str]:
    """
    Convierte la columna `col` de `df`, que debe contener fechas en formato `date_format`,
    a tipo datetime. Ejemplo: "20150826" → "2015-08-26".
    
    Retorna:
      * code = 1 y df_modificado si la conversión fue exitosa.
      * code = 0 y mensaje de error si hubo algún problema.
    """
    try:
        if col not in df.columns:
            raise KeyError(f"Columna '{col}' no encontrada en el DataFrame")

        # Primero, verificar que todos los valores sean cadenas numéricas o ints de largo correcto
        raw = df[col].astype(str).str.strip()
        expected_len = len(date_format)
        invalid = raw[~raw.str.fullmatch(r"\d{" + str(expected_len) + r"}")]
        if not invalid.empty:
            ejemplo = invalid.iloc[0]
            raise ValueError(f"Entrada inválida en '{col}': '{ejemplo}' no coincide con formato {date_format}")

        # Intentar conversión
        df_mod = df.copy()
        df_mod[col] = pd.to_datetime(raw, format="%Y%m%d", errors="raise")

        msg = f"✅ Columna '{col}' convertida a datetime con formato {date_format}."
        return 1, df_mod, msg

    except Exception as e:
        head_str = df.head(5).to_string(index=False)
        dtypes_str = df.dtypes.astype(str).to_string()
        full_msg = (
            f"❌ Error en transform_date: {e}\n\n"
            f"DataFrame.head(5):\n{head_str}\n\n"
            f"Estructura de columnas:\n{dtypes_str}"
        )
        return 0, df, full_msg
