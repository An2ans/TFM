# tasks/Transform/transform_date.py

import pandas as pd
from typing import Tuple
from prefect import task, get_run_logger

def _build_strptime_format(date_format: str) -> str:
    """
    Traduce un patrón tipo 'DDMMYYYY' a un formato strptime '%d%m%Y',
    reconociendo los tokens 'YYYY', 'MM', 'DD' en ese orden.
    Cualquier otro carácter se copia tal cual.
    """
    i = 0
    fmt = ""
    while i < len(date_format):
        if date_format.startswith("YYYY", i):
            fmt += "%Y"
            i += 4
        elif date_format.startswith("YY", i):
            fmt += "%y"
            i += 2
        elif date_format.startswith("MM", i):
            fmt += "%m"
            i += 2
        elif date_format.startswith("DD", i):
            fmt += "%d"
            i += 2
        else:
            # Copiamos cualquier separador literal ('-', '/', etc.)
            fmt += date_format[i]
            i += 1
    return fmt

@task
def transform_date(
    df: pd.DataFrame,
    col: str,
    date_format: str = "YYYYMMDD"
) -> Tuple[int, str, pd.DataFrame]:
    """
    Convierte la columna `col` de `df` a datetime según `date_format` (p.ej. "DDMMYYYY").
    Retorna (code, msg, df_modificado):
      * code=1: df es None o está vacío.
      * code=2: columna no existe.
      * code=3: longitud de cadena no coincide con date_format.
      * code=4: caracteres no numéricos encontrados.
      * code=9: otro error inesperado.
      * code=0: conversión exitosa.
    """
    logger = get_run_logger()

    # 1) df inválido
    if df is None or df.empty:
        return 1, "transform_date ❌ Error: DataFrame vacío o None.", df

    # 2) columna existe?
    if col not in df.columns:
        return 2, f"transform_date ❌ Error: Columna '{col}' no encontrada en el DataFrame.", df

    raw = df[col].astype(str).str.strip()
    expected_len = len(date_format)

    # 3) longitud incorrecta
    wrong_len = raw[raw.str.len() != expected_len]
    if not wrong_len.empty:
        ejemplo = wrong_len.iloc[0]
        return 3, (
            f"transform_date ❌ Error: Entrada de longitud inválida en '{col}': "
            f"'{ejemplo}' no coincide con formato {date_format}."
        ), df

    # 4) caracteres no numéricos
    non_digits = raw[raw.str.contains(r"\D")]
    if not non_digits.empty:
        ejemplo = non_digits.iloc[0]
        return 4, (
            f"transform_date ❌ Error: Caracteres no numéricos en '{col}': ejemplo '{ejemplo}'."
        ), df

    # 5) construir el formato correcto para strptime
    try:
        fmt = _build_strptime_format(date_format)
    except Exception as e:
        return 9, f"transform_date ❌ Error interno construyendo el formato: {e}", df

    # 6) intentar conversión
    try:
        df_mod = df.copy()
        df_mod[col] = pd.to_datetime(raw, format=fmt, errors="raise")
        return 0, f"transform_date ✅ Columna '{col}' convertida a datetime con formato {date_format}.", df_mod
    except Exception as e:
        head_str = df.head(5).to_string(index=False)
        dtypes_str = df.dtypes.astype(str).to_string()
        full_msg = (
            f"transform_date ❌ Error inesperado al convertir '{col}': {e}\n\n"
            f"DataFrame.head(5):\n{head_str}\n\n"
            f"Estructura de columnas:\n{dtypes_str}"
        )
        return 9, full_msg, df
