# tasks/Transform/group_by_dates.py

import pandas as pd
from typing import Tuple, Any
from prefect import task, get_run_logger

@task
def group_by_dates(
    df: pd.DataFrame,
    dates_col: str,
    value_col: str,
    agg_func: str = "SUM"
) -> Tuple[int, pd.DataFrame, str]:
    """
    Agrupa `df` por cada día en `dates_col` y aplica la agregación `agg_func`
    sobre `value_col`. Los agg disponibles son: "SUM", "MAX", "MIN", "AVG".
    
    Retorna:
      * code = 1, df_agregado, mensaje si todo correcto.
      * code = 0, df original, mensaje si error.
    """
    logger = get_run_logger()
    try:
        if dates_col not in df.columns:
            raise KeyError(f"Columna de fechas '{dates_col}' no existe")

        if not pd.api.types.is_datetime64_any_dtype(df[dates_col]):
            raise TypeError(f"Columna '{dates_col}' no es tipo fecha/datetime")

        if value_col not in df.columns:
            raise KeyError(f"Columna de valores '{value_col}' no existe")

        if not pd.api.types.is_numeric_dtype(df[value_col]):
            raise TypeError(f"Columna '{value_col}' no contiene datos numéricos")

        agg = agg_func.upper()
        if agg not in {"SUM", "MAX", "MIN", "AVG"}:
            raise ValueError("Función de agregación inválida; opciones: SUM, MAX, MIN, AVG")

        grouping = df.groupby(dates_col)[value_col]
        if agg == "SUM":
            df_agg = grouping.sum().reset_index()
        elif agg == "MAX":
            df_agg = grouping.max().reset_index()
        elif agg == "MIN":
            df_agg = grouping.min().reset_index()
        else:  # "AVG"
            df_agg = grouping.mean().reset_index()

        df_agg = df_agg.rename(columns={value_col: f"{value_col}_{agg.lower()}"})
        msg = f"✅ DataFrame agrupado por '{dates_col}' con '{agg}' en columna '{value_col}'."
        logger.info(msg)
        return 1, df_agg, msg

    except Exception as e:
        head_str = df.head(5).to_string(index=False)
        dtypes_str = df.dtypes.astype(str).to_string()
        full_msg = (
            f"❌ Error en group_by_dates: {e}\n\n"
            f"DataFrame.head(5):\n{head_str}\n\n"
            f"Estructura de columnas:\n{dtypes_str}"
        )
        logger.error(full_msg)
        return 0, df, full_msg
