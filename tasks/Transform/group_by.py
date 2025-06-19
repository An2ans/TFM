# tasks/Transform/group_by.py

import pandas as pd
from typing import Tuple, Dict
from prefect import task, get_run_logger

@task
def group_by(
    df: pd.DataFrame,
    key: str,
    agg_map: Dict[str, str]
) -> Tuple[int, str, pd.DataFrame]:
    """
    Agrupa `df` por la columna `key` y aplica para cada columna especificada
    en `agg_map` la agregación indicada.
    
    Parámetros:
    - df: DataFrame a procesar.
    - key: columna sobre la que agrupar (debe existir en df, y ser str).
    - agg_map: dict cuyo formato es {columna: agregación}, donde agregación es
      uno de: "FIRST", "LAST", "SUM", "MAX", "MIN", "AVG".
    
    Códigos de retorno:
    1 → parámetros inválidos (df no es DataFrame o está vacío, key no es str, agg_map no es dict o está vacío).
    2 → columnas faltantes en df (key o alguna columna de agg_map no existe).
    3 → agregador inválido (no está en FIRST, LAST, SUM, MAX, MIN, AVG).
    4 → uso de agregador numérico en columna no numérica.
    9 → otro error inesperado.
    0 → éxito.
    
    Retorna: (code, mensaje, df_resultado)
    - El DataFrame resultado contendrá solo la columna `key` y las columnas de agg_map,
      con los valores agregados, y los nombres de columna permanecerán sin cambiar.
    """
    logger = get_run_logger()
    try:
        # 1) Validación de parámetros
        if not isinstance(df, pd.DataFrame) or df is None or df.empty:
            return 1, "group_by ❌ Error: DataFrame inválido o vacío.", df
        if not isinstance(key, str):
            return 1, "group_by ❌ Error: 'key' debe ser string.", df
        if not isinstance(agg_map, dict):
            return 1, "group_by ❌ Error: agg_map debe ser un dict {columna: agregación}.", df
        if len(agg_map) == 0:
            return 1, "group_by ❌ Error: agg_map está vacío; se necesita al menos una columna a agregar.", df

        # 2) Verificar existencia de columnas
        missing = []
        if key not in df.columns:
            missing.append(key)
        for col in agg_map.keys():
            if not isinstance(col, str) or col not in df.columns:
                missing.append(col)
        if missing:
            return 2, f"group_by ❌ Error: columnas no encontradas en DataFrame: {missing}.", df

        # 3) Validar agregaciones
        allowed = {"FIRST", "LAST", "SUM", "MAX", "MIN", "AVG"}
        agg_map_upper: Dict[str, str] = {}
        for col, agg in agg_map.items():
            if not isinstance(agg, str):
                return 3, f"group_by ❌ Error: agregación para columna '{col}' debe ser string.", df
            agg_u = agg.strip().upper()
            if agg_u not in allowed:
                return 3, f"group_by ❌ Error: agregación inválida para columna '{col}': '{agg}'. Opciones: {allowed}.", df
            agg_map_upper[col] = agg_u

        # 4) Verificar tipos de columna vs agregación
        numeric_aggs = {"SUM", "MAX", "MIN", "AVG"}
        for col, agg_u in agg_map_upper.items():
            if agg_u in numeric_aggs and not pd.api.types.is_numeric_dtype(df[col]):
                return 4, (
                    f"group_by ❌ Error: columna '{col}' no es numérica, "
                    f"no se puede aplicar agregación '{agg_u}'."
                ), df

        # 5) Construir dict para pandas .agg
        pandas_agg: Dict[str, str] = {}
        for col, agg_u in agg_map_upper.items():
            if agg_u == "FIRST":
                pandas_agg[col] = "first"
            elif agg_u == "LAST":
                pandas_agg[col] = "last"
            elif agg_u == "SUM":
                pandas_agg[col] = "sum"
            elif agg_u == "MAX":
                pandas_agg[col] = "max"
            elif agg_u == "MIN":
                pandas_agg[col] = "min"
            elif agg_u == "AVG":
                pandas_agg[col] = "mean"

        # 6) Ejecutar agrupación: resultará un DataFrame con índice = key y columnas de agg_map
        try:
            df_grouped = df.groupby(key).agg(pandas_agg)
            # Convertir índice a columna
            df_res = df_grouped.reset_index()
        except Exception as e:
            # Error inesperado al agrupar
            head_str = df.head(5).to_string(index=False)
            dtypes_str = df.dtypes.astype(str).to_string()
            full_msg = (
                f"group_by ❌ Error al agrupar por '{key}': {e}\n\n"
                f"DataFrame.head(5):\n{head_str}\n\n"
                f"Estructura de columnas:\n{dtypes_str}"
            )
            logger.error(full_msg)
            return 9, full_msg, df

        # 7) El DataFrame resultante solo contiene 'key' y las columnas agrupadas, con nombres originales
        msg = (
            f"group_by ✅ Agrupación por '{key}' exitosa. "
            f"Columnas agregadas: " +
            ", ".join(f"{col}->{agg_map_upper[col]}" for col in agg_map_upper)
        )
        logger.info(msg)
        return 0, msg, df_res

    except Exception as e:
        # Capturar cualquier otro error inesperado
        try:
            head_str = df.head(5).to_string(index=False)
            dtypes_str = df.dtypes.astype(str).to_string()
            full_msg = (
                f"group_by ❌ Error inesperado: {e}\n\n"
                f"DataFrame.head(5):\n{head_str}\n\n"
                f"Estructura de columnas:\n{dtypes_str}"
            )
        except Exception:
            full_msg = f"group_by ❌ Error inesperado: {e} (además falló al mostrar head/dtypes)"
        logger.error(full_msg)
        return 9, full_msg, df
