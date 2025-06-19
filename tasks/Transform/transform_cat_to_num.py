# tasks/Transform/transform_cat_to_num.py

import pandas as pd
from typing import Dict, Tuple, Any, Optional
from prefect import task, get_run_logger

@task
def transform_cat_to_num(
    df: pd.DataFrame,
    col: str,
    cat_map: Optional[Dict[Any, int]] = None
) -> Tuple[int, str, pd.DataFrame]:
    """
    Crea una nueva columna '<col>_num' mapeando valores únicos de la columna categórica df[col] a números comenzando en 1.
    - Si hay nulos o valores ausentes en df[col], reciben el valor 0 en la columna numérica.
    - Puede aceptar un dict opcional cat_map con mapeo {valor: número}.

    Parámetros:
    - df: DataFrame a procesar.
    - col: nombre de la columna categórica en df.
    - cat_map: dict opcional {valor: número}, para forzar un mapeo predefinido.

    Códigos de retorno:
      1 → Parámetros inválidos (df no es DataFrame válido o está vacío, o col no es str)
      2 → Columna especificada no existe en df
      3 → cat_map presente pero formato inválido (no dict str->int o valores no apropiados)
      4 → cat_map presente pero conjunto de claves no coincide con valores únicos de df[col]
      5 → Demasiados valores únicos (>99) en df[col]
      9 → Otro error inesperado
      0 → Éxito

    Retorna: (code, mensaje, df_modificado)
    """
    logger = get_run_logger()
    try:
        # 1) Validación básica de parámetros
        if not isinstance(df, pd.DataFrame) or df is None or df.empty:
            return 1, "transform_cat_to_num ❌ Error: DataFrame inválido o vacío.", df
        if not isinstance(col, str) or col.strip() == "":
            return 1, "transform_cat_to_num ❌ Error: 'col' debe ser un string no vacío.", df

        # 2) Verificar existencia de la columna
        if col not in df.columns:
            return 2, f"transform_cat_to_num ❌ Error: Columna '{col}' no existe en el DataFrame.", df

        # Obtener valores únicos (excluyendo NaN)
        try:
            unique_vals_series = df[col].dropna()
            unique_vals = unique_vals_series.unique().tolist()
        except Exception as e:
            return 9, f"transform_cat_to_num ❌ Error al obtener valores únicos de '{col}': {e}", df

        # 5) Verificar cantidad de valores únicos
        if len(unique_vals) > 99:
            return 5, f"transform_cat_to_num ❌ Error: Demasiados valores únicos en '{col}' ({len(unique_vals)} > 99).", df

        mapping: Dict[Any, int] = {}

        # 3) Si se proporcionó cat_map, validar formato y contenido
        if cat_map is not None:
            if not isinstance(cat_map, dict) or len(cat_map) == 0:
                return 3, "transform_cat_to_num ❌ Error: cat_map debe ser un dict no vacío {valor: número}.", df
            # Verificar que los valores del dict sean ints válidos >= 1
            invalid_values = [v for v in cat_map.values() if not isinstance(v, int)]
            if invalid_values:
                return 3, f"transform_cat_to_num ❌ Error: Valores de cat_map inválidos (deben ser int >= 1): {invalid_values}.", df

            # Verificar que las claves coincidan con los valores únicos en df[col]
            set_unique = set(unique_vals)
            set_map_keys = set(cat_map.keys())
            if set_map_keys != set_unique:
                return 4, (
                    f"transform_cat_to_num ❌ Error: Las claves de cat_map no coinciden con los valores únicos de '{col}'.\n"
                    f"Valores únicos en df: {set_unique}\n"
                    f"Claves en cat_map: {set_map_keys}"
                ), df

            # Si pasa validación, usar cat_map como mapping, manteniendo el orden de unique_vals
            mapping = {val: cat_map[val] for val in unique_vals}
        else:
            # Construir mapping automáticamente: enumerar unique_vals empezando en 1
            mapping = {val: i + 1 for i, val in enumerate(unique_vals)}

        # 6) Crear la nueva columna
        try:
            df_mod = df.copy()
            new_col = f"{col}_num"
            df_mod[new_col] = df_mod[col].map(mapping).fillna(0).astype(int)
        except Exception as e:
            head_str = df.head(5).to_string(index=False)
            dtypes_str = df.dtypes.astype(str).to_string()
            full_msg = (
                f"transform_cat_to_num ❌ Error al asignar nueva columna '{col}_num': {e}\n\n"
                f"DataFrame.head(5):\n{head_str}\n\n"
                f"Estructura de columnas:\n{dtypes_str}"
            )
            logger.error(full_msg)
            return 9, full_msg, df

        # 7) Éxito: incluir mapping en el mensaje para transparencia
        msg = f"transform_cat_to_num ✅ Columna '{new_col}' creada con éxito. Mapping: {mapping}."
        logger.info(msg)
        return 0, msg, df_mod

    except Exception as e:
        # Error inesperado general
        try:
            head_str = df.head(5).to_string(index=False)
            dtypes_str = df.dtypes.astype(str).to_string()
            full_msg = (
                f"transform_cat_to_num ❌ Error inesperado: {e}\n\n"
                f"DataFrame.head(5):\n{head_str}\n\n"
                f"Estructura de columnas:\n{dtypes_str}"
            )
        except Exception:
            full_msg = f"transform_cat_to_num ❌ Error inesperado: {e} (además falló al mostrar head/dtypes)."
        logger.error(full_msg)
        return 9, full_msg, df
