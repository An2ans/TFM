# tasks/Quality/check_datatypes.py

import pandas as pd
import great_expectations as ge
from prefect import task, get_run_logger
from typing import Tuple, Any, Dict

@task
def check_datatypes(
    df: pd.DataFrame,
    expected_types: Dict[str, str]
) -> Tuple[int, pd.DataFrame, str]:
    """
    Verifica que `df` tenga exactamente las columnas y tipos indicados en `expected_types`,
    en el orden especificado. Si faltan o sobran columnas, reporta error. Si el orden es distinto,
    reordena. Si el tipo no coincide, intenta convertir y reporta el cambio.

    expected_types: { column_name: type_string }, donde type_string ∈ {"str", "string", "int", "integer", "float", "number", "bool", "boolean", "datetime"}

    Devuelve:
      * code = 1 si logra dejar df con columnas y tipos correctos.
      * df_mod = DataFrame ajustado.
      * message = HEAD(5) + ESTRUCTURA O LISTA DE ERRORES seguidos de descriptions de cambios.
      * code = 0 si error bloqueante, con mensaje que incluye HEAD(5) y estructura.
    """
    logger = get_run_logger()
    try:
        # 1. Verificar columnas exactas (sin extras ni faltantes)
        expected_cols = list(expected_types.keys())
        actual_cols = list(df.columns)

        missing = [c for c in expected_cols if c not in actual_cols]
        extra   = [c for c in actual_cols if c not in expected_cols]
        if missing or extra:
            msg = ""
            if missing:
                msg += f"❌ Faltan columnas: {missing}. "
            if extra:
                msg += f"❌ Columnas inesperadas: {extra}. "
            head_str = df.head(5).to_string(index=False)
            dtypes_str = df.dtypes.astype(str).to_string()
            full_msg = (
                f"{msg}\n\n"
                f"DataFrame.head(5):\n{head_str}\n\n"
                f"Estructura de columnas:\n{dtypes_str}"
            )
            return 0, df, full_msg

        # 2. Reordenar si el orden difiere
        if actual_cols != expected_cols:
            df = df[expected_cols]
            logger.info(f"🔄 Columnas reordenadas a {expected_cols}.")

        # 3. Mapear cadenas a tipos Python/numpy
        type_map = {
            "str": str, "string": str,
            "int": int, "integer": int,
            "float": float, "number": float,
            "bool": bool, "boolean": bool,
            "datetime": "datetime"
        }

        changes = []
        for col, type_str in expected_types.items():
            requested = type_str.lower()
            if requested not in type_map:
                raise ValueError(f"Unrecognized type '{type_str}' para columna '{col}'")
            target_type = type_map[requested]

            current_dtype = df[col].dtype
            # Check datetime separately
            if target_type == "datetime":
                if not pd.api.types.is_datetime64_any_dtype(current_dtype):
                    df[col] = pd.to_datetime(df[col], errors="raise")
                    changes.append(f"Columna '{col}' convertida a datetime")
            else:
                # Para tipos numéricos y de texto
                if requested in ("int", "integer"):
                    if not pd.api.types.is_integer_dtype(current_dtype):
                        df[col] = df[col].astype(int)
                        changes.append(f"Columna '{col}' convertida a int")
                elif requested in ("float", "number"):
                    if not pd.api.types.is_float_dtype(current_dtype):
                        df[col] = df[col].astype(float)
                        changes.append(f"Columna '{col}' convertida a float")
                elif requested in ("bool", "boolean"):
                    if not pd.api.types.is_bool_dtype(current_dtype):
                        df[col] = df[col].astype(bool)
                        changes.append(f"Columna '{col}' convertida a bool")
                else:  # str / string
                    if not pd.api.types.is_object_dtype(current_dtype):
                        df[col] = df[col].astype(str)
                        changes.append(f"Columna '{col}' convertida a string")

        # 4. Construir mensaje final
        head_str = df.head(5).to_string(index=False)
        dtypes_str = df.dtypes.astype(str).to_string()

        if changes:
            changes_str = "\n".join(f"✅ {c}" for c in changes)
            msg = (
                f"✅ check_datatypes completado. Se aplicaron cambios:\n"
                f"{changes_str}\n\n"
                f"DataFrame.head(5):\n{head_str}\n\n"
                f"Estructura de columnas:\n{dtypes_str}"
            )
        else:
            msg = (
                f"✅ check_datatypes completado. No fue necesario ningún cambio.\n\n"
                f"DataFrame.head(5):\n{head_str}\n\n"
                f"Estructura de columnas:\n{dtypes_str}"
            )

        return 1, df, msg

    except Exception as e:
        head_str = df.head(5).to_string(index=False)
        dtypes_str = df.dtypes.astype(str).to_string()
        full_msg = (
            f"❌ Error en check_datatypes: {e}\n\n"
            f"DataFrame.head(5):\n{head_str}\n\n"
            f"Estructura de columnas:\n{dtypes_str}"
        )
        return 0, df, full_msg
