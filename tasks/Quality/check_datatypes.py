# tasks/Quality/check_datatypes.py

import pandas as pd
import great_expectations as ge
from prefect import task, get_run_logger
from typing import Tuple, Dict

@task
def check_datatypes(
    df: pd.DataFrame,
    expected_types: Dict[str, str]
) -> Tuple[int, str, pd.DataFrame]:

    # Validaciones iniciales
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return 1, "❌ DataFrame de entrada no existe o está vacío.", df

    if expected_types is None or not isinstance(expected_types, dict) or not expected_types:
        return 2, "❌ expected_types debe ser un diccionario no vacío.", df

    expected_cols = list(expected_types.keys())
    actual_cols = list(df.columns)

    # 1) Verificar que todas las columnas de expected existan en df
    for col in expected_cols:
        if col not in actual_cols:
            return 3, f"❌ Columna esperada '{col}' no existe en el DataFrame.", df

    # 2) Verificar que todos los tipos estén soportados
    type_map = {
        "str": str, "string": str,
        "int": int, "integer": int,
        "float": float, "number": float,
        "bool": bool, "boolean": bool,
        "datetime": "datetime"
    }

    for col, type_str in expected_types.items():
        if type_str.lower() not in type_map:
            return 4, f"❌ Tipo no reconocido para columna '{col}': '{type_str}'", df

    # 3) Validar tipos con Great Expectations
    try:
        gdf = ge.from_pandas(df)
        ge_results = {}
        for col, type_str in expected_types.items():
            if type_str.lower() == "datetime":
                result = gdf.expect_column_values_to_match_strftime_format(col, "%Y-%m-%d", mostly=0.9, result_format="COMPLETE")
            else:
                result = gdf.expect_column_values_to_be_of_type(col, type_str.lower(), result_format="COMPLETE")
            ge_results[col] = result
    except Exception as e:
        return 9, f"❌ Error ejecutando validaciones con GE: {e}", df

    # 4) Transformaciones necesarias
    changes = []
    df_mod = df.copy()
    for col, type_str in expected_types.items():
        if not ge_results[col]["success"]:
            try:
                # Convertir columna
                canonical = type_str.lower()
                if canonical in ["int", "integer"]:
                    df_mod[col] = pd.to_numeric(df_mod[col], errors="coerce").astype("Int64")
                    changes.append(f"Columna '{col}' convertida a Int64")
                elif canonical in ["float", "number"]:
                    df_mod[col] = pd.to_numeric(df_mod[col], errors="coerce").astype(float)
                    changes.append(f"Columna '{col}' convertida a float64")
                elif canonical in ["bool", "boolean"]:
                    df_mod[col] = df_mod[col].astype("boolean")
                    changes.append(f"Columna '{col}' convertida a boolean")
                elif canonical == "datetime":
                    df_mod[col] = pd.to_datetime(df_mod[col], errors="coerce")
                    changes.append(f"Columna '{col}' convertida a datetime")
                elif canonical in ["str", "string"]:
                    df_mod[col] = df_mod[col].astype(str)
                    changes.append(f"Columna '{col}' convertida a string")
            except Exception as e:
                return 5, f"❌ Error al convertir columna '{col}' a tipo '{type_str}': {e}", df

    # 5) Reordenar si es necesario
    if list(df_mod.columns) != expected_cols:
        df_mod = df_mod[expected_cols]
        changes.append(f"🔄 Columnas reordenadas a: {expected_cols}")

    # 6) Construir mensaje
    head_str = df_mod.head(5).to_string(index=False)
    dtypes_str = df_mod.dtypes.astype(str).to_string()
    if changes:
        msg = (
            f"✅ check_datatypes: cambios aplicados:\n"
            + "\n".join(f"   • {c}" for c in changes) +
            f"\n\nDataFrame.head(5):\n{head_str}\n\nEstructura:\n{dtypes_str}"
        )
    else:
        msg = (
            f"✅ check_datatypes: no se detectaron problemas ni se aplicaron cambios.\n\n"
            f"DataFrame.head(5):\n{head_str}\n\nEstructura:\n{dtypes_str}"
        )

    return 0, msg, df_mod
