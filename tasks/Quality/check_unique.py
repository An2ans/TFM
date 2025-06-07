# tasks/Quality/check_unique_ge.py

import pandas as pd
import great_expectations as ge
from prefect import task, get_run_logger
from typing import Tuple, List, Any

@task
def check_unique(df: Any, col: str) -> Tuple[int, str]:
    """
    Usa Great Expectations para verificar unicidad en la columna `col` de `df`.
    - Siempre devuelve code=0 si la tarea se ejecuta correctamente (aunque existan duplicados).
      El mensaje indicará si todos los valores son únicos o listará los duplicados encontrados.
    - Si ocurre un error interno:
        * code=1 si `df` no es un DataFrame válido.
        * code=2 si la columna `col` no existe en `df`.
        * code=9 para cualquier otro error inesperado.

    En caso de duplicados, se emite un logger.warning con los detalles, 
    pero el flujo continúa (devuelve siempre código 0 salvo error interno).
    """

    logger = get_run_logger()

    # 1) Validar que df es un DataFrame
    if not isinstance(df, pd.DataFrame):
        return 1, "❌ check_unique: el objeto proporcionado no es un DataFrame válido."

    # 2) Validar que la columna existe
    if col not in df.columns:
        return 2, f"❌ check_unique: la columna '{col}' no existe en el DataFrame."

    try:
        # 3) Crear un DataFrame de Great Expectations
        ge_df = ge.from_pandas(df)

        # 4) Expectation para unicidad
        resultado = ge_df.expect_column_values_to_be_unique(column=col)

        if resultado.success:
            mensaje = f"✅ Todos los valores de la columna '{col}' son únicos."
            return 0, mensaje

        # 5) Si hay duplicados, extraer lista de valores duplicados
        try:
            unexpected = resultado.result.get("unexpected_list", [])
        except Exception:
            unexpected = []

        if not unexpected:
            # Fallback usando pandas directamente
            dup_serie = df[col][df[col].duplicated(keep=False)]
            unexpected = dup_serie.unique().tolist()

        # 6) Construir mensaje con conteo de duplicados
        conteos: List[str] = []
        for val in unexpected:
            count = int((df[col] == val).sum())
            conteos.append(f"'{val}' aparece {count} veces")
        detalles = "; ".join(conteos)

        warning_msg = f"⚠️ La columna '{col}' tiene duplicados: {detalles}"
        logger.warning(warning_msg)

        # 7) Aunque haya duplicados, devolvemos code=0 y el mensaje de advertencia
        return 0, warning_msg

    except Exception as e:
        # 8) Cualquier otro error inesperado
        return 9, f"❌ check_unique: error inesperado → {e}"
