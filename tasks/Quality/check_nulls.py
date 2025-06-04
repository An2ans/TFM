# tasks/Quality/check_nulls_ge.py

import pandas as pd
import great_expectations as ge
from prefect import task
from typing import Tuple

@task
def check_nulls_ge(df: pd.DataFrame) -> Tuple[int, str]:
    """
    Usa Great Expectations para verificar valores nulos en cada columna de `df`.
    - Si no hay columnas con nulos: devuelve (0, mensaje).
    - Si hay N columnas con nulos: devuelve (N, mensaje que lista columnas y nulos).
    
    Códigos de retorno:
    - 0 → no hay nulos en ninguna columna.
    - 1, 2, 3,... → número de columnas que tienen al menos un nulo.
    """

    # Convertir el DataFrame normal a un PandasDataset GE
    ge_df = ge.from_pandas(df)

    columnas_con_nulos = []
    detalle = []

    for col in ge_df.columns:
        # Expectation: los valores de la columna NO deben ser null
        resultado = ge_df.expect_column_values_to_not_be_null(column=col)
        if not resultado.success:
            # Contamos cuántos nulos hay (GE devuelve "unexpected_count" en resultado['result'])
            null_count = resultado.result.get("unexpected_count", None)
            # Si Unexpected_count no existe, calculamos con pandas
            if null_count is None:
                null_count = int(df[col].isna().sum())
            columnas_con_nulos.append(col)
            detalle.append(f"'{col}' → {null_count} nulos")

    if not columnas_con_nulos:
        msg = "✅ No se encontraron nulos en ninguna columna."
        return 0, msg

    # Si hay columnas con nulos:
    num_cols = len(columnas_con_nulos)
    cols_lista = "; ".join(detalle)
    msg = f"⚠️ Se encontraron nulos en {num_cols} columna(s): {cols_lista}"
    return num_cols, msg
