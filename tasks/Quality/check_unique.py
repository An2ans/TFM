# tasks/Quality/check_unique_ge.py

import pandas as pd
import great_expectations as ge
from prefect import task
from typing import Tuple, List

@task
def check_unique_ge(df: pd.DataFrame, col: str) -> Tuple[int, str]:
    """
    Usa Great Expectations para verificar unicidad en la columna `col` de `df`.
    - Si TODOS los valores son únicos: devuelve (1, mensaje de OK).
    - Si hay duplicados: devuelve (0, mensaje que lista los valores duplicados y cuántas veces aparecen).
    
    Códigos de retorno:
    - 1 → todos los valores en la columna son únicos.
    - 0 → se hallaron duplicados.
    """

    ge_df = ge.from_pandas(df)

    # Expectation para unicidad
    resultado = ge_df.expect_column_values_to_be_unique(column=col)

    if resultado.success:
        msg = f"✅ Todos los valores de la columna '{col}' son únicos."
        return 1, msg

    # Si no es exitoso, GE en 'unexpected_list' da extracto de valores duplicados
    # O se puede obtener con pandas directamente
    try:
        unexpected = resultado.result.get("unexpected_list")
    except Exception:
        unexpected = None

    if not unexpected:
        # fallback: con pandas obtenemos duplicados exactos
        dup_serie = df[col][df[col].duplicated(keep=False)]
        # le pedimos los valores únicos duplicados
        unexpected = dup_serie.unique().tolist()

    # Construir mensaje con lista de duplicados y conteo
    conteos: List[str] = []
    for val in unexpected:
        count = int((df[col] == val).sum())
        conteos.append(f"'{val}' aparece {count} veces")

    details = "; ".join(conteos)
    msg = f"⚠️ La columna '{col}' tiene duplicados: {details}"
    return 0, msg
