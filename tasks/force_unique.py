import pandas as pd
from typing import Tuple

def force_unique(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Garantiza que los valores en df[col] sean únicos.
    Si aparece un duplicado, añade ' - N' donde N es el contador de duplicados.
    Ejemplo: ['A', 'A', 'B', 'A'] → ['A', 'A - 2', 'B', 'A - 3']
    """
    if col not in df.columns:
        raise KeyError(f"Columna '{col}' no encontrada en el DataFrame")

    counts = {}
    new_values = []
    for val in df[col].fillna("").astype(str):
        counts[val] = counts.get(val, 0) + 1
        if counts[val] == 1:
            new_values.append(val)
        else:
            new_values.append(f"{val} - {counts[val]}")
    df = df.copy()
    df[col] = new_values
    return df
