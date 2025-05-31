import pandas as pd
from typing import Dict

def check_nulls(df: pd.DataFrame) -> Dict[str, int]:
    """
    Recorre todas las columnas del DataFrame y devuelve un dict
    { nombre_columna: nº_nulls }.
    """
    null_counts = df.isnull().sum().to_dict()
    # Opcional: filtrar solo las columnas con nulls > 0
    return {col: cnt for col, cnt in null_counts.items() if cnt > 0}
