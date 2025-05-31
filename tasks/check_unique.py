import pandas as pd

def check_unique(df: pd.DataFrame, col: str) -> bool:
    """
    Devuelve True si todos los valores de la columna 'col' son únicos,
    False en caso contrario.
    """
    if col not in df.columns:
        raise KeyError(f"La columna '{col}' no existe en el DataFrame.")
    total = len(df)
    unique = df[col].nunique(dropna=False)
    return unique == total
