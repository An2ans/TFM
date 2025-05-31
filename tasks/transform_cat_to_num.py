import pandas as pd
from typing import Dict

def transform_cat_to_num(df: pd.DataFrame, col: str) -> Tuple[pd.DataFrame, Dict[str,int]]:
    """
    Mapea los valores únicos de la columna categórica df[col] a números empezando en 1.
    Devuelve (df_modificado, mapping) donde mapping es {valor_categórico: número}.
    """
    if col not in df.columns:
        raise KeyError(f"Columna '{col}' no existe en el DataFrame")
    df = df.copy()
    unique_vals = df[col].dropna().unique().tolist()
    mapping = {val: i+1 for i, val in enumerate(unique_vals)}
    # Asignamos 0 a NaN o valores no mapeados
    df[col] = df[col].map(mapping).fillna(0).astype(int)
    return df, mapping
