import pandas as pd
from pathlib import Path

def import_csv(ruta: str, delimitador: str = ";") -> pd.DataFrame:
    """
    Lee un CSV desde 'ruta' usando el delimitador dado y devuelve un DataFrame.
    """
    path = Path(ruta)
    if not path.exists():
        raise FileNotFoundError(f"No existe el fichero: {ruta}")
    df = pd.read_csv(path, sep=delimitador)
    return df
