# tasks/Extract/extract_csv.py

import pandas as pd
from pathlib import Path
from typing import Tuple
from prefect import task

@task
def extract_csv(ruta: str, delimitador: str = ";") -> Tuple[int, str, pd.DataFrame]:
    """
    Lee un CSV desde 'ruta' usando el delimitador dado y devuelve (code, message, df).
    
    Códigos de retorno:
      - 1 → Ruta o archivo no existe.
      - 2 → El archivo no es un CSV válido o no se pudo abrir.
      - 9 → Cualquier otro error inesperado.
      - 0 → Éxito.
    """
    path = Path(ruta)
    if not path.exists():
        msg = f"❌ Ruta o archivo no existe: {ruta}"
        return 1, msg, pd.DataFrame()
    try:
        df = pd.read_csv(path, sep=delimitador)
        filas, cols = df.shape
        msg = f"✅ CSV extraído con éxito: {filas} filas, {cols} columnas."
        return 0, msg, df
    except pd.errors.EmptyDataError as e:
        msg = f"❌ El archivo parece estar vacío o no es un CSV válido: {e}"
        return 2, msg, pd.DataFrame()
    except pd.errors.ParserError as e:
        msg = f"❌ El archivo no es un CSV válido o está dañado: {e}"
        return 2, msg, pd.DataFrame()
    except Exception as e:
        msg = f"❌ Error inesperado en extract_csv: {e}"
        return 9, msg, pd.DataFrame()
