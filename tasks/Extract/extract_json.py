# tasks/Extract/extract_json.py

import json
import re
from pathlib import Path
from typing import Dict, Tuple
import pandas as pd
from prefect import task, get_run_logger

@task
def extract_json(
    path: str,
    settings: Dict[str, str]
) -> Tuple[int, str, pd.DataFrame]:
    """
    Lee un JSON en `path` y extrae solo las columnas de `settings` (col: tipo).
    - 1: ruta/archivo no existe
    - 2: no es JSON válido
    - 3: faltan columnas solicitadas
    - 9: otro error
    - 0: OK
    """
    logger = get_run_logger()
    task_name = "extract_json"

    try:
        json_path = Path(path)
        if not json_path.exists():
            return 1, f"{task_name} ❌ Ruta o archivo no existe: {path}", pd.DataFrame()

        # Cargar JSON
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            return 2, f"{task_name} ❌ El archivo no es un JSON válido o no se pudo abrir: {e}", pd.DataFrame()

        # Normalizar
        try:
            df_full = pd.json_normalize(data)
        except Exception as e:
            return 9, f"{task_name} ❌ Error al convertir JSON a DataFrame: {e}", pd.DataFrame()

        # Comprobar columnas
        missing = [c for c in settings if c not in df_full.columns]
        if missing:
            ejemplo = (data[0] if isinstance(data, list) and data else data)
            return 3, (
                f"{task_name} ❌ No se pudieron extraer las columnas pedidas: {missing}. "
                f"Primer objeto JSON: {ejemplo}"
            ), pd.DataFrame()

        # Extraer y convertir
        df = df_full[list(settings.keys())].copy()
        for col, dtype in settings.items():
            dt = dtype.lower()
            # si el tipo es texto, unimos listas y convertimos todo a str
            if dt in ("str", "string", "object"):
                df[col] = df[col].apply(
                    lambda v: " , ".join(v) if isinstance(v, list) else str(v)
                )
                continue

            if dt == "date":
                # Warning y eliminar no-dígitos
                logger.warning(f"{task_name} ⚠️ No se puede convertir en formato date, "
                               f"utilice transform_date en su lugar. Columna '{col}' "
                               f"convertida en numérico eliminando no-dígitos.")
                df[col] = (
                    df[col]
                    .astype(str)
                    .apply(lambda s: re.sub(r"\D", "", s))
                    .replace("", pd.NA)
                )
                continue

            try:
                if dt in ("int", "integer"):
                    df[col] = pd.to_numeric(df[col], errors="raise").astype("Int64")
                elif dt in ("float", "double"):
                    df[col] = pd.to_numeric(df[col], errors="raise").astype(float)
                elif dt in ("str", "string", "object"):
                    df[col] = df[col].astype(str)
                elif dt in ("bool", "boolean"):
                    df[col] = df[col].astype(bool)
                else:
                    logger.warning(f"{task_name} ⚠️ Tipo desconocido para '{col}': {dtype}. "
                                   "Se mantiene sin conversión.")
            except Exception as e:
                return 9, f"{task_name} ❌ Error al convertir columna '{col}' al tipo '{dtype}': {e}", pd.DataFrame()

        msg = f"{task_name} ✅ JSON extraído correctamente: {len(df)} registros y {df.shape[1]} columnas."
        return 0, msg, df

    except Exception as e:
        return 9, f"{task_name} ❌ Ha habido un error inesperado en extract_json: {e}", pd.DataFrame()
