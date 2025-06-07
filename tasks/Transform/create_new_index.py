# tasks/Transform/create_new_index.py

import pandas as pd
from typing import Tuple, Any
from prefect import task

@task
def create_new_index(
    df: pd.DataFrame,
    col: str,
    name: str = "Index"
) -> Tuple[int, str, Any ]:
    """
    Crea una nueva columna 'name' basada en la columna `col` de df, garantizando unicidad.
    - Si df es None o está vacío → code=1.
    - Si col no existe en el DataFrame → code=2.
    - Si col existe pero contiene valores con caracteres no permitidos (ni todos dígitos ni texto con guiones/dígitos) → code=3.
    - Cualquier otra excepción inesperada → code=9.
    - Si todo OK → code=0, se devuelve df_modificado y mensaje de éxito.

    Ejemplo de índice para duplicados:
      123, 123, 124, 123 → '1230', '1231', '1240', '1232'
    """
    try:
        # 1) Validar que df existe y no esté vacío
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            return 1, f"❌ Error en create_new_index: DataFrame vacío o inválido.", df

        # 2) Validar que la columna existe
        if col not in df.columns:
            return 2, f"❌ Error en create_new_index: Columna '{col}' no existe en el DataFrame.", df

        # 3) Comprobar que cada valor de la columna sea aceptable:
        #    – Si todos los caracteres son dígitos, está OK.
        #    – Si contiene guiones y el resto dígitos, lo consideramos texto válido.
        #    – En cualquier otro caso (letras sueltas, símbolos, etc.) → error.
        for v in df[col].astype(str):
            if v.isdigit():
                continue
            stripped = v.replace("-", "")
            if stripped.isdigit():
                continue
            # cualquier otro caso no permitido
            return 3, f"❌ Error en create_new_index: Valor inválido en '{col}': '{v}'.", df

        # 4) Construir el nuevo índice con sufijo incremental para duplicados
        df_mod = df.copy()
        counters: dict[str, int] = {}

        def make_index(val_str: str) -> str:
            cnt = counters.get(val_str, 0)
            counters[val_str] = cnt + 1
            return f"{val_str}{cnt}"

        df_mod[name] = df_mod[col].astype(str).apply(make_index)

        # 5) Reordenar columnas para que 'name' quede al principio
        cols = df_mod.columns.tolist()
        cols.remove(name)
        new_order = [name] + cols
        df_mod = df_mod[new_order]

        return 0, f"✅ Nuevo índice '{name}' creado con éxito basándose en columna '{col}'.", df_mod

    except Exception as e:
        # Código 9 para cualquier otra excepción inesperada
        return 9,  f"❌ Error inesperado en create_new_index para '{col}': {e}", df
