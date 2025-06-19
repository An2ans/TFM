# tasks/Load/create_local_table.py

import duckdb
import pandas as pd
from prefect import task
from typing import Tuple, Any

@task(cache_key_fn=lambda *_: None)
def create_local_table(
    df: pd.DataFrame,
    table_name: str,
    con: duckdb.DuckDBPyConnection
) -> Tuple[int, str, pd.DataFrame]:
    """
    0 → Éxito: tabla creada y poblada (devuelve head_df para preview).
    1 → Parámetros inválidos (df o table_name).
    2 → No se pudo conectar / validar la base de datos.
    3 → Error al borrar tabla existente.
    4 → Error al crear o poblar la tabla.
    9 → Otro error inesperado.
    Siempre retorna (code, mensaje, df_para_preview).
    """
    # Para devolver en errores un DataFrame vacío:
    empty_df = pd.DataFrame()

    # 1) Validación de parámetros
    if not isinstance(df, pd.DataFrame) or df.empty or not isinstance(table_name, str) or not table_name:
        msg = "❌ create_local_table Error: parámetros inválidos (df debe ser DataFrame no vacío y table_name str no vacío)."
        return 1, msg, empty_df

    # 2) Verificar conexión básica
    try:
        # probamos una consulta sencilla
        con.execute("SELECT 1").fetchall()
    except Exception as e:
        msg = f"❌ create_local_table Error conectando a DuckDB: {e}"
        return 2, msg, empty_df

    # 3) Comprobar existencia y borrar si existe
    try:
        exists = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = ?",
            (table_name.upper(),)  # DuckDB suele almacenar mayúsculas, convención
        ).fetchdf()
    except Exception as e:
        msg = f"❌ create_local_table Error consultando metadata: {e}"
        return 2, msg, empty_df

    try:
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
    except Exception as e:
        return 3, f"❌ create_local_table Error borrando tabla existente '{table_name}': {e}", pd.DataFrame()

    # 4) Construir DDL infiriendo tipos a partir de df.dtypes
    try:
        type_map = {
            'datetime64[ns]': 'DATE',
            'datetime64[ns, UTC]': 'TIMESTAMP',  # si hay timezone
            'int64': 'INTEGER',
            'Int64': 'INTEGER',
            'float64': 'DOUBLE',
            'bool': 'BOOLEAN',
            'boolean': 'BOOLEAN'
        }
        def duck_type(dtype_str: str) -> str:
            return type_map.get(dtype_str, 'TEXT')

        cols_ddl = []
        # Recorremos columnas en orden
        for idx, (col, dt) in enumerate(df.dtypes.astype(str).items()):
            sql_type = duck_type(dt)
            if idx == 0:
                # primera columna como PRIMARY KEY
                cols_ddl.append(f"{col} {sql_type} PRIMARY KEY")
            else:
                cols_ddl.append(f"{col} {sql_type}")

        ddl = f"CREATE TABLE {table_name} (\n  " + ",\n  ".join(cols_ddl) + "\n)"
        con.execute(ddl)
    except Exception as e:
        msg = f"❌ create_local_table Error creando tabla '{table_name}': {e}"
        return 4, msg, empty_df

    # 5) Poblar desde pandas
    try:
        # Registramos el DataFrame temporalmente
        con.register("temp_df", df)
        con.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
    except Exception as e:
        msg = f"❌ create_local_table Error poblando '{table_name}': {e}"
        return 4, msg, empty_df

    # 6) Leer un head para devolver
    try:
        head_df = con.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchdf()
        msg = f"✅ create_local_table: Tabla '{table_name}' creada y poblada con éxito."
        return 0, msg, head_df
    except Exception as e:
        msg = f"❌ create_local_table Error leyendo primeras filas de '{table_name}': {e}"
        return 4, msg, empty_df

    # 7) En caso muy inesperado:
    # return 9, "❌ create_local_table Error inesperado.", empty_df
