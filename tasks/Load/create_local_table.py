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
) -> Tuple[int, str, Any]:
    """
    0 → Éxito: tabla creada y poblada (devuelve head_df)
    1 → Parámetros inválidos (df o table_name)
    2 → No se pudo conectar / validar la base de datos
    3 → Error al borrar tabla existente
    4 → Error al crear o poblar la tabla
    9 → Otro error inesperado
    """
    # 1) Validación de parámetros
    if not isinstance(df, pd.DataFrame) or df.empty or not isinstance(table_name, str):
        return 1, "❌ create_local_table Error: parámetros inválidos (df debe ser DataFrame no vacío y table_name str).", None

    # 2) Verificar conexión
    try:
        con.execute("SELECT 1").fetchall()
    except Exception as e:
        return 2, f"❌ create_local_table Error conectando a DuckDB: {e}", None

    # 3) Comprobar existencia y borrar si existe
    try:
        exists = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = ?",
            (table_name,)
        ).fetchdf()
    except Exception as e:
        return 2, f"❌ create_local_table Error consultando metadata: {e}", None

    if not exists.empty:
        try:
            con.execute(f"DROP TABLE {table_name}")
        except Exception as e:
            return 3, f"❌ create_local_table Error borrando tabla existente '{table_name}': {e}", None

    # 4) Construir DDL infiriendo tipos
    try:
        type_map = {
            'datetime64[ns]': 'DATE',
            'int64': 'INTEGER',
            'Int64': 'INTEGER',
            'float64': 'DOUBLE',
            'bool': 'BOOLEAN',
            'boolean': 'BOOLEAN'
        }
        def duck_type(dtype: str) -> str:
            return type_map.get(dtype, 'TEXT')

        cols_ddl = []
        for col, dt in df.dtypes.astype(str).items():
            sql_type = duck_type(dt)
            if col == df.columns[0]:
                cols_ddl.append(f"{col} {sql_type} PRIMARY KEY")
            else:
                cols_ddl.append(f"{col} {sql_type}")

        ddl = f"CREATE TABLE {table_name} (\n  " + ",\n  ".join(cols_ddl) + "\n)"
        con.execute(ddl)
    except Exception as e:
        return 4, f"❌ create_local_table Error creando tabla '{table_name}': {e}", None

    # 5) Poblar desde pandas
    try:
        con.register("temp_df", df)
        con.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
    except Exception as e:
        return 4, f"❌ create_local_table Error poblando '{table_name}': {e}", None

    # 6) Leer un head para devolver
    try:
        head_df = con.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchdf()
        return 0, f"✅ create_local_table Tabla '{table_name}' creada y poblada con éxito.", head_df
    except Exception as e:
        return 4, f"❌ create_local_table Error leyendo primeras filas de '{table_name}': {e}", None