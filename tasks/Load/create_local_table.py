# tasks/Load/create_local_table.py

import duckdb
from prefect import task
from typing import Any
import pandas as pd

@task(cache_key_fn=lambda *_: None)
def create_local_table(df: pd.DataFrame, table_name: str, con: duckdb.DuckDBPyConnection) -> tuple[int, str, Any]:
    """
    1) Comprueba si existe table_name en DuckDB.
       - Si existe, devuelve (2, "Ya existe la tabla <nombre>", None).
    2) Si no existe, crea la tabla usando la primera columna como PRIMARY KEY.
       - Si éxito, devuelve (1, "Tabla <nombre> creada exitosamente", head_df).
       - Si hay error, devuelve (0, mensaje_error, None).
    """

    try:
        # Verificar existencia
        existing = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        ).fetchdf()
        # Nota: en DuckDB, la metainformación de tablas va en duckdb_tables o INFORMATION_SCHEMA
        # En versiones recientes de DuckDB podemos usar:
        #   SELECT table_name FROM information_schema.tables WHERE table_name = '<table_name>';
        # Pero para compatibilidad simple:
        tbls = con.execute(
            f"SELECT table_name FROM information_schema.tables WHERE table_name = '{table_name}'"
        ).fetchdf()

        if not tbls.empty:
            return 2, f"🔄 Ya existe la tabla '{table_name}'.", None

        # Crear tabla: la primera columna del df como PK
        pk_col = df.columns[0]
        # Crear la tabla con CREATE TABLE AS
        con.execute(f"""
            CREATE TABLE {table_name} (
                {pk_col} TEXT PRIMARY KEY,
                {', '.join(f'{c} TEXT' for c in df.columns if c != pk_col)}
            )
        """)
        # Insertar datos desde pandas
        con.register("temp_df", df)
        con.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
        head_df = con.execute(f"SELECT * FROM {table_name} LIMIT 5").df()
        return 1, f"✅ Tabla '{table_name}' creada y poblada.", head_df

    except Exception as e:
        return 0, f"❌ Error creando tabla '{table_name}': {e}", None
