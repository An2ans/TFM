# tasks/Load/update_local_table.py

import duckdb
from prefect import task
import pandas as pd

@task(cache_key_fn=lambda *_: None)
def update_local_table(df: pd.DataFrame, table_name: str, con: duckdb.DuckDBPyConnection) -> tuple[int, str]:
    """
    1) Comprueba si existe table_name. Si no, devuelve (0, mensaje_error).
    2) Si existe, borra todos los registros y los sustituye por los de df.
       - Si éxito, devuelve (1, mensaje_ok).
       - Si error, devuelve (0, mensaje_error).
    """

    try:
        tbls = con.execute(
            f"SELECT table_name FROM information_schema.tables WHERE table_name = '{table_name}'"
        ).fetchdf()
        if tbls.empty:
            return 0, f"❌ La tabla '{table_name}' no existe."

        # Borrar todos los registros
        con.execute(f"DELETE FROM {table_name}")

        # Insertar los nuevos datos
        con.register("temp_df_update", df)
        con.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df_update")
        return 1, f"✅ Datos de la tabla '{table_name}' actualizados."

    except Exception as e:
        return 0, f"❌ Error actualizando datos de '{table_name}': {e}"
