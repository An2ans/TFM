# tasks/Load/insert_added_date.py

import duckdb
from datetime import datetime
from prefect import task

@task(cache_key_fn=lambda *_: None)
def insert_added_date(table_name: str, con: duckdb.DuckDBPyConnection) -> tuple[int, str]:
    """
    1) Comprueba si existe table_name. Si no, devuelve (0, mensaje).
    2) Si existe, añade columna 'added_date' con valor CURRENT_DATE (o la fecha actual).
       - Si éxito, devuelve (1, mensaje_ok).
       - Si error, devuelve (0, mensaje_error).
    """

    try:
        tbls = con.execute(
            f"SELECT table_name FROM information_schema.tables WHERE table_name = '{table_name}'"
        ).fetchdf()
        if tbls.empty:
            return 0, f"❌ La tabla '{table_name}' no existe."

        # Añadimos la columna; si ya existe, atrapamos el error
        con.execute(f"ALTER TABLE {table_name} ADD COLUMN added_date DATE")
        # Rellenamos con la fecha actual para todas las filas
        today = datetime.utcnow().date()
        con.execute(f"UPDATE {table_name} SET added_date = '{today}'")
        return 1, f"✅ Columna 'added_date' añadida correctamente a '{table_name}'."

    except Exception as e:
        return 0, f"❌ Error en insert_added_date para '{table_name}': {e}"
