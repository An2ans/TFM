# tasks/Load/update_summary.py

import pandas as pd
import duckdb
from datetime import datetime
from typing import Tuple
from prefect import task
from datetime import timezone

@task(cache_key_fn=lambda *_: None)
def update_summary(
    df: pd.DataFrame,
    table_id: str,
    table_name: str,
    con: duckdb.DuckDBPyConnection
) -> Tuple[int, str]:
    """
    Inserta o actualiza un registro en 'tables_summary' con:
    Si ya existía un registro con el mismo table_id, lo borra antes de insertar el nuevo.
    Si la tabla no existe, la crea automáticamente.
    Devuelve:
      * (1, mensaje_ok) si la operación tuvo éxito.
      * (0, mensaje_error) si hubo cualquier excepción.
    """
    try:
        row_count = df.shape[0]
        flow_name = "affiliated_flow"
        now_iso   = datetime.now(timezone.utc).isoformat()

        # Crear la tabla si no existe
        con.execute("""
            CREATE TABLE IF NOT EXISTS tables_summary (
                table_id INT PRIMARY KEY,
                table_name TEXT,
                rows BIGINT,
                flow_name TEXT,
                last_update TIMESTAMP
            )
        """)

        # Eliminar posible registro previo con el mismo table_id
        con.execute(
            "DELETE FROM tables_summary WHERE table_id = ?",
            (table_id,)
        )

        # Insertar el nuevo registro
        con.execute(
            """
            INSERT INTO tables_summary
              (table_id, table_name, rows, flow_name, last_update)
            VALUES (?, ?, ?, ?, ?)
            """,
            (table_id, table_name, row_count, flow_name, now_iso)
        )

        return 1, f"✅ 'tables_summary' actualizado para table_id='{table_id}'."

    except Exception as e:
        return 0, f"❌ Error en update_summary: {e}"
