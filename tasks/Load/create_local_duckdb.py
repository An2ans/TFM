# tasks/Load/create_local_duckdb.py

import duckdb
import os
from pathlib import Path
from prefect import task, get_run_logger
from typing import Tuple, Any

@task(cache_key_fn=lambda *_: None)
def create_local_duckdb(ruta: str) -> Tuple[int, str, Any]:
    """
    1) Si existe el archivo en `ruta`, lo borra y crea uno nuevo. Si no existe, lo crea.
       - Devuelve (1, "Nueva DB creada en 'ruta'", con).
    2) Tras conectar, crea la tabla vacía 'tables_summary' con columnas:
                table_id INT PRIMARY KEY,
                table_name TEXT,
                rows BIGINT,
                flow_name TEXT,
                last_update TIMESTAMP
        - Devuelve (1, "DB reinicializada y 'tables_summary' creada", con).
    3) Si hay algún error, devuelve (0, mensaje_error, None).
    """

    logger = get_run_logger()
    try:
        db_path = Path(ruta)
        parent = db_path.parent
        if not parent.exists():
            raise FileNotFoundError(f"La carpeta '{parent}' no existe.")

        # 1) Si el archivo existe, borrarlo
        if db_path.exists():
            os.remove(db_path)
            logger.info(f"ℹ️ Archivo existente '{db_path.name}' eliminado.")

        # Crear (o recrear) la base de datos DuckDB
        con = duckdb.connect(str(db_path))
        msg_create = f"✅ Nueva DB DuckDB creada en '{db_path}'."
        logger.info(msg_create)

        # 2) Crear la tabla tables_summary
        con.execute("""
            CREATE TABLE tables_summary (
                table_id    INT PRIMARY KEY,
                table_name  TEXT,
                rows        BIGINT,
                flow_name   TEXT,
                last_update TIMESTAMP
            )
        """)
        msg_table = "✅ Tabla 'tables_summary' vacía creada con las 5 columnas."
        logger.info(msg_table)

        return 1, f"{msg_create} {msg_table}", con

    except Exception as e:
        err = f"❌ Error en create_local_duckdb: {e}"
        logger.error(err)
        return 0, err, None
