# tasks/Load/connect_local_duckdb.py

import duckdb
from pathlib import Path
from prefect import task
from typing import Tuple, Any

@task
def connect_local_duckdb(ruta: str) -> Tuple[int, str, Any]:
    """
    Comprueba que la carpeta de `ruta` exista, luego:
      - Si el archivo DuckDB no existe, lo crea.
      - Se conecta al archivo DuckDB y devuelve (code, message, con).
    Devuelve:
      * code = 1 si se conectó o creó con éxito, 0 si error.
      * message = descripción del resultado o error.
      * con = DuckDBPyConnection (o None si hubo error).
    """
    try:
        db_path = Path(ruta)
        parent_dir = db_path.parent
        if not parent_dir.exists():
            msg = f"La carpeta '{parent_dir}' no existe."
            return 1, msg, None

        if not db_path.exists():
            # El archivo no existe: DuckDB.create automáticamente al conectar
            con = duckdb.connect(str(db_path))
            msg = f"✅ Archivo '{db_path.name}' creado con éxito en '{parent_dir}'."
            return 0, msg, con
        else:
            # Ya existe: solo conectar
            con = duckdb.connect(str(db_path))
            msg = f"✅ Conectado con éxito al archivo '{db_path.name}'."
            return 0, msg, con

    except Exception as e:
        err = f"❌ Error en connect_local_duckdb: {e}"
        return 1, err, None
