import duckdb
from pathlib import Path
from typing import Any

def connect_duckdb(db_path: str = None, **connect_kwargs) -> Any:
    """
    Abre una conexión a DuckDB.
    - Si db_path es None, usa ':memory:' (in-memory).
    - Si es una ruta, crea/usa ese archivo .duckdb.
    - kwargs permiten pasar opciones (read_only, config, etc).
    """
    if db_path:
        db_file = Path(db_path)
        # crea carpeta si no existe
        db_file.parent.mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect(str(db_file), **connect_kwargs)
    else:
        conn = duckdb.connect(database=":memory:", **connect_kwargs)
    print(f"[DuckDB] Conectado a {conn.database}")
    return conn
