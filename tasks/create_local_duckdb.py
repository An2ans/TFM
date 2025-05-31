import duckdb
from pathlib import Path

def create_local_duckdb(db_dir: str, db_name: str = "mi_datawarehouse.duckdb"):
    """
    Crea (o abre) un archivo DuckDB en db_dir/db_name.
    """
    path = Path(db_dir) / db_name
    path.parent.mkdir(parents=True, exist_ok=True)
    # Conexión: si no existe, crea el archivo; si existe, lo abre
    conn = duckdb.connect(str(path))
    print(f"[DuckDB] Base de datos creada/abierta en: {path}")
    return conn

if __name__ == "__main__":
    conn = create_local_duckdb(
        r"C:\Users\anton\OneDrive - UNIR\Equipo\TFM2\DB"
    )
    # Prueba rápida
    print(conn.execute("SELECT 42 AS test").fetchall())
    conn.close()
