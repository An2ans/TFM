# tasks/Load/connect_cloud_db.py

import os
import duckdb
from prefect import task, get_run_logger
from typing import Tuple, Optional

@task
def connect_cloud_db() -> Tuple[int, str, Optional[duckdb.DuckDBPyConnection]]:
    """
    Intenta conectar a DuckDB Cloud usando:
      1) DUCKDB_CLOUD_CON_STRING (env var) directamente.
      2) Si falla o no existe, construye: "md:<DB_NAME>?motherduck_token=<TOKEN>" a partir
         de DUCKDB_CLOUD_NAME y DUCKDB_CLOUD_TOKEN.
    Realiza un pequeño 'ping' ("SELECT 1") para verificar la conexión.
    
    Devuelve:
      - code=0: conexión exitosa, devuelve (0, mensaje, con)
      - code=1: no pudo conectar tras ambos intentos, devuelve (1, mensaje, None)
    """
    logger = get_run_logger()

    # 1) Leer intento directo
    con_string = os.getenv("DUCKDB_CLOUD_CON_STRING")
    tried = []
    con = None

    def intentar_conexion(conn_str: str) -> Tuple[bool, Optional[duckdb.DuckDBPyConnection], str]:
        """
        Intenta duckdb.connect(conn_str), luego SELECT 1 para verificar.
        Devuelve (True, con, mensaje) o (False, None, mensaje_error).
        """
        try:
            con_local = duckdb.connect(conn_str)
            # Ping sencillo:
            try:
                con_local.execute("SELECT 1").fetchall()
            except Exception as ping_err:
                # cerrar conexión si hay fallo en ping
                try:
                    con_local.close()
                except:
                    pass
                return False, None, f"Falló el ping tras conectar {ping_err}"
            return True, con_local, f"Conexión exitosa."
        except Exception as e:
            return False, None, f"No se pudo conectar: {e}"

    # Intento 1: variable DUCKDB_CLOUD_CON_STRING
    if con_string:
        ok, con_obj, msg = intentar_conexion(con_string)
        tried.append(msg)
        if ok:
            logger.info(msg)
            return 0, f"connect_cloud_db ✅ {msg}", con_obj
        else:
            logger.warning(f"connect_cloud_db ⚠️ Intento con DUCKDB_CLOUD_CON_STRING falló: {msg}")

    # Intento 2: construir desde DUCKDB_CLOUD_NAME y DUCKDB_CLOUD_TOKEN
    cloud_name = os.getenv("DUCKDB_CLOUD_NAME")
    cloud_token = os.getenv("DUCKDB_CLOUD_TOKEN")
    if cloud_name and cloud_token:
        # Construir cadena: según MotherDuck docs: "md:<database_name>?motherduck_token=<token>"
        conn_str2 = f"md:{cloud_name}?motherduck_token={cloud_token}"
        ok2, con_obj2, msg2 = intentar_conexion(conn_str2)
        tried.append(msg2)
        if ok2:
            logger.info(msg2)
            return 0, f"connect_cloud_db ✅ {msg2}", con_obj2
        else:
            logger.warning(f"connect_cloud_db ⚠️ Intento con DUCKDB_CLOUD_NAME/TOKEN falló: {msg2}")
    else:
        # No tenemos suficientes vars de entorno para construir
        tried.append("No se encontró DUCKDB_CLOUD_NAME o DUCKDB_CLOUD_TOKEN en variables de entorno.")

    # Si llegamos aquí, ambos intentos fallaron
    detalles = " | ".join(tried)
    msg_final = f"connect_cloud_db ❌ No se pudo conectar a DuckDB Cloud. Intentos: {detalles}"
    logger.error(msg_final)
    return 1, msg_final, None
