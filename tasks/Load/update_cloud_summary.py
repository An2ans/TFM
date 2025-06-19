# tasks/Load/update_cloud_summary.py

import pandas as pd
from datetime import datetime, timezone
from prefect import task, get_run_logger
from typing import Tuple, Any

@task(cache_key_fn=lambda *args, **kwargs: None)
def update_cloud_summary(
    df: pd.DataFrame,
    table_id: Any,
    table_name: Any,
    con
) -> Tuple[int, str]:
    """
    Actualiza o crea registros en dos tablas de resumen en DuckDB Cloud:
      1) summary_tables:
         - table_id (INTEGER PK)
         - table_name (VARCHAR)
         - rows (BIGINT)
         - columns (INT)
         - last_updated (DATE)
      2) summary_loads:
         - load_id (VARCHAR(20) PK) con formato 'YYYYMMDD-N'
         - table_id (INTEGER)
         - load_rows (BIGINT)  # número de filas del df proporcionado
         - updated_at (DATE)

    Genera load_id consultando los existentes que empiecen por la fecha actual 'YYYYMMDD', 
    extrayendo el sufijo N y usando el siguiente N (si no hay ninguno, N=1).

    Códigos de retorno:
      1 → Parámetros insuficientes o de tipo incorrecto.
      2 → Error testeando/con la conexión `con`.
      3 → Error creando o validando tabla `summary_tables`.
      4 → Error creando o validando tabla `summary_loads`.
      5 → Error al actualizar o insertar registros en alguna de las tablas.
      9 → Otro error inesperado.
      0 → Éxito.
    """
    logger = get_run_logger()

    # 1) Validar parámetros
    if not isinstance(df, pd.DataFrame):
        return 1, "update_cloud_summary ❌ Parámetros inválidos: df debe ser un pandas DataFrame."
    if df.empty:
        return 1, "update_cloud_summary ❌ Parámetros inválidos: df está vacío."
    # table_id debería ser convertible a entero
    try:
        tid_int = int(table_id)
    except Exception:
        return 1, f"update_cloud_summary ❌ Parámetros inválidos: table_id debe ser entero, se recibió: {table_id}"
    if not isinstance(table_name, str) or not table_name.strip():
        return 1, f"update_cloud_summary ❌ Parámetros inválidos: table_name debe ser str no vacío, se recibió: {table_name!r}"
    # con: testeamos con una simple consulta
    try:
        con.execute("SELECT 1").fetchall()
    except Exception as e:
        return 2, f"update_cloud_summary ❌ Error con la conexión a DuckDB Cloud: {e}"

    # 2) Preparar nombres de tablas de resumen
    tbl_summary_tables = "summary_tables"
    tbl_summary_loads  = "summary_loads"

    # 3) Crear o verificar summary_tables
    try:
        # Nota: VARCHAR sin especificar longitud en DuckDB es aceptable; no obstante, si quieres,
        # puedes usar VARCHAR(255). Aquí usamos VARCHAR.
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {tbl_summary_tables} (
                table_id   INTEGER PRIMARY KEY,
                table_name VARCHAR,
                rows       BIGINT,
                columns    INTEGER,
                last_updated DATE
            )
        """)
        logger.info(f"update_cloud_summary ✅ Tabla '{tbl_summary_tables}' verificada/creada.")
    except Exception as e:
        return 3, f"update_cloud_summary ❌ Error creando o validando '{tbl_summary_tables}': {e}"

    # 4) Crear o verificar summary_loads
    try:
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {tbl_summary_loads} (
                load_id    VARCHAR(20) PRIMARY KEY,
                table_id   INTEGER,
                load_rows  BIGINT,
                updated_at DATE
            )
        """)
        logger.info(f"update_cloud_summary ✅ Tabla '{tbl_summary_loads}' verificada/creada.")
    except Exception as e:
        return 4, f"update_cloud_summary ❌ Error creando o validando '{tbl_summary_loads}': {e}"

    # 5) Actualizar/inserción en summary_tables
    try:
        # Obtener el número de filas y columnas de la tabla real en cloud:
        # - Filas: SELECT COUNT(*) FROM table_name
        # - Columnas: consultar information_schema.columns
        # Es posible que la tabla referenciada (table_name) no exista en cloud aún; manejarlo:
        # Primero, verificar si la tabla existe en cloud:
        exists_df = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = ?", (table_name,)
        ).fetchdf()
        if exists_df.empty:
            # La tabla de datos aún no existe en cloud; consideramos rows=0, columns=0 o podemos indicar warning.
            rows_cloud = 0
            columns_cloud = 0
            logger.warning(f"update_cloud_summary ⚠️ La tabla de datos '{table_name}' no existe aún en cloud; se registra rows=0, columns=0 en summary_tables.")
        else:
            # Obtener filas
            res_row = con.execute(f"SELECT COUNT(*) AS cnt FROM {table_name}").fetchdf()
            rows_cloud = int(res_row["cnt"].iloc[0])
            # Obtener columnas
            cols_df = con.execute(
                "SELECT COUNT(*) AS cnt_cols FROM information_schema.columns WHERE table_name = ?", (table_name,)
            ).fetchdf()
            columns_cloud = int(cols_df["cnt_cols"].iloc[0])

        # Verificar si ya hay un registro en summary_tables para este table_id
        existing = con.execute(
            f"SELECT table_id FROM {tbl_summary_tables} WHERE table_id = ?", (tid_int,)
        ).fetchdf()
        today_date = datetime.now(timezone.utc).date().isoformat()  # 'YYYY-MM-DD'
        if existing.empty:
            # Insert nuevo
            con.execute(
                f"INSERT INTO {tbl_summary_tables} (table_id, table_name, rows, columns, last_updated) VALUES (?, ?, ?, ?, ?)",
                (tid_int, table_name, rows_cloud, columns_cloud, today_date)
            )
            logger.info(f"update_cloud_summary ✅ Insertado en '{tbl_summary_tables}': table_id={tid_int}.")
        else:
            # Update existente
            con.execute(
                f"UPDATE {tbl_summary_tables} SET table_name = ?, rows = ?, columns = ?, last_updated = ? WHERE table_id = ?",
                (table_name, rows_cloud, columns_cloud, today_date, tid_int)
            )
            logger.info(f"update_cloud_summary ✅ Update en '{tbl_summary_tables}' para table_id={tid_int}.")
    except Exception as e:
        return 5, f"update_cloud_summary ❌ Error actualizando/inserción en '{tbl_summary_tables}': {e}"

    # 6) Actualizar/inserción en summary_loads con load_id formato YYYYMMDD-N
    try:
        # Fecha actual en formato YYYYMMDD
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        # Buscar load_id existentes que empiecen con 'YYYYMMDD-'
        # En DuckDB, usamos LIKE 'YYYYMMDD-%'
        like_pattern = f"{date_str}-%"
        rows = con.execute(
            f"SELECT load_id FROM {tbl_summary_loads} WHERE load_id LIKE ?", (like_pattern,)
        ).fetchdf()
        if rows.empty:
            next_n = 1
        else:
            # Extraer el sufijo numérico tras '-' y obtener máximo
            # Usamos SPLIT_PART(load_id, '-', 2) que devuelve la parte tras '-'
            # Convertimos a integer y calculamos el máximo +1
            nums = con.execute(
                f"""
                SELECT CAST(SPLIT_PART(load_id, '-', 2) AS INTEGER) AS num
                FROM {tbl_summary_loads}
                WHERE load_id LIKE ?
                """, (like_pattern,)
            ).fetchdf()
            # Puede haber algún valor inesperado, pero asumimos que los existentes cumplen patrón
            # Obtener max:
            max_n = nums["num"].max()
            try:
                max_n_int = int(max_n)
            except Exception:
                max_n_int = 0
            next_n = max_n_int + 1

        new_load_id = f"{date_str}-{next_n}"
        # Insertar nuevo registro en summary_loads
        load_rows = int(df.shape[0])
        updated_at = datetime.now(timezone.utc).date().isoformat()  # 'YYYY-MM-DD'
        con.execute(
            f"INSERT INTO {tbl_summary_loads} (load_id, table_id, load_rows, updated_at) VALUES (?, ?, ?, ?)",
            (new_load_id, tid_int, load_rows, updated_at)
        )
        logger.info(f"update_cloud_summary ✅ Insertado en '{tbl_summary_loads}' load_id={new_load_id}.")
    except Exception as e:
        return 5, f"update_cloud_summary ❌ Error creando o validando '{tbl_summary_loads}': {e}"

    # 7) Todo OK
    msg = (
        f"update_cloud_summary ✅ Resumen actualizado: summary_tables(table_id={tid_int}), "
        f"summary_loads(load_id={new_load_id}, load_rows={load_rows})."
    )
    return 0, msg
