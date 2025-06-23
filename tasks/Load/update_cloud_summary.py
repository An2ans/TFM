# tasks/Load/update_cloud_summary.py

import pandas as pd
from datetime import datetime, timezone
from prefect import task, get_run_logger
from typing import Tuple, Any, Dict

@task(cache_key_fn=lambda *args, **kwargs: None)
def update_cloud_summary(
    load_report: Dict[str, int],
    table_id: Any,
    table_name: Any,
    con
) -> Tuple[int, str]:
    logger = get_run_logger()

    # 1) Validar parámetros
    if not isinstance(load_report, dict):
        return 1, "update_cloud_summary ❌ Parámetros inválidos: load_report debe ser un dict."
    for key in ["total_inserted", "total_updated", "total_ignored"]:
        if key not in load_report:
            return 1, f"update_cloud_summary ❌ Faltan métricas en load_report: falta '{key}'"

    try:
        tid_int = int(table_id)
    except Exception:
        return 1, f"update_cloud_summary ❌ table_id debe ser entero, se recibió: {table_id}"

    if not isinstance(table_name, str) or not table_name.strip():
        return 1, f"update_cloud_summary ❌ table_name inválido: '{table_name!r}'"

    try:
        con.execute("SELECT 1").fetchall()
    except Exception as e:
        return 2, f"update_cloud_summary ❌ Error con la conexión: {e}"

    # 2) Tablas de resumen
    tbl_summary_tables = "summary_tables"
    tbl_summary_loads = "summary_loads"

    # 3) Crear summary_tables si no existe
    try:
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {tbl_summary_tables} (
                table_id   INTEGER PRIMARY KEY,
                table_name VARCHAR,
                rows       BIGINT,
                columns    INTEGER,
                last_updated DATE
            )
        """)
        logger.info(f"✅ Tabla '{tbl_summary_tables}' verificada/creada.")
    except Exception as e:
        return 3, f"❌ Error creando/verificando '{tbl_summary_tables}': {e}"

    # 4) Crear summary_loads con nuevas columnas
    try:
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {tbl_summary_loads} (
                load_id        VARCHAR(20) PRIMARY KEY,
                table_id       INTEGER,
                rows_inserted  BIGINT,
                rows_updated   BIGINT,
                rows_ignored   BIGINT,
                updated_at     DATE
            )
        """)
        logger.info(f"✅ Tabla '{tbl_summary_loads}' verificada/creada.")
    except Exception as e:
        return 4, f"❌ Error creando/verificando '{tbl_summary_loads}': {e}"

    # 5) Actualizar summary_tables
    try:
        exists_df = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = ?", (table_name,)
        ).fetchdf()
        if exists_df.empty:
            rows_cloud = 0
            columns_cloud = 0
            logger.warning(f"⚠️ La tabla '{table_name}' no existe aún en cloud; filas=0, columnas=0.")
        else:
            rows_cloud = int(con.execute(f"SELECT COUNT(*) AS cnt FROM {table_name}").fetchdf()["cnt"].iloc[0])
            columns_cloud = int(con.execute(
                "SELECT COUNT(*) AS cnt_cols FROM information_schema.columns WHERE table_name = ?",
                (table_name,)
            ).fetchdf()["cnt_cols"].iloc[0])

        today = datetime.now(timezone.utc).date().isoformat()
        existing = con.execute(
            f"SELECT table_id FROM {tbl_summary_tables} WHERE table_id = ?", (tid_int,)
        ).fetchdf()
        if existing.empty:
            con.execute(
                f"INSERT INTO {tbl_summary_tables} (table_id, table_name, rows, columns, last_updated) VALUES (?, ?, ?, ?, ?)",
                (tid_int, table_name, rows_cloud, columns_cloud, today)
            )
            logger.info(f"✅ Insertado en '{tbl_summary_tables}': table_id={tid_int}.")
        else:
            con.execute(
                f"UPDATE {tbl_summary_tables} SET table_name = ?, rows = ?, columns = ?, last_updated = ? WHERE table_id = ?",
                (table_name, rows_cloud, columns_cloud, today, tid_int)
            )
            logger.info(f"✅ Actualizado '{tbl_summary_tables}' para table_id={tid_int}.")
    except Exception as e:
        return 5, f"❌ Error actualizando '{tbl_summary_tables}': {e}"

    # 6) Insertar en summary_loads con load_id = YYYYMMDD-N
    try:
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        like_pattern = f"{date_str}-%"
        nums = con.execute(
            f"""
            SELECT CAST(SPLIT_PART(load_id, '-', 2) AS INTEGER) AS num
            FROM {tbl_summary_loads}
            WHERE load_id LIKE ?
            """, (like_pattern,)
        ).fetchdf()
        max_n = nums["num"].max() if not nums.empty else 0
        next_n = int(max_n) + 1
        load_id = f"{date_str}-{next_n}"
        today_date = datetime.now(timezone.utc).date().isoformat()

        con.execute(
            f"""
            INSERT INTO {tbl_summary_loads} 
            (load_id, table_id, rows_inserted, rows_updated, rows_ignored, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                load_id, tid_int,
                load_report["total_inserted"],
                load_report["total_updated"],
                load_report["total_ignored"],
                today_date
            )
        )
        logger.info(
            f"✅ Insertado en '{tbl_summary_loads}' load_id={load_id}, "
            f"inserted={load_report['total_inserted']}, updated={load_report['total_updated']}, ignored={load_report['total_ignored']}"
        )
    except Exception as e:
        return 5, f"❌ Error insertando en '{tbl_summary_loads}': {e}"

    # 7) OK final
    msg = (
        f"✅ update_cloud_summary: summary_tables(table_id={tid_int}) actualizado; "
        f"summary_loads(load_id={load_id}) registrado con "
        f"{load_report['total_inserted']} insertados, "
        f"{load_report['total_updated']} actualizados, "
        f"{load_report['total_ignored']} ignorados."
    )
    return 0, msg
