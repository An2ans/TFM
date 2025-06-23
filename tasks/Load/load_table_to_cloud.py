# tasks/Load/load_table_to_cloud.py

import os
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime
from prefect import task, get_run_logger
from typing import Tuple, Dict

@task(cache_key_fn=lambda *args, **kwargs: None)
def load_table_to_cloud(
    df: pd.DataFrame,
    table_name: str,
    con
) -> Tuple[int, str, Dict[str, int]]:
    logger = get_run_logger()

    if not isinstance(df, pd.DataFrame) or df.empty:
        return 1, f"❌ Error: df inválido o vacío para tabla '{table_name}'.", {}

    if not isinstance(table_name, str) or not table_name.strip():
        return 1, f"❌ Error: table_name inválido: '{table_name}'.", {}

    pk_col = df.columns[0]
    dupes = df[pk_col].duplicated()
    if dupes.any():
        duplicated_keys = df.loc[dupes, pk_col].unique().tolist()
        return 1, (
            f"❌ Error: valores duplicados en la clave primaria '{pk_col}': {duplicated_keys}"
        ), {}

    load_report = {
        "total_inserted": 0,
        "total_updated": 0,
        "total_ignored": 0
    }

    try:
        temp_dir = Path.cwd() / "TEMP"
        temp_dir.mkdir(exist_ok=True)
        parquet_path = temp_dir / f"{table_name}.parquet"
        if parquet_path.exists():
            parquet_path.unlink()
        df.to_parquet(parquet_path, index=False)
        logger.info(f"✅ Parquet temporal escrito: '{parquet_path}', filas: {len(df)}.")
    except Exception as e:
        return 3, f"❌ Error escribiendo Parquet: {e}", {}

    try:
        exists_df = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = ?",
            (table_name,)
        ).fetchdf()
        table_exists = not exists_df.empty
    except Exception as e:
        try: parquet_path.unlink()
        except: pass
        return 2, f"❌ Error consultando metadata: {e}", {}

    if not table_exists:
        try:
            type_map = {
                'datetime64[ns]': 'TIMESTAMP',
                'int64': 'BIGINT',
                'Int64': 'BIGINT',
                'float64': 'DOUBLE',
                'bool': 'BOOLEAN',
                'boolean': 'BOOLEAN'
            }
            def duck_type(dtype_str): return type_map.get(dtype_str, 'VARCHAR')
            cols_ddl = [
                f"{col} {duck_type(str(dtype))}" + (" PRIMARY KEY" if col == pk_col else "")
                for col, dtype in df.dtypes.items()
            ]
            ddl = f"CREATE TABLE {table_name} (\n  " + ",\n  ".join(cols_ddl) + "\n)"
            con.execute(ddl)
            logger.info(f"✅ Tabla '{table_name}' creada en cloud.")
        except Exception as e:
            try: parquet_path.unlink()
            except: pass
            return 4, f"❌ Error creando tabla: {e}", {}

        try:
            con.execute(f"COPY {table_name} FROM '{parquet_path}' (FORMAT 'parquet')")
            load_report["total_inserted"] = len(df)
            try: parquet_path.unlink()
            except: pass
            return 0, (
                f"✅ Tabla '{table_name}' creada y cargada con {load_report['total_inserted']} registros."
            ), load_report
        except Exception as e:
            try: parquet_path.unlink()
            except: pass
            return 5, f"❌ Error insertando datos en nueva tabla: {e}", {}

    # ----- Si la tabla ya existe -----
    total_keys = set(df[pk_col].tolist())
    substituted_keys = []
    ignored_keys = []

    try:
        tmp_table = f"tmp_{table_name}_{int(datetime.utcnow().timestamp())}"
        con.execute(f"""
            CREATE TEMPORARY TABLE {tmp_table} AS 
            SELECT * FROM read_parquet('{parquet_path}')
        """)

        cols = df.columns.tolist()
        non_pk_cols = [col for col in cols if col != pk_col]

        comparison_clauses = " OR ".join([
            f"(t.\"{col}\" IS DISTINCT FROM tmp.\"{col}\")" for col in non_pk_cols
        ])

        changed_keys_df = con.execute(f"""
            SELECT t.{pk_col} FROM {table_name} t
            INNER JOIN {tmp_table} tmp ON t.{pk_col} = tmp.{pk_col}
            WHERE {comparison_clauses}
        """).fetchdf()

        substituted_keys = changed_keys_df[pk_col].tolist()
        all_keys_df = con.execute(f"SELECT {pk_col} FROM {tmp_table}").fetchdf()
        all_tmp_keys = set(all_keys_df[pk_col].tolist())
        ignored_keys = list(all_tmp_keys - set(substituted_keys))
        inserted_keys = list(total_keys - all_tmp_keys)

        if substituted_keys:
            con.execute(f"""
                DELETE FROM {table_name} 
                WHERE {pk_col} IN (
                    SELECT {pk_col} FROM {tmp_table}
                    WHERE {pk_col} IN ({','.join(map(str, substituted_keys))})
                )
            """)
            con.execute(f"""
                INSERT INTO {table_name}
                SELECT * FROM {tmp_table}
                WHERE {pk_col} IN ({','.join(map(str, substituted_keys))})
            """)
            load_report["total_updated"] = len(substituted_keys)

        if inserted_keys:
            con.execute(f"""
                INSERT INTO {table_name}
                SELECT * FROM {tmp_table}
                WHERE {pk_col} IN ({','.join(map(str, inserted_keys))})
            """)
            load_report["total_inserted"] = len(inserted_keys)

        load_report["total_ignored"] = len(ignored_keys)

        con.execute(f"DROP TABLE IF EXISTS {tmp_table}")

    except Exception as e:
        try: parquet_path.unlink()
        except: pass
        return 5, f"❌ Error durante upsert en tabla '{table_name}': {e}", {}

    try:
        parquet_path.unlink()
    except:
        pass

    # --- Mensaje Final ---
    msg = (
        f"✅ Tabla '{table_name}' actualizada: "
        f"{load_report['total_inserted']} insertados, "
        f"{load_report['total_updated']} actualizados, "
        f"{load_report['total_ignored']} sin cambios."
    )

    return 0, msg, load_report
