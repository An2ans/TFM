# tasks/Load/load_table_to_cloud.py

import os
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime
from prefect import task, get_run_logger
from typing import Tuple

@task(cache_key_fn=lambda *args, **kwargs: None)
def load_table_to_cloud(
    df: pd.DataFrame,
    table_name: str,
    con
) -> Tuple[int, str]:
    """
    Carga o actualiza datos de un DataFrame `df` en la tabla `table_name` de DuckDB Cloud (MotherDuck),
    sin borrar datos existentes; en caso de clave primaria repetida, sustituye los registros existentes.
    Además, informa las claves primarias sustituidas y el número total de registros insertados.
    Usa un archivo Parquet temporal en ./TEMP/TABLE_NAME.parquet.

    Retorna:
      - (0, mensaje) si todo OK.
      - (1, mensaje) si parámetros inválidos.
      - (2, mensaje) si error conectando a DuckDB Cloud.
      - (3, mensaje) si error escribiendo Parquet o temporal.
      - (4, mensaje) si error creando tabla nueva.
      - (5, mensaje) si error durante el proceso de upsert.
      - (9, mensaje) otro error inesperado.
    """
    logger = get_run_logger()

    # 1) Validar parámetros
    if not isinstance(df, pd.DataFrame) or df.empty:
        return 1, f"load_table_to_cloud_upsert ❌ Error: df inválido o vacío para tabla '{table_name}'."
    if not isinstance(table_name, str) or not table_name.strip():
        return 1, f"load_table_to_cloud_upsert ❌ Error: table_name inválido: '{table_name}'."

    # Detectar clave primaria como primera columna
    pk_col = df.columns[0]
    # Verificar que no hay duplicados en df sobre la PK
    dupes = df[pk_col].duplicated()
    if dupes.any():
        duplicated_keys = df.loc[dupes, pk_col].unique().tolist()
        return 1, (
            f"load_table_to_cloud_upsert ❌ Error: el DataFrame tiene valores duplicados en la clave primaria '{pk_col}': "
            f"{duplicated_keys}"
        )


    # 3) Preparar carpeta TEMP y ruta Parquet
    try:
        temp_dir = Path.cwd() / "TEMP"
        temp_dir.mkdir(exist_ok=True)
        parquet_path = temp_dir / f"{table_name}.parquet"
        # Si existe prev Parquet con mismo nombre, lo sobreescribimos
        if parquet_path.exists():
            try:
                parquet_path.unlink()
            except Exception:
                pass
        # Volcar df a Parquet
        df.to_parquet(parquet_path, index=False)
        logger.info(f"load_table_to_cloud_upsert ✅ Parquet temporal escrito: '{parquet_path}', filas: {len(df)}.")
    except Exception as e:
        return 3, f"load_table_to_cloud_upsert ❌ Error escribiendo Parquet temporal '{parquet_path}': {e}"

    try:
        # 4) Verificar existencia de tabla en cloud
        exists_df = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = ?", (table_name,)
        ).fetchdf()
        table_exists = not exists_df.empty
    except Exception as e:
        # Limpieza parquet
        try: parquet_path.unlink()
        except: pass
        return 2, f"load_table_to_cloud_upsert ❌ Error consultando metadata en cloud para tabla '{table_name}': {e}"

    # 5) Si no existe, crear tabla nueva e insertar todos los registros
    if not table_exists:
        try:
            # Inferir esquema desde df.dtypes
            type_map = {
                'datetime64[ns]': 'TIMESTAMP',
                'int64': 'BIGINT',
                'Int64': 'BIGINT',
                'float64': 'DOUBLE',
                'bool': 'BOOLEAN',
                'boolean': 'BOOLEAN'
            }
            def duck_type(dtype_str: str) -> str:
                return type_map.get(dtype_str, 'VARCHAR')
            cols_ddl = []
            for col, dt in df.dtypes.astype(str).items():
                sql_type = duck_type(dt)
                if col == pk_col:
                    cols_ddl.append(f"{col} {sql_type} PRIMARY KEY")
                else:
                    cols_ddl.append(f"{col} {sql_type}")
            ddl = f"CREATE TABLE {table_name} (\n  " + ",\n  ".join(cols_ddl) + "\n)"
            con.execute(ddl)
            logger.info(f"load_table_to_cloud_upsert ✅ Tabla '{table_name}' creada en cloud.")
        except Exception as e:
            # Limpieza parquet
            try: parquet_path.unlink()
            except: pass
            return 4, f"load_table_to_cloud_upsert ❌ Error creando tabla '{table_name}' en cloud: {e}"
        try:
            # Insertar todos los registros desde Parquet
            con.execute(f"COPY {table_name} FROM '{parquet_path}' (FORMAT 'parquet')")
            total_inserted = len(df)
            msg = f"load_table_to_cloud_upsert ✅ Tabla '{table_name}' creada y poblada con {total_inserted} registros."
            # Limpieza parquet
            try: parquet_path.unlink()
            except: pass
            return 0, msg
        except Exception as e:
            # Limpieza parquet
            try: parquet_path.unlink()
            except: pass
            return 5, f"load_table_to_cloud_upsert ❌ Error insertando datos en nueva tabla '{table_name}': {e}"

    # 6) Si la tabla existe, proceder a upsert:
    substituted_keys = []
    try:
        # 6.a) Crear tabla temporal en cloud a partir del Parquet
        tmp_table = f"tmp_{table_name}_{int(datetime.utcnow().timestamp())}"
        # Usamos un nombre único en caso de concurrencia; es temporal en la sesión
        con.execute(f"""
            CREATE TEMPORARY TABLE {tmp_table} AS 
            SELECT * FROM read_parquet('{parquet_path}')
        """)
        # 6.b) Obtener claves primarias en tmp
        tmp_keys_df = con.execute(f"SELECT {pk_col} FROM {tmp_table}").fetchdf()
        tmp_keys = tmp_keys_df[pk_col].tolist()
        # 6.c) Encontrar qué claves ya existen en la tabla destino
        # Usamos join para mayor eficiencia
        existing_keys_df = con.execute(f"""
            SELECT t.{pk_col} FROM {table_name} t
            INNER JOIN {tmp_table} tmp ON t.{pk_col} = tmp.{pk_col}
        """).fetchdf()
        substituted_keys = existing_keys_df[pk_col].tolist()
        # 6.d) Borrar registros existentes con esas claves
        if substituted_keys:
            # DELETE usando subconsulta
            con.execute(f"""
                DELETE FROM {table_name} 
                WHERE {pk_col} IN (SELECT {pk_col} FROM {tmp_table})
            """)
            logger.info(f"load_table_to_cloud_upsert ℹ️ Se borraron {len(substituted_keys)} registros existentes en '{table_name}'.")
        # 6.e) Insertar todos los registros del tmp_table
        con.execute(f"""
            INSERT INTO {table_name} SELECT * FROM {tmp_table}
        """)
        total_inserted = len(df)
        # 6.f) Borrar la tabla temporal
        con.execute(f"DROP TABLE IF EXISTS {tmp_table}")
    except Exception as e:
        # Limpieza parquet
        try: parquet_path.unlink()
        except: pass
        return 5, f"load_table_to_cloud_upsert ❌ Error durante upsert en tabla '{table_name}': {e}"

    # 7) Limpieza Parquet temporal
    try:
        parquet_path.unlink()
    except Exception:
        pass

    # 8) Construir mensaje final
    if substituted_keys:
        msg = (
            f"load_table_to_cloud_upsert ✅ Tabla '{table_name}' actualizada: "
            f"{len(substituted_keys)} registros sustituidos (PKs: {substituted_keys}); "
            f"total insertados: {total_inserted}."
        )
    else:
        msg = (
            f"load_table_to_cloud_upsert ✅ Tabla '{table_name}' existente: "
            f"no se encontraron registros previos con mismas PK; total insertados: {total_inserted}."
        )

    return 0, msg
