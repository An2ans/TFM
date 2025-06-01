# tasks/create_local_table.py

import duckdb
from prefect import task, get_run_logger
import pandas as pd

@task(cache_result_in_memory=False)
def create_local_table(df: pd.DataFrame, table_name: str, con: duckdb.DuckDBPyConnection) -> None:
    """
    Crea una tabla en el DuckDB conectado (con) a partir del DataFrame `df`,
    usando el nombre `table_name`. Si la tabla ya existe, informa y no hace nada.
    En caso de error en la estructura de datos o en la conexión, informa claramente.
    Si se crea con éxito, muestra las primeras 10 filas.

    Parámetros:
    - df: DataFrame de pandas a insertar.
    - table_name: Nombre de la tabla a crear en DuckDB.
    - con: Conexión DuckDB obtenida previamente (desde connect_local_duckdb).

    Comportamiento:
    - DuckDB infiere tipos SQL de los dtypes de pandas.
    - Se asigna la primera columna de `df` como PRIMARY KEY en el DDL.
    - Si la tabla ya existe, se registra info y retorna sin error.
    - Si falla la creación o inserción, lanza RuntimeError con mensaje adecuado.
    """
    logger = get_run_logger()

    # 1) Comprobar si ya existe la tabla
    try:
        exists = con.execute(
            f"""
            SELECT COUNT(*)
              FROM information_schema.tables
             WHERE table_schema='main'
               AND table_name='{table_name.lower()}';
            """
        ).fetchone()[0] > 0
    except Exception as e:
        msg = f"❌ Error al consultar si la tabla '{table_name}' existe: {e}"
        logger.error(msg)
        raise RuntimeError(msg)

    if exists:
        logger.info(f"ℹ️ La tabla '{table_name}' ya existe en DuckDB. No se crea de nuevo.")
        return

    # 2) Construir la sentencia CREATE TABLE con PK en la primera columna
    try:
        # Mapear pandas dtypes a tipos DuckDB
        dtype_map = {}
        for col, pd_dtype in df.dtypes.items():
            if pd.api.types.is_integer_dtype(pd_dtype):
                dtype_map[col] = "BIGINT"
            elif pd.api.types.is_float_dtype(pd_dtype):
                dtype_map[col] = "DOUBLE"
            elif pd.api.types.is_bool_dtype(pd_dtype):
                dtype_map[col] = "BOOLEAN"
            elif pd.api.types.is_datetime64_any_dtype(pd_dtype):
                dtype_map[col] = "TIMESTAMP"
            else:
                dtype_map[col] = "VARCHAR"

        first_col = df.columns[0]
        columns_ddl = []
        for col in df.columns:
            col_type = dtype_map[col]
            if col == first_col:
                columns_ddl.append(f'"{col}" {col_type} PRIMARY KEY')
            else:
                columns_ddl.append(f'"{col}" {col_type}')

        ddl = f'CREATE TABLE "{table_name}" (\n  ' + ",\n  ".join(columns_ddl) + "\n);"
        con.execute(ddl)
        logger.info(f"✅ Tabla '{table_name}' creada con primera columna '{first_col}' como PRIMARY KEY.")
    except Exception as e:
        msg = f"❌ Error al crear la tabla '{table_name}': {e}"
        logger.error(msg)
        raise RuntimeError(msg)

    # 3) Insertar datos y mostrar head(10)
    try:
        con.register("temp_df", df)
        con.execute(f'INSERT INTO "{table_name}" SELECT * FROM temp_df;')
        head_df = con.execute(f'SELECT * FROM "{table_name}" LIMIT 10;').df()
        logger.info(f"🎉 Tabla '{table_name}' poblada con éxito. Primeras 10 filas:\n{head_df}")
    except Exception as e:
        msg = f"❌ Error al insertar datos en la tabla '{table_name}': {e}"
        logger.error(msg)
        raise RuntimeError(msg)
