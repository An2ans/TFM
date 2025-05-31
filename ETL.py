# ETL.py

import os
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

# Importamos las tareas custom
from tasks.import_csv import import_csv
from tasks.check_nulls import check_nulls
from tasks.check_unique import check_unique
from tasks.force_unique import force_unique
from tasks.create_workqueue import create_work_queue

# Ruta fija del CSV de Affiliated
AFFILIATED_PATH = r"C:\Users\anton\OneDrive - UNIR\Equipo\TFM2\DATA\Affiliated_Outlets.csv"

# Cargamos las variables de entorno desde .env
load_dotenv()
# .env debe contener:
#   PREFECT_API_KEY=tu_prefect_api_key
#   PREFECT_WORKSPACE_ID=4da65d4a-7646-4f01-8698-06f44d20fbfb

@task
def t_read_affiliated(path: str, sep: str = ";"):
    """
    Lee el CSV de Affiliated y devuelve un DataFrame.
    """
    logger = get_run_logger()
    logger.info(f"▶️ Iniciando lectura de {path}")
    df = import_csv(path, sep)
    logger.info(f"✅ Lectura completada: {df.shape[0]} filas, {df.shape[1]} columnas")
    return df

@task
def t_check_nulls(df):
    """
    Comprueba nulos en todas las columnas del DataFrame.
    """
    logger = get_run_logger()
    logger.info("▶️ Iniciando check de nulos")
    nulls = check_nulls(df)
    if nulls:
        logger.warning(f"⚠️ Se encontraron nulos: {nulls}")
    else:
        logger.info("✅ No hay nulos")
    return nulls

@task
def t_check_unique(df, col: str):
    """
    Comprueba que la columna 'col' sea única en el DataFrame.
    """
    logger = get_run_logger()
    logger.info(f"▶️ Iniciando check de unicidad en columna '{col}'")
    is_unique = check_unique(df, col)
    if is_unique:
        logger.info("✅ Todos los valores son únicos")
    else:
        logger.warning("⚠️ Hay duplicados")
    return is_unique

@task
def t_handle_duplicates(df, col: str, is_unique: bool):
    """
    Si 'is_unique' es False, fuerza unicidad en la columna 'col'.
    Finalmente imprime 'TODO OK' y devuelve el DataFrame (modificado o no).
    """
    logger = get_run_logger()
    if is_unique:
        logger.info("ℹ️ No hace falta corregir unicidad")
    else:
        logger.info(f"▶️ Forzando unicidad en '{col}'")
        df = force_unique(df, col)
        logger.info("✅ Unicidad corregida")
    logger.info("🎉 TODO OK")
    return df

@flow(name="flow_affiliated")
def flow_affiliated():
    """
    Flow principal para procesar el CSV 'Affiliated_Outlets.csv':
    1) Crear (o reutilizar) el Work Pool
    2) Leer CSV
    3) Check de nulos
    4) Check de unicidad en 'Affiliated_Code'
    5) Manejar duplicados si es necesario
    6) Devolver el DataFrame final
    """
    # 1) Intentamos crear/reutilizar la Work Queue "default"
    queue_name = create_work_queue("default")
    # Si falla, create_work_queue lanzará RuntimeError y el flow se detendrá.

    # 2) Leer el CSV de Affiliated
    df = t_read_affiliated(AFFILIATED_PATH)

    # 3) Comprobar nulos
    _nulls = t_check_nulls(df)

    # 4) Comprobar unicidad en la columna "Affiliated_Code"
    is_u = t_check_unique(df, "Affiliated_Code")

    # 5) Manejar duplicados si existen
    df_final = t_handle_duplicates(df, "Affiliated_Code", is_u)

    # 6) Devolver el DataFrame final
    return df_final

if __name__ == "__main__":
    # 1) Ejecutar el flow localmente. Si create_work_pool falla, aquí se detendrá.
    flow_affiliated()

    # 2) Registrar (deploy) el flow en Prefect Cloud, apuntando al Work Pool "default"
    #flow_affiliated.deploy(
    #    name="affiliated-deployment",
    #    work_queue_name="default"
    #)