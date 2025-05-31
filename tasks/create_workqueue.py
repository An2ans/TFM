# tasks/create_workqueue.py

import subprocess
from prefect import task, get_run_logger

@task
def create_work_queue(queue_name: str) -> str:
    """
    Crea (o verifica) una Work Queue en Prefect 3 usando el CLI.
    Devuelve el nombre de la cola. Si ya existe, no hace nada.
    """
    logger = get_run_logger()

    # 1) Listar work queues existentes (sin usar --no-format)
    try:
        result = subprocess.run(
            ["prefect", "work-queue", "ls"],
            capture_output=True,
            text=True,
            check=True
        )
        output = result.stdout.lower()
    except subprocess.CalledProcessError as e:
        logger.error(f"Error al listar Work Queues: {e.stderr.strip()}")
        raise

    # 2) Si ya existe la cola, devolvemos su nombre
    #    Buscamos en toda la salida de 'prefect work-queue ls'
    if queue_name.lower() in output:
        logger.info(f"Work Queue '{queue_name}' ya existe.")
        return queue_name

    # 3) Si no existe, la creamos
    try:
        subprocess.run(
            ["prefect", "work-queue", "create", queue_name],
            capture_output=True,
            text=True,
            check=True
        )
        logger.info(f"✅ Creada Work Queue '{queue_name}'.")
        return queue_name
    except subprocess.CalledProcessError as e:
        msg = (
            f"❌ No se pudo crear Work Queue '{queue_name}'.\n"
            f"Salida CLI:\n{e.stderr.strip()}"
        )
        logger.error(msg)
        raise RuntimeError(msg)
