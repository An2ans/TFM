import subprocess
from prefect import task, get_run_logger

@task
def create_work_pool(pool_name: str, pool_type: str = "process") -> str:
    """
    Crea un Work Pool en Prefect 3 si no existe, usando el CLI de Prefect.
    Devuelve el nombre del pool (o lanza excepción si falla).
    """
    logger = get_run_logger()

    # 1) Listar work pools existentes
    try:
        result = subprocess.run(
            ["prefect", "work-pool", "ls"],
            capture_output=True,
            text=True,
            check=True
        )
        output = result.stdout.lower()
    except subprocess.CalledProcessError as e:
        logger.error(f"Error al listar Work Pools: {e.stderr}")
        raise

    # 2) Si ya existe, lo devolvemos
    if pool_name.lower() in output:
        logger.info(f"🔍 Work Pool '{pool_name}' ya existe.")
        return pool_name

    # Intentamos crear el pool; si falla, lanzamos excepción para detener el flow
    result = subprocess.run(
        ["prefect", "work-pool", "create", pool_name, "--type", pool_type],
        capture_output=True,
        text=True,
        check=False
    )
    if result.returncode == 0:
        logger.info(f"✅ Creado Work Pool '{pool_name}' (tipo={pool_type}).")
        return pool_name
    else:
        # Si falla (p.ej. 403 Forbidden u otro), lanzamos excepto para detener el flow
        msg = (
            f"❌ No se pudo crear Work Pool '{pool_name}' "
            f"(returncode={result.returncode}).\nSalida CLI:\n{result.stderr.strip()}"
        )
        logger.error(msg)
        raise RuntimeError(msg)