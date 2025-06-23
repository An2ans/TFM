# ETL.py

import os
import json
import time
from dotenv import load_dotenv
from pathlib import Path
from prefect import flow, get_run_logger
from dataclasses import dataclass

from tasks.Load.connect_prefect_workpool import connect_prefect_workpool
from tasks.Load.finish_ETL import finish_ETL


# Importar subflows
from flows.affiliated_flow import affiliated_flow
from flows.product_flow import product_flow
from flows.sales_flow import sales_flow
from flows.calendar_flow import calendar_flow
from flows.delivery_flow import delivery_flow
from flows.oos_flow import oos_flow

# Cargar variables de entorno
load_dotenv()

# Cargar settings
BASE_DIR      = Path(__file__).parent
SETTINGS_PATH = BASE_DIR / "ETL_settings.json"

with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
    settings = json.load(f)["settings"]

global_settings = settings.get("global", {})
flow_settings   = settings.get("flows", {})
LOCAL_DB_PATH   = global_settings.get("LOCAL_DB_PATH")
MAX_TRIES       = int(global_settings.get("MAX_TRIES", 3))

@dataclass
class FlowJob:
    alias: str
    flow_fn: callable
    config: dict
    status: str = "pending"
    tries: int = 0


@flow(name="etl_orquestador")
def etl_orquestador():
    logger = get_run_logger()
    start_time = time.time()

    flows_to_run = []
    for alias, conf in flow_settings.items():
        flow_name_str = conf.get("FLOW_NAME")
        if not flow_name_str or not isinstance(flow_name_str, str):
            logger.warning(f"Ignorando configuración de flow '{alias}': no se encontró clave 'FLOW_NAME' válida.")
            continue
        # Intentar obtener la función a partir del nombre
        flow_fn = globals().get(flow_name_str)
        if flow_fn is None:
            # Si no está en globals, quizá no fue importado; podrías intentar import dinámico:
            logger.error(f"No se halló la función de flow llamada '{flow_name_str}' (alias '{alias}'). Asegúrate de importarla en ETL.py.")
            continue
        if not callable(flow_fn):
            logger.error(f"El objeto encontrado para '{flow_name_str}' no es callable. Alias: '{alias}'.")
            continue
        # Confirma que la configuración de este flow sea un dict
        if not isinstance(conf, dict):
            logger.warning(f"Ignorando configuración de flow '{alias}': su sección en settings no es un dict.")
            continue
        # OK, agregamos a la lista: (alias, función, settings_para_ese_flow)
        flows_to_run.append(FlowJob(alias, flow_fn, conf))

    # 1) (Opcional) conectar al work pool
    try:
        code_pool, msg_pool = connect_prefect_workpool()
        logger.info(msg_pool)
        # Asumimos convención: code_pool == 0 indica error, !=0 éxito
        if code_pool == 0:
            # Si quieres abortar todo cuando no se conecta, descomenta:
            # raise RuntimeError("Aborting ETL: " + msg_pool)
            logger.error(f"connect_prefect_workpool indicó fallo: {msg_pool}. Se continúa localmente.")
    except Exception as e:
        logger.error(f"Excepción al conectar work pool: {e}. Se continúa localmente.")

        

    logger.info(f"Se van a ejecutar {len(flows_to_run)} flows con un máximo de {MAX_TRIES} intentos cada uno.")

    while any(job.status != "completed" and job.tries < MAX_TRIES for job in flows_to_run):
        for job in flows_to_run:
            if job.status == "completed" or job.tries >= MAX_TRIES:
                continue

            logger.info(f"Ejecutando flow '{job.alias}' (intento {job.tries + 1})")
            job.tries += 1

            try:
                result = job.flow_fn(job.config)
                if isinstance(result, tuple) and result[0] == 0:
                    job.status = "completed"
                    job.message = result[1] if len(result) > 1 else ""
                    logger.info(f"✅ Flow '{job.alias}' completado: {job.message}")
                else:
                    job.status = "failed"
                    logger.error(f"❌ Flow '{job.alias}' fallido (intento {job.tries})")

            except Exception as e:
                job.status = "failed"
                logger.error(f"❌ Excepción al ejecutar flow '{job.alias}' (intento {job.tries}): {e}")

    # Evaluación final
    failed_jobs = [job for job in flows_to_run if job.status != "completed"]
    total_time = time.time() - start_time

    if not failed_jobs:
        logger.info(f"🎉 Todos los flows se ejecutaron correctamente en {total_time:.2f} segundos.")
    else:
        failed_aliases = [job.alias for job in failed_jobs]
        logger.error(f"⚠️ Algunos flows fallaron tras {MAX_TRIES} intentos: {failed_aliases}")
        logger.info(f"⏱️ Tiempo total de ejecución: {total_time:.2f} segundos.")

    # Finalmente, finish_ETL
    try:
        code_fin, msg_fin = finish_ETL()
        logger.info(msg_fin)
    except Exception as e:
        logger.error(f"Error en finish_ETL: {e}")

    logger.info("🎉 etl_orquestador finalizado.")

if __name__ == "__main__":
    etl_orquestador()
