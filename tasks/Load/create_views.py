# tasks/Load/create_views.py

import re
from typing import Dict, Tuple, Any
from prefect import task, get_run_logger

@task
def create_views(
    views: Dict[str, str],
    con: Any
) -> Tuple[int, str]:
    """
    create_views: crea vistas en DuckDB/MotherDuck según el diccionario `views`.
    Parámetros:
      - views: dict donde clave = nombre de la vista (str), valor = query SQL (str).
      - con: objeto de conexión DuckDB (DuckDBPyConnection) ya conectado a la base de datos cloud.
    Códigos de retorno:
      1 → Parámetro `views` inválido (no es dict o está vacío, o keys/values no son str válidos).
      2 → Conexión inválida (no responde a un ping simple).
      3 → Error de sintaxis en alguna query; el mensaje incluirá la query que falló.
      4 → Error al crear la vista en MotherDuck (no sintaxis, p.ej. permisos, objeto dependiente, etc.).
      5 → Nombre de vista inválido según las reglas de identificadores de DuckDB.
      9 → Otro error inesperado.
      0 → Todo OK; vistas creadas o reemplazadas exitosamente.
    Validaciones sobre view_name:
      - Debe coincidir con las reglas de identificador de DuckDB:
        * Solo letras ASCII, dígitos y guión bajo.
        * Comenzar con letra ASCII o guión bajo.
        * No debe coincidir con palabra reservada 
      - Adicionalmente, sugerimos prefijo “v_” para nombres de vistas, pero esto no aborta la tarea:
    """
    task_name = "create_views"

    # 1) Validar parámetro `views`
    if not isinstance(views, dict) or not views:
        msg = f"{task_name} ❌ Error: parámetro `views` inválido o vacío. Debe ser dict[str, str] con al menos una entrada."
        return 1, msg
    # Validar cada key/value
    for v_name, v_query in views.items():
        if not isinstance(v_name, str) or not v_name.strip():
            msg = f"{task_name} ❌ Error: nombre de vista inválido: {v_name!r}. Debe ser str no vacío."
            return 1, msg
        if not isinstance(v_query, str) or not v_query.strip():
            msg = f"{task_name} ❌ Error: query inválida para vista '{v_name}'. Debe ser str no vacío."
            return 1, msg

    # 2) Validar conexión: “ping” simple
    try:
        con.execute("SELECT 1").fetchall()
    except Exception as e:
        msg = f"{task_name} ❌ Error: conexión inválida o ping fallido: {e}"
        return 2, msg

    # Precompile regex para identificador DuckDB
    # Reglas básicas:
    #  - Comenzar con letra ASCII (a-z, A-Z) o guión bajo _
    #  - Seguir con letras ASCII, dígitos o guión bajo
    #  - No espacios ni caracteres especiales
    # Nota: no se comprueban exhaustivamente palabras reservadas.
    identifier_pattern = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

    # 3) Para cada vista, validar nombre y luego crear/reemplazar
    created_views = []
    logger = get_run_logger()
    for view_name, query in views.items():
        # 3.1) Validar sintaxis básica de nombre según DuckDB
        if not identifier_pattern.match(view_name):
            msg = f"{task_name} ❌ Error: nombre de vista inválido según reglas de DuckDB: '{view_name}'. Debe empezar con letra o '_', y contener solo letras, dígitos o '_'."
            return 5, msg
        # 3.2) Validación de prefijo “v_” (no fatal, solo warning)
        if not view_name.startswith("v_"):
            logger.warning(f"{task_name} ⚠️ Estándar de nombres: se recomienda que la vista empiece con 'v_'. Nombre actual: '{view_name}'.")

        # 3.3) Intentar CREATE OR REPLACE VIEW
        try:
            sql = f"CREATE OR REPLACE VIEW {view_name} AS {query}"
            con.execute(sql)
            created_views.append(view_name)
        except Exception as e:
            err_msg = str(e)
            # Sintaxis detectada en el mensaje de error?
            if "syntax error" in err_msg.lower():
                msg = f"{task_name} ❌ Error de sintaxis al crear vista '{view_name}'. Query: {query!r}. Error: {err_msg}"
                return 3, msg
            else:
                msg = f"{task_name} ❌ Error al crear/reemplazar vista '{view_name}'. Query: {query!r}. Error: {err_msg}"
                return 4, msg

    # 0) Todo OK
    msg = f"{task_name} ✅ Vistas creadas/reemplazadas exitosamente: {created_views}"
    return 0, msg
