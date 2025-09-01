ETL con Prefect – TFM Inteligencia de Negocio
📌 Introducción y descripción del proyecto

Este proyecto forma parte de un Trabajo Fin de Máster en Inteligencia de Negocio.
La finalidad es construir un proceso ETL (Extract, Transform, Load) orquestado con Prefect que permita:

Extraer datos desde diferentes fuentes.

Transformarlos y validarlos con tareas modulares.

Cargarlos en un destino final, en este caso, un Data Warehouse en la nube por MotherDuck.

El desarrollo se ha realizado sobre datos reales proporcionados en el contexto del TFM, pero la arquitectura es totalmente adaptable a otros conjuntos de datos y fuentes.

Toda la configuración del pipeline ETL se puede personalizar fácilmente a través del archivo ETL_settings.json, lo que permite modificar fuentes, parámetros y reglas sin tener que alterar el código principal.

📂 Estructura del proyecto
.
├── prefect-env/              # Entorno virtual utilizado para desarrollo
├── flows/                    # Definición de flujos Prefect
├── tasks/                    # Tareas reutilizables (transformación, validación, carga, etc.)
├── ETL.py                    # Script principal que ejecuta la orquestación de Prefect
├── ETL_settings.json         # Configuración de la ETL (fuentes, parámetros, etc.)
├── .env                      # Variables de entorno (credenciales, configuración sensible)
└── README.md                 # Este archivo

⚙️ Replicación del proyecto

Sigue estos pasos para replicar el entorno de trabajo en tu máquina local:

Clonar el repositorio

git clone https://github.com/An2ans/TFM.git
cd TFM


Instalar dependencias y entorno Prefect
Se ha utilizado un entorno llamado prefect-env.

python -m venv prefect-env
source prefect-env/bin/activate   # En Linux/Mac
prefect-env\Scripts\activate      # En Windows


Instalar librerías necesarias

pip install prefect pandas great_expectations duckdb python-dotenv


Configurar el archivo .env
Crea un archivo .env en la raíz del proyecto con las variables necesarias (ejemplo: claves API, rutas de acceso, credenciales).

DB_URL=...
API_KEY=...


Abrir el proyecto en un editor de código (recomendado)

Ejecutar el flujo ETL

prefect run python ETL.py
