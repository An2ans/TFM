# Aplicación ETL con Prefect – TFM Inteligencia de Negocio  

## 📌 Introducción y descripción del proyecto  
Este proyecto forma parte de un **Trabajo Fin de Máster en Inteligencia de Negocio**.  
La finalidad es construir un **proceso ETL (Extract, Transform, Load) orquestado con [Prefect](https://docs.prefect.io/)** que permita:  

- Extraer datos desde diferentes fuentes.  
- Transformarlos y validarlos con tareas modulares.  
- Cargarlos en un destino final, en este caso, un Data Warehouse en la nube por [MotherDuck](app.motherduck.com).  

El desarrollo se ha realizado sobre **datos reales proporcionados en el contexto del TFM**, pero la arquitectura es **totalmente adaptable** a otros conjuntos de datos y fuentes.  

Toda la configuración del pipeline ETL se puede personalizar fácilmente a través del archivo **`ETL_settings.json`**, lo que permite modificar fuentes, parámetros y reglas sin tener que alterar el código principal.  

---

## 📂 Estructura del proyecto  


```bash
.
├── prefect-env/              # Entorno virtual utilizado para desarrollo local
├── flows/                    # Definición de subflows Prefect (uno por tabla/proceso)
│   ├── affiliated_flow.py
│   ├── product_flow.py
│   ├── sales_flow.py
│   ├── calendar_flow.py
│   ├── delivery_flow.py
│   └── oos_flow.py
├── tasks/                    # Tareas modulares de la ETL
│   ├── Extract/              # Extracción (APIs, CSV, DB, etc.)
│   ├── Transform/            # Transformaciones de datos
│   ├── Load/                 # Carga en destino (MotherDuck, Cloud, etc.)
│   ├── Quality/              # Control de calidad (validaciones con GE, checks, etc.)
│   └── Utils/                # Funciones auxiliares (manejo de errores, utilidades)
├── ETL.py                    # Orquestador principal del flujo completo
├── ETL_settings.json         # Configuración de la ETL (fuentes, tablas, parámetros)
└── README.md                 # Este archivo
```
## 🔄 Replicación del proyecto

### 1. Clonar el repositorio

```bash
git clone https://github.com/An2ans/TFM.git  
cd TFM
```

### 2. Instalar dependencias y entorno virtual

Se utiliza un entorno basado en **prefect-env**. Para crear y activar el entorno:
```bash
python -m venv prefect-env  
source prefect-env/bin/activate   # En Linux/Mac  
prefect-env\Scripts\activate      # En Windows  
pip install prefect pandas great_expectations duckdb python-dotenv
```
### 3. Configurar variables de entorno

Crear un archivo **.env** en la raíz del proyecto con las credenciales necesarias (bases de datos, APIs, etc.).

### 4. Abrir el proyecto en un editor de código

Se recomienda **Visual Studio Code**, aunque puede usarse cualquier IDE.

### 5. Ejecutar el flujo principal

```bash
prefect run python ETL.py  
```

---

## 🚧 Bugs conocidos


## 🚀 Futuras mejoras

## 📜 Licencia

