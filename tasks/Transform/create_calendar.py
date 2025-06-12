# tasks/Transform/create_calendar.py

from datetime import datetime, timedelta
from typing import Tuple
import pandas as pd
from pathlib import Path
from prefect import task

# Mapas para nombres en español
_DAY_NAME_ES = {
    0: "Lunes",
    1: "Martes",
    2: "Miércoles",
    3: "Jueves",
    4: "Viernes",
    5: "Sábado",
    6: "Domingo"
}

_MONTH_NAME_ES = {
    1: "Enero",
    2: "Febrero",
    3: "Marzo",
    4: "Abril",
    5: "Mayo",
    6: "Junio",
    7: "Julio",
    8: "Agosto",
    9: "Septiembre",
    10: "Octubre",
    11: "Noviembre",
    12: "Diciembre"
}

@task
def create_calendar(
    fi: str,
    ff: str
) -> Tuple[int, str, pd.DataFrame]:
    """
    Genera un calendario entre fechas fi y ff (ambas inclusive). Las fechas
    de entrada deben estar en formato "DD-MM-YYYY".
    
    Devuelve:
      - code = 1 si el formato de fi/ff es incorrecto
      - code = 9 si ocurre cualquier otro error al crear el DataFrame
      - code = 0 si todo sale bien
    
    El DataFrame resultante ('calendar') tendrá columnas:
      - Day (datetime.date)
      - Week_day (1=Lunes … 7=Domingo)
      - Week_day_name (en español)
      - Month (1–12)
      - Month_name (en español)
      - Year
    """
    try:
        # Intentar parsear fi y ff con formato "DD-MM-YYYY"
        try:
            dt_fi = datetime.strptime(fi, "%d-%m-%Y").date()
            dt_ff = datetime.strptime(ff, "%d-%m-%Y").date()
        except Exception:
            return 1, "❌ Formato de fechas incorrecto. Debe ser 'DD-MM-YYYY'.", pd.DataFrame()

        if dt_ff < dt_fi:
            return 1, "❌ Fecha final (ff) anterior a fecha inicial (fi).", pd.DataFrame()

        # Generar lista de fechas entre fi y ff inclusive
        total_days = (dt_ff - dt_fi).days + 1
        lista_fechas = [dt_fi + timedelta(days=i) for i in range(total_days)]

        # Construir DataFrame
        records = []
        for d in lista_fechas:
            weekday_index = d.weekday()        # 0=Lunes … 6=Domingo
            records.append({
                "Day": d,
                "Week_day": weekday_index + 1,  # Queremos 1–7
                "Week_day_name": _DAY_NAME_ES[weekday_index],
                "Month": d.month,
                "Month_name": _MONTH_NAME_ES[d.month],
                "Year": d.year
            })

        try:
            calendar = pd.DataFrame(records)
            calendar["Day"] = pd.to_datetime(calendar["Day"])
        except Exception as e:
            return 9, f"❌ Error al convertir a DataFrame: {e}", pd.DataFrame()

        # Orden natural ya viene ordenada porque lista_fechas es creciente
        msg = f"✅ Calendario creado correctamente: {len(calendar)} filas."
        return 0, msg, calendar

    except Exception as e:
        return 9, f"❌ Ha ocurrido un error inesperado en create_calendar: {e}", pd.DataFrame()
