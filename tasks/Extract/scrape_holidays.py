import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json

# 1. Definimos las comunidades y sus slugs
REGIONS = {
    "Andalucía": "andalucia",
    "Aragón": "aragon",
    "Asturias": "asturias",
    "Cantabria": "cantabria",
    "Ceuta": "ceuta",
    "Castilla y León": "castile-and-leon",
    "Castilla-La Mancha": "castilla-la-mancha",
    "Canarias": "canary-islands",
    "Cataluña": "catalonia",
    "Extremadura": "extremadura",
    "Galicia": "galicia",
    "Baleares": "balearic-islands",
    "Murcia": "murcia",
    "Madrid": "madrid",
    "Melilla": "melilla",
    "Navarra": "navarra",
    "País Vasco": "basque-country",
    "La Rioja": "la-rioja",
    "Valencia": "valenciana",
}

YEAR = 2015
BASE_URL = "https://www.officeholidays.com/countries/spain/{slug}/{year}"

def parse_table(region_name, slug):
    """Descarga y parsea la tabla de festivos para una región."""
    url = BASE_URL.format(slug=slug, year=YEAR)
    resp = requests.get(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", {"class": "country-table"})
    holidays = []
    for row in table.tbody.find_all("tr"):
        cols = row.find_all("td")
        if len(cols) < 4:
            continue
        date_str = cols[1].get_text(strip=True)       # e.g. "Mar 19"
        name = cols[2].get_text(strip=True)
        htype = cols[3].get_text(strip=True)
        # Solo Public Holiday y Regional Holiday
        if htype not in ("Public Holiday", "Regional Holiday"):
            continue
        comment = cols[4].get_text(strip=True) if len(cols) > 4 else ""
        # Convertimos fecha a DD-MM-YYYY
        dt = datetime.strptime(f"{date_str} {YEAR}", "%b %d %Y")
        day = dt.strftime("%d-%m-%Y")
        holidays.append({
            "Day": day,
            "Name": name,
            "Type": "Nacional" if htype == "Public Holiday" else "Regional",
            "Region": region_name,
            "Comments": comment
        })
    return holidays

def merge_holidays(all_hols):
    """
    Une y normaliza por fecha:
      - Para festivos nacionales: toma el Name de la primera región siempre y
        acumula en Comments cualquier comentario adicional.
      - Para festivos regionales en varias regiones: toma el Name de la primera región,
        y si las demás regiones tienen un Name distinto, los añade a Comments.
    """
    merged = {}
    # Primero agrupamos por fecha
    for entry in all_hols:
        day = entry["Day"]
        region = entry["Region"]
        name   = entry["Name"]
        htype  = entry["Type"]      # "Nacional" o "Regional"
        comment = entry.get("Comment", "")

        if day not in merged:
            merged[day] = {
                "Type": htype,
                "Regions": [region],
                "Names": [name] if name else [],
                "Comments": [comment] if comment else []
            }
        else:
            m = merged[day]
            # consolidamos tipo: si alguno es nacional, todo es nacional
            if htype == "Nacional":
                m["Type"] = "Nacional"
            else:
                # añadimos región única
                if region not in m["Regions"]:
                    m["Regions"].append(region)
            # nombres: guardamos sólo si no está ya
            if name and name not in m["Names"]:
                m["Names"].append(name)
            # comentarios: idem
            if comment and comment not in m["Comments"]:
                m["Comments"].append(comment)

    # Ahora construimos la lista final
    final = []
    for day in sorted(merged.keys(), key=lambda d: datetime.strptime(d, "%d-%m-%Y")):
        e = merged[day]
        first_name = e["Names"][0] if e["Names"] else ""
        # si es nacional, nos fiamos del first_name
        if e["Type"] == "Nacional":
            final.append({
                "Day": day,
                "Name": first_name,
                "Type": "Nacional",
                "Region": "Todas",
                "Comments": "; ".join(e["Comments"])
            })
        else:
            # regional
            # first_name ya viene de la primera región
            # comentarios sólo si el resto de Names difiere
            comments = []
            for nm in e["Names"][1:]:
                if nm != first_name:
                    comments.append(nm)
            # añadir además los comments originales
            comments.extend(e["Comments"])
            final.append({
                "Day": day,
                "Name": first_name,
                "Type": "Regional",
                "Region": sorted(e["Regions"]),
                "Comments": "; ".join(comments)
            })

    return final

def main():
    # 2. Recolectamos todos los festivos
    all_holidays = []
    for region_name, slug in REGIONS.items():
        all_holidays.extend(parse_table(region_name, slug))

    # 3. Normalizamos y fusionamos
    holidays_2015 = merge_holidays(all_holidays)

    # 4. Guardamos JSON
    with open("holidays_2015.json", "w", encoding="utf-8") as f:
        json.dump(holidays_2015, f, ensure_ascii=False, indent=2)

    print(f"Generado holidays_2015.json con {len(holidays_2015)} registros.")

if __name__ == "__main__":
    main()
