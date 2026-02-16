# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ccee8ca9-4e3a-45c9-b786-3b544967b329",
# META       "default_lakehouse_name": "lkontologiessource",
# META       "default_lakehouse_workspace_id": "ba719d48-0ec5-4425-846f-885d29d89764",
# META       "known_lakehouses": [
# META         {
# META           "id": "ccee8ca9-4e3a-45c9-b786-3b544967b329"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# declare workspaceid and other rstuff
workspace_id = "ba719d48-0ec5-4425-846f-885d29d89764"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# read exixting ontologies
import requests, json

# === configura tu workspace ===
#workspace_id = "d16cb8b3-dbea-4fbe-8788-4161d982473a"

import requests

# Token con audiencia Fabric API (recomendado en notebooks de Fabric)
try:
    access_token = mssparkutils.credentials.getToken("https://api.fabric.microsoft.com/")
except NameError:
    # Alternativa si tu runtime expone notebookutils
    access_token = notebookutils.credentials.getToken('pbi')

base = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/ontologies"
headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json"
}

ontologies = []
url = base
while True:
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        raise RuntimeError(f"Error {resp.status_code}: {resp.text}")

    page = resp.json()
    ontologies.extend(page.get("value", []))
    token = page.get("continuationToken")
    if not token:
        break
    url = f"{base}?continuationToken={token}"

if not ontologies:
    print("No hay ontologías en este workspace.")
else:
    print(f"Ontologías encontradas: {len(ontologies)}\n")
    for o in ontologies:
        print(f"- {o.get('displayName')}  |  id: {o.get('id')}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ALmacena una ontologia concreta en la ruta especificada por defecto 
import json, base64, time, requests
from pathlib import Path
import json, base64, time, requests
from urllib.parse import urlparse

# ===== CONFIGURA =====
#workspace_id = "d16cb8b3-dbea-4fbe-8788-4161d982473a"
ontology_id  = "ae1c5653-98f9-4c73-be27-c8f0f8b0701d"
output_dir   = "/lakehouse/default/Files/ontology_export"





# Token con audiencia Fabric API
access_token = mssparkutils.credentials.getToken("https://api.fabric.microsoft.com/")

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# 1) Lanza la operación LRO para obtener la definición (SIN format)
start_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/ontologies/{ontology_id}/getDefinition"
resp = requests.post(start_url, headers=headers, data="{}")
print("start:", resp.status_code)

if resp.status_code not in (200, 202):
    raise RuntimeError(f"Error {resp.status_code}: {resp.text}")

# 2) Obtén operationId (de x-ms-operation-id o a partir de Location)
op_id = resp.headers.get("x-ms-operation-id")
loc   = resp.headers.get("Location")

def extract_op_id_from_location(location_url: str):
    # Espera rutas del estilo .../v1/operations/{operationId}
    if not location_url:
        return None
    path = urlparse(location_url).path
    parts = [p for p in path.split("/") if p]
    # Busca el segmento "operations" y toma el siguiente como ID
    for i, seg in enumerate(parts):
        if seg.lower() == "operations" and i + 1 < len(parts):
            return parts[i+1]
    return None

if not op_id:
    op_id = extract_op_id_from_location(loc)

if not op_id:
    raise RuntimeError("No pude obtener operationId (ni x-ms-operation-id ni Location).")

print("operationId:", op_id)

# 3) Poll de ESTADO hasta Succeeded (GET /v1/operations/{operationId})
state_url = f"https://api.fabric.microsoft.com/v1/operations/{op_id}"



poll_interval_sec = 2
max_polls = 20

status = None
for i in range(max_polls):
    r_state = requests.get(state_url, headers=headers)
    if r_state.status_code != 200:
        raise RuntimeError(f"Error obteniendo estado {r_state.status_code}: {r_state.text}")
    st = r_state.json() if r_state.text else {}
    status = st.get("status")

    # El header Location de este GET puede cambiar a .../result cuando termina
    loc2 = r_state.headers.get("Location")
    print(f"poll {i+1}: status={status} Location={loc2}")

    if status in ("Succeeded", "Completed"):
        break
    elif status == "Failed":
        raise RuntimeError(f"Operación fallida: {r_state.text}")

    # Respeta Retry-After si viene
    retry_after = int(r_state.headers.get("Retry-After", "2"))
    time.sleep(retry_after)

else:
    raise TimeoutError("Timeout esperando a que la operación finalice")

# 4) Una vez Succeeded, pide el RESULTADO (GET /v1/operations/{operationId}/result)
result_url = f"https://api.fabric.microsoft.com/v1/operations/{op_id}/result"
r_res = requests.get(result_url, headers=headers)
print("result:", r_res.status_code, r_res.headers.get("Content-Type"))

if r_res.status_code != 200:
    raise RuntimeError(f"Error al obtener el resultado: {r_res.status_code} - {r_res.text}")

data = r_res.json() if r_res.text else {}
if "definition" not in data:
    raise RuntimeError(f"El resultado no contiene 'definition'. Body: {r_res.text[:600]}")

definition = data["definition"]
parts = definition.get("parts", [])
print("parts:", len(parts))

# 5) Decodificar y guardar parts
root = Path(output_dir)
root.mkdir(parents=True, exist_ok=True)

def write_part(part):
    path = part["path"]                # p.ej. "definition.json" o "EntityTypes/.../definition.json"
    payload = part.get("payload", "")
    ptype = part.get("payloadType", "InlineBase64")
    dest = root / path
    dest.parent.mkdir(parents=True, exist_ok=True)
    if ptype != "InlineBase64":
        raise NotImplementedError(f"payloadType no soportado: {ptype}")
    raw = base64.b64decode(payload) if payload else b""
    with open(dest, "wb") as f:
        f.write(raw)
    return str(dest)

written = [write_part(p) for p in parts]
print("Ejemplos guardados:", written[:5])

# Vista rápida de definition.json si existe
dpath = root / "definition.json"
if dpath.exists():
    print("definition.json (primeros 800 chars):")
    print(dpath.read_text(encoding="utf-8")[:800], "...")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
