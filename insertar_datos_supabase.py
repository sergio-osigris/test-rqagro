from supabase import create_client
import json, datetime, os, boto3
import pandas as pd
import math
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()
# DESCARGA FICHERO JSON
# Obtener el valor de la variable de entorno
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_DEFAULT_REGION")

# Se crea una sesión para conectar con S3
if aws_access_key and aws_secret_key and aws_region:
    s3_session = boto3.Session(aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=aws_region)
else:
    s3_session = boto3.Session(profile_name='Master', region_name='eu-south-2')
s3_client = s3_session.client("s3")

def get_bucket_file_log(file):
    try:
        s3_response_object = s3_client.get_object(Bucket='master-file-bucket', Key=file)
        file_content = s3_response_object['Body'].read().decode('utf-8')
        return file_content
    except ClientError:
        print('Error al descargar el fichero ' + file)
        return None
    
week = int(datetime.datetime.now().strftime("%U")) + 1
file = get_bucket_file_log('tphytosanitary.json')
with open(f"jsonFormateado_semana-{week}.json", "w", encoding='utf-8') as tphytoexfile:
        file = file.replace('\r', '')  
        tphytoexfile.write(file) 

# === Conexión a Supabase ===
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase = create_client(url, key)

# Cargar el JSON desde un archivo
with open('jsonFormateado_semana-'+str(int(datetime.datetime.now().strftime("%U"))+1)+'.json', 'r', encoding='utf-8') as file:
    datos = json.load(file)

# Extraer solo info.name e info.code
filas = []
for item in datos:
    nombre = item.get("info", {}).get("name")
    codigo = item.get("info", {}).get("code")
    if nombre and codigo:
        filas.append({"name": nombre, "num_registro": codigo})
    for designation in item.get("designations", []):
        nombre_designation = designation.get("name")
        if nombre_designation:
            filas.append({"name": nombre_designation.strip(), "num_registro": codigo})

# # === Cargar archivo Excel ===
archivo = "top50_materiasActivas.xlsx"
df = pd.read_excel(archivo)

# Extraer por posición: columna 0 = código, columna 1 = nombre
for _, row in df.iterrows():
    codigo = str(row.iloc[0])+"-9999"
    nombre = row.iloc[1]
    if pd.notna(codigo) and pd.notna(nombre):
        filas.append({
            "name": str(nombre).strip(),
            "num_registro": str(codigo).strip()
        })

# === Eliminar todas las filas previas ===
print("Eliminando registros existentes...")
delete_res = supabase.table("fitosanitarios").delete().neq("num_registro", "").execute()
num_eliminados = len(delete_res.data) if delete_res.data else 0
print("Registros eliminados:", num_eliminados)

# === Insertar en bloques de 500 ===
if filas:
    batch_size = 500
    total = len(filas)
    num_batches = math.ceil(total / batch_size)

    for i in range(num_batches):
        start = i * batch_size
        end = start + batch_size
        batch = filas[start:end]
        res = supabase.table("fitosanitarios").insert(batch).execute()
        print(f"Bloque {i+1}/{num_batches} insertado. Filas: {len(batch)}")

    print(f"✅ Inserción completa. Total filas insertadas: {total}")
else:
    print("No hay filas válidas para insertar.")
