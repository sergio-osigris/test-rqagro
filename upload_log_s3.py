import os, boto3
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
# Obtener el directorio raíz del proyecto
ROOT_DIR = Path.cwd()

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

# Subir archivo log a s3
s3_client.upload_file(ROOT_DIR / "rqagro_weekly.log", "infoserver-file-bucket", "rqagro_weekly.log")