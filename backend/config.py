# etl/config.py
 # rutas, claves, settings
import os
from dotenv import load_dotenv
load_dotenv()

def get_db_path() -> str:
    """
    Devuelve y asegura la existencia de la carpeta que contiene la base SQLite.
    """
    db_path = os.getenv("DB_PATH", "./data/facturas_xml_items.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    return db_path

def get_carpeta_descarga() -> str:
    """
    Devuelve y asegura la existencia de la carpeta de descarga.
    """
    carpeta = os.getenv("CARPETA_DESCARGA")
    os.makedirs(carpeta, exist_ok=True)
    return carpeta

def get_carpeta_procesados() -> str:
    """
    Devuelve y asegura la existencia de la carpeta de archivos procesados.
    """
    carpeta = os.getenv("CARPETA_PROCESADOS", "./data/xmls_procesados")
    os.makedirs(carpeta, exist_ok=True)
    return carpeta

def get_datalogic_credentials():
    return {
        "url_login": os.getenv("URL_DATALOGIC"),
        "usuario": os.getenv("USUARIO_DATALOGIC"),
        "contrasena": os.getenv("CLAVE_DATALOGIC"),
        "empresa": "REDOSRL - REDOMON URUGUAY SRL"
    }

# Locale para español (depende del sistema operativo)
LOCALE_ES = "es_ES.UTF-8" if os.name != "nt" else "Spanish_Spain"

# Diccionario de meses en español
MESES_ES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12
}

