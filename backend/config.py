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
    """
    Returns a list of dictionaries containing credentials for each client.
    Each client's credentials should be defined in .env with a numeric suffix:
    CLIENT1_URL_DATALOGIC, CLIENT1_USUARIO_DATALOGIC, etc.
    CLIENT2_URL_DATALOGIC, CLIENT2_USUARIO_DATALOGIC, etc.
    """
    clients = []
    client_num = 1
    
    while True:
        # Check if we have credentials for this client number
        url = os.getenv(f"CLIENT{client_num}_URL_DATALOGIC")
        if not url:  # No more clients found
            break
            
        clients.append({
            "client_id": client_num,
            "url_login": url,
            "usuario": os.getenv(f"CLIENT{client_num}_USUARIO_DATALOGIC"),
            "contrasena": os.getenv(f"CLIENT{client_num}_CLAVE_DATALOGIC"),
            "empresa": os.getenv(f"CLIENT{client_num}_EMPRESA_DATALOGIC")
        })
        client_num += 1
    
    if not clients:
        raise ValueError("No client credentials found in environment variables")
        
    return clients

# Locale para español (depende del sistema operativo)
LOCALE_ES = "es_ES.UTF-8" if os.name != "nt" else "Spanish_Spain"

# Diccionario de meses en español
MESES_ES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12
}

