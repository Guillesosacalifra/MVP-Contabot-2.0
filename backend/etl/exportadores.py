import os
import re
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
from backend.utils import obtener_numero_mes, obtener_nombre_mes

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_API_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def exportar_json_mes_desde_supabase(mes: str, anio: int, empresa: str):
    print(f"🔄 Descargando datos desde Supabase para {mes} {anio}...")

    # Descargar todos los datos desde la tabla
    tabla = f"{empresa}_{anio}"
    response = supabase.table(tabla).select("*").execute()
    df = pd.DataFrame(response.data)

    # Normalización y filtrado
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df = df[(df["fecha"].dt.month == obtener_numero_mes(mes)) & (df["fecha"].dt.year == anio)]

    if df.empty:
        raise ValueError(f"❌ No hay datos para {mes} {anio} en Supabase.")

    df["ruc"] = df["ruc"].astype(str).str.strip()
    df["monto_item"] = pd.to_numeric(df["monto_item"], errors="coerce")
    df["fecha"] = df["fecha"].dt.strftime("%Y-%m-%d")

    # Crear directorio si no existe
    os.makedirs(f"data/{empresa}", exist_ok=True)
    salida_json = f"data/{empresa}/{mes}_{anio}.json"

    df.to_json(salida_json, orient="records", force_ascii=False, indent=2)
    print(f"✅ Exportado correctamente a {salida_json}")



def exportar_xls_dgi_a_json(path_xls: str):
    # Extraer mes y año del nombre del archivo usando regex
    nombre_archivo = os.path.basename(path_xls)
    match = re.search(r"Periodo-(\d{4})_(\d{1,2})_", nombre_archivo)
    if not match:
        raise ValueError(f"❌ No se pudo extraer el año y mes del nombre: {nombre_archivo}")

    anio = int(match.group(1))
    mes_num = int(match.group(2))
    mes_nombre = obtener_nombre_mes(mes_num)  # ejemplo: "enero"

    # Leer archivo Excel
    df = pd.read_excel(path_xls, skiprows=9)

    # Normalizamos los nombres de columnas para trabajar con nombres consistentes
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # Mostramos columnas para debugging
    print("🧾 Columnas normalizadas:", df.columns.tolist())

    df["rut_emisor"] = df["rut_emisor"].astype(str).str.strip()
    df["monto_total"] = pd.to_numeric(df["monto_total"], errors="coerce")
    df["monto_neto"] = pd.to_numeric(df["monto_neto"], errors="coerce")

    df["mes"] = mes_nombre
    df["anio"] = anio
    
    salida_json = f"data/dgi/dgi_{mes_nombre}_{anio}.json"
    os.makedirs(os.path.dirname(salida_json), exist_ok=True)
    df.to_json(salida_json, orient="records", force_ascii=False, indent=2)

    print(f"✅ Exportado correctamente a {salida_json}")


def exportar_a_json(mes: str, anio: int, empresa: str):
    """
    Exporta los datos a un archivo JSON.
    """
    print(f"📤 Exportando datos a JSON para {mes}/{anio}")
    
    # Descargar todos los datos desde la tabla
    tabla = f"{empresa}_{anio}"
    response = supabase.table(tabla).select("*").execute()
    
    if not response.data:
        print("⚠️ No hay datos para exportar")
        return
    
    # Convertir a DataFrame
    df = pd.DataFrame(response.data)
    
    # Crear directorio si no existe
    os.makedirs("data/datalogic", exist_ok=True)
    
    # Guardar como JSON
    salida_json = f"data/datalogic/{empresa}_{mes}_{anio}.json"
    df.to_json(salida_json, orient="records", date_format="iso")
    print(f"✅ Datos exportados a {salida_json}")




