# etl/supabase_client.py

import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime
import psycopg2
from calendar import monthrange
from backend.utils import obtener_nombre_mes
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")
DATABASE_URL = os.getenv("SUPABASE_URI")  # Cambiado de TRANSACTION_POOLER a SUPABASE_URI

if not SUPABASE_URL or not SUPABASE_API_KEY or not DATABASE_URL:
    raise ValueError("❌ Faltan variables SUPABASE_URL, SUPABASE_API_KEY o SUPABASE_URI en .env")

try:
    # Inicializar cliente Supabase con configuración básica
    supabase: Client = create_client(
        supabase_url=SUPABASE_URL,
        supabase_key=SUPABASE_API_KEY,
        options={
            'schema': 'public',
            'headers': {'X-Client-Info': 'supabase-py/2.3.1'}
        }
    )
except Exception as e:
    print(f"❌ Error inicializando cliente Supabase: {e}")
    raise


def crear_tabla_si_no_existe(df: pd.DataFrame, nombre_tabla: str):
    """
    Crea una tabla en Supabase con los campos del DataFrame si no existe.
    Incluye un campo 'id SERIAL PRIMARY KEY' generado por la base.
    """

    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    columnas_sql = ['id SERIAL PRIMARY KEY']
    
    for col, tipo in df.dtypes.items():
        if col.lower() == "id":
            continue  # Evitar duplicar la columna id

        if "int" in str(tipo):
            columnas_sql.append(f'"{col}" INTEGER')
        elif "float" in str(tipo):
            columnas_sql.append(f'"{col}" REAL')
        elif "bool" in str(tipo):
            columnas_sql.append(f'"{col}" BOOLEAN')
        elif "datetime" in str(tipo):
            columnas_sql.append(f'"{col}" DATE')  # Solo fecha
        else:
            columnas_sql.append(f'"{col}" TEXT')

    sql = f'CREATE TABLE IF NOT EXISTS public."{nombre_tabla}" ({", ".join(columnas_sql)});'
    
    try:
        cursor.execute(sql)
        conn.commit()
        print(f"📦 Tabla '{nombre_tabla}' verificada o creada con ID autoincremental.")
    except Exception as e:
        print(f"❌ Error creando la tabla '{nombre_tabla}': {e}")
    finally:
        cursor.close()
        conn.close()


def subir_dataframe(df: pd.DataFrame, tabla_nombre: str) -> None:
    """
    Sube un DataFrame a Supabase. La tabla se nombra como {empresa}_{año}.
    Si no existe, se crea automáticamente. Si ya existe, se agrega la información.
    Muestra advertencia si ya existen registros del mismo mes y año.
    """
    print(f"⬆️ Subiendo {len(df)} ítems a Supabase...")

    if df.empty:
        print("⚠️ DataFrame vacío, no se sube nada.")
        return

    if "fecha" not in df.columns:
        raise ValueError("❌ El DataFrame debe contener una columna 'fecha' para determinar el año.")

    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    if df["fecha"].isna().all():
        raise ValueError("❌ Ninguna fecha válida encontrada en la columna 'fecha'.")

    anio = df["fecha"].dt.year.mode()[0]
    mes = df["fecha"].dt.month.mode()[0]

    df["año"] = anio
    df["mes"] = obtener_nombre_mes(mes)

    if "verificado" not in df.columns:
        df["verificado"] = False

    # Eliminar columnas que no queremos subir
    columnas_a_eliminar = ["rowid", "id"]
    for col in columnas_a_eliminar:
        if col in df.columns:
            df = df.drop(columns=[col])

    # Crear tabla si no existe
    crear_tabla_si_no_existe(df, tabla_nombre)

    # Verificar si ya hay datos para ese mes en la tabla
    inicio_mes = datetime(anio, mes, 1).strftime("%Y-%m-%d")
    ultimo_dia = monthrange(anio, mes)[1]
    fin_mes = datetime(anio, mes, ultimo_dia).strftime("%Y-%m-%d")

    try:
        resultado = supabase.table(tabla_nombre).select("fecha").gte("fecha", inicio_mes).lte("fecha", fin_mes).limit(1).execute()
        if resultado.data:
            print(f"⚠️ Advertencia: ya existen registros en {tabla_nombre} para el mes {mes:02}/{anio}.")
    except Exception as e:
        print(f"ℹ️ No se pudo verificar existencia previa en {tabla_nombre}: {e}")

    # Convertir fechas a string y manejar valores nulos
    df["fecha"] = df["fecha"].dt.strftime("%Y-%m-%d")
    df = df.where(pd.notnull(df), None)

    # Subir en bloques de 100
    for i in range(0, len(df), 100):
        bloque = df.iloc[i:i+100].to_dict(orient="records")
        try:
            response = supabase.table(tabla_nombre).insert(bloque).execute()
            print(f"✅ Subido bloque {i//100} a {tabla_nombre}")
        except Exception as e:
            print(f"❌ Error al subir bloque {i//100} en tabla {tabla_nombre}: {e}")
            print("🛑 Detalles del bloque con error:")
            for fila in bloque:
                print(fila)
            pd.DataFrame(bloque).to_csv(f"bloque_error_{i//100}.csv", index=False)
            break

def obtener_historico(empresa: str, años: list[int]) -> pd.DataFrame:
    """
    Descarga datos históricos verificados desde Supabase para aplicar la red de pescadores.
    Devuelve un DataFrame con proveedor, descripción, categoría y año.
    """
    print("🧠 Descargando histórico desde Supabase...")
    
    frames = []

    for año in años:
        tabla = f"{empresa}_{año}"
        print(f"🔎 Consultando tabla {tabla}...")

        try:
            res = supabase.table(tabla) \
                .select("proveedor, descripcion, categoria, verificado") \
                .eq("verificado", True) \
                .execute()
            datos = res.data
            if datos:
                df = pd.DataFrame(datos)
                df["año"] = año
                frames.append(df)
        except Exception as e:
            print(f"⚠️ No se pudo consultar la tabla {tabla}: {e}")
    
    if frames:
        df_final = pd.concat(frames, ignore_index=True)
        print(f"📦 Total de registros históricos verificados: {len(df_final)}")
        return df_final
    else:
        print("⚠️ No se encontraron registros históricos verificados.")
        return pd.DataFrame()


