# etl/supabase_client.py

import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime
import psycopg2
from calendar import monthrange

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")
DATABASE_URL = os.getenv("TRANSACTION_POOLER")

if not SUPABASE_URL or not SUPABASE_API_KEY or not DATABASE_URL:
    raise ValueError("❌ Faltan variables SUPABASE_URL, SUPABASE_API_KEY o TRANSACTION_POOLER en .env")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_API_KEY)


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
    Sube un DataFrame a Supabase. La tabla se nombra como datalogic_{año}_test.
    Si no existe, se crea automáticamente. Si ya existe, se agrega la información.
    Muestra advertencia si ya existen registros del mismo mes y año.
    """
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

    if not tabla_nombre:
        tabla_nombre = f"datalogic_{anio}"

    # Elimino id relativo. Supabase debe tener su propio ID serial primary key
    if "rowid" in df.columns:
        df = df.drop(columns=["rowid"])

    # Crear tabla si no existe
    crear_tabla_si_no_existe(df, tabla_nombre)

    # Verificar si ya hay datos para ese mes en la tabla
    inicio_mes = datetime(anio, mes, 1).strftime("%Y-%m-%d")
    ultimo_dia = monthrange(anio, mes)[1]
    fin_mes = datetime(anio, mes, ultimo_dia).strftime("%Y-%m-%d")
    # fin_mes = datetime(anio, mes, 28).replace(day=28).strftime("%Y-%m-%d")

    try:
        resultado = supabase.table(tabla_nombre).select("fecha").gte("fecha", inicio_mes).lte("fecha", fin_mes).limit(1).execute()
        if resultado.data:
            print(f"⚠️ Advertencia: ya existen registros en {tabla_nombre} para el mes {mes:02}/{anio}.")
    except Exception as e:
        print(f"ℹ️ No se pudo verificar existencia previa en {tabla_nombre}: {e}")

    # Convertir fechas a string
    df["fecha"] = df["fecha"].dt.strftime("%Y-%m-%d")
    df = df.where(pd.notnull(df), None)

    for i in range(0, len(df), 100):

        bloque = df.iloc[i:i+100].to_dict(orient="records")
        
        try:
            response = supabase.table(tabla_nombre).insert(bloque).execute()
            print(f"✅ Subido bloque {i} a {tabla_nombre}")
        except Exception as e:

            print(f"❌ Error al subir bloque {i} en tabla {tabla_nombre}: {e}")
            print("🛑 Detalles del bloque con error:")
            for fila in bloque:
                print(fila)
            pd.DataFrame(bloque).to_csv(f"bloque_error_{i}.csv", index=False)
            break

def obtener_historico(años: list[int]) -> pd.DataFrame:
    """
    Descarga datos históricos desde Supabase para aplicar la red de pescadores.
    Devuelve un DataFrame con proveedor, descripicion y categoría.
    """
    frames = []

    for año in años:
        tabla = f"datalogic_{año}"
        print(f"🔎 Consultando tabla {tabla}...")

        try:
            res = supabase.table(tabla).select("proveedor, descripcion, categoria").execute()
            datos = res.data
            if datos:
                df = pd.DataFrame(datos)
                df["año"] = año
                frames.append(df)
        except Exception as e:
            print(f"⚠️ No se pudo consultar la tabla {tabla}: {e}")

    if frames:
        return pd.concat(frames, ignore_index=True)
    else:
        return pd.DataFrame()