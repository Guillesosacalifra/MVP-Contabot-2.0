from supabase import create_client
import time 
import os
from dotenv import load_dotenv
from openai import OpenAI
load_dotenv()

# CONFIGURACIÓN
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Inicializar cliente OpenAI
openai_client = OpenAI(api_key=OPENAI_API_KEY)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def generar_embedding(texto):
    response = openai_client.embeddings.create(
        input=texto,
        model="text-embedding-3-small"
    )
    return response.data[0].embedding

def concatenar_columnas_contenido(fila, excluir=("id", "embedding")):
    partes = []
    for clave, valor in fila.items():
        if clave not in excluir and valor is not None:
            partes.append(str(valor))
    return " ".join(partes)

def actualizar_embeddings(tabla):
    # Obtener filas sin embeddings
    filas = supabase.table(tabla).select("*").is_("embedding", "null").execute()
    if not filas.data:
        print("✅ No hay filas sin embeddings.")
        return

    print(f"🔎 Encontradas {len(filas.data)} filas sin embeddings.")
    for fila in filas.data:
        try:
            texto_concatenado = concatenar_columnas_contenido(fila)
            embedding = generar_embedding(texto_concatenado)

            supabase.table(tabla).update({
                "embedding": embedding
            }).eq("id", fila["id"]).execute()

            print(f"✅ Embedding generado para ID {fila['id']}")
            time.sleep(1)  # Evita pasarte del rate limit de OpenAI

        except Exception as e:
            print(f"❌ Error con ID {fila['id']}: {e}")

# Usar la función
actualizar_embeddings("vector_redomon_2025")