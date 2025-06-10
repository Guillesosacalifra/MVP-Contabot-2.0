import os
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel
# from langchain_openai import OpenAI
from langchain_community.chat_models import ChatOpenAI
from langchain.agents import AgentType
from langchain_community.agent_toolkits.sql.base import create_sql_agent
from langchain_community.utilities import SQLDatabase
from langchain.schema import HumanMessage, AIMessage, SystemMessage
from dotenv import load_dotenv
from supabase import create_client, Client
# from sqlalchemy.pool import QueuePool
# from sqlalchemy import create_engine
import time
from functools import wraps

# Decorador para reintentos
def retry_db_connection(max_retries=3, delay=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise e
                    print(f"⚠️ Intento {attempt + 1} fallido, reintentando en {delay}s...")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

# Inicialización
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not OPENAI_API_KEY:
    print("⚠️ Advertencia: Faltan variables de entorno críticas")
    print(f"SUPABASE_URL: {'✅' if SUPABASE_URL else '❌'}")
    print(f"SUPABASE_KEY: {'✅' if SUPABASE_KEY else '❌'}")
    print(f"OPENAI_API_KEY: {'✅' if OPENAI_API_KEY else '❌'}")
    raise RuntimeError("❌ Faltan configurar SUPABASE_URL, SUPABASE_KEY u OPENAI_API_KEY en .env")

# Inicializar cliente de Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

@retry_db_connection(max_retries=3, delay=2)
def get_db_connection():
    """Obtiene una conexión a la base de datos usando Supabase"""
    try:
        # Verificar conexión haciendo una consulta simple
        response = supabase.table('historial_chat').select('count').limit(1).execute()
        return response
    except Exception as e:
        print(f"❌ Error conectando a Supabase: {e}")
        raise

# Verificar conexión inicial
try:
    get_db_connection()
    print("✅ Conexión a Supabase establecida")
except Exception as e:
    print(f"❌ Error conectando a Supabase: {e}")
    raise RuntimeError(f"Error de conexión a Supabase: {e}")

router = APIRouter()

# Modelos
class ConsultaRequest(BaseModel):
    pregunta: str
    año: int
    usuario: str
    tabla_datos: str

class Respuesta(BaseModel):
    respuesta: str

@retry_db_connection(max_retries=3, delay=2)
def obtener_historial_usuario(usuario: str, limite: int = 5):
    try:
        response = supabase.table('historial_chat')\
            .select('pregunta, respuesta')\
            .eq('usuario', usuario)\
            .order('fecha', desc=True)\
            .limit(limite)\
            .execute()
        return response.data
    except Exception as e:
        print(f"⚠️ Error al obtener historial de {usuario}: {e}")
        return []

@retry_db_connection(max_retries=3, delay=2)
def ejecutar_consulta_sql(tabla: str, query: str):
    """Ejecuta una consulta SQL usando la API REST de Supabase"""
    try:
        # Primero verificamos que la tabla existe
        response = supabase.table(tabla).select('count').limit(1).execute()
        
        # Luego ejecutamos la consulta
        response = supabase.rpc('execute_sql', {'query': query}).execute()
        return response.data
    except Exception as e:
        print(f"❌ Error ejecutando consulta SQL: {e}")
        raise

# Endpoints
@router.post("/consultar", response_model=Respuesta)
@retry_db_connection(max_retries=3, delay=2)
def consultar_datos(request: ConsultaRequest):
    print(f"[Backend] Recibida consulta de {request.usuario}: {request.pregunta}")
    print(f"[Backend] Usando tabla: {request.tabla_datos}")
    tabla_objetivo = request.tabla_datos

    try:
        # Verificar que la tabla existe
        response = supabase.table(tabla_objetivo).select('count').limit(1).execute()
        
        # Obtener historial anterior
        historial = obtener_historial_usuario(request.usuario)

        # Construir mensajes de contexto
        mensajes = [
            SystemMessage(content=f"""
                Eres un asistente financiero amigable que responde consultas en español sobre gastos y facturas.
                Estás trabajando con la tabla {tabla_objetivo} que contiene datos específicos de una empresa.
                Instrucciones importantes:
                1. Responde en lenguaje natural y conciso
                2. No muestres consultas SQL
                3. Formatea montos con símbolo $ y separadores de miles
                4. Usa listas o tablas simples si hay muchos datos
                5. Sé claro con fechas y categorías
                6. Si no hay datos, responde "No se encontraron registros"
                7. No uses jerga técnica
                8. La tabla {tabla_objetivo} contiene datos específicos de la empresa del usuario
            """)
        ]
        for fila in reversed(historial):
            mensajes.append(HumanMessage(content=fila['pregunta']))
            mensajes.append(AIMessage(content=fila['respuesta']))

        mensajes.append(HumanMessage(content=request.pregunta))

        # Crear modelo y agente
        llm = ChatOpenAI(
            openai_api_key=OPENAI_API_KEY,
            model_name="gpt-3.5-turbo",
            temperature=0
        )

        # Crear una clase personalizada para manejar las consultas SQL
        class CustomSQLDatabase:
            def __init__(self, tabla):
                self.tabla = tabla

            def run(self, query):
                return ejecutar_consulta_sql(self.tabla, query)

        # Crear instancia de la base de datos personalizada
        db = CustomSQLDatabase(tabla_objetivo)
        
        # Crear el agente con la base de datos personalizada
        agent = create_sql_agent(
            llm=llm,
            db=db,
            agent_type=AgentType.OPENAI_FUNCTIONS,
            verbose=False
        )
        
        respuesta_llm = agent.invoke(mensajes)
        respuesta_final = respuesta_llm.get('output', str(respuesta_llm)).strip()
        respuesta_final = respuesta_final.replace("```sql", "").replace("```", "")

        # Guardar en historial_chat
        try:
            fecha_actual = datetime.utcnow().isoformat()
            supabase.table('historial_chat').insert({
                'fecha': fecha_actual,
                'usuario': request.usuario,
                'pregunta': request.pregunta,
                'respuesta': respuesta_final
            }).execute()
        except Exception as e:
            print(f"⚠️ Error al guardar en historial: {e}")

        return Respuesta(respuesta=respuesta_final)

    except Exception as e:
        print(f"❌ Error en consulta: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.options("/consultar")
async def consultar_options():
    return JSONResponse(content={}) 