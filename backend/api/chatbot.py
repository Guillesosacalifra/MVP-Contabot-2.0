import os
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from langchain_community.chat_models import ChatOpenAI
from langchain.agents import AgentType
from langchain_community.agent_toolkits.sql.base import create_sql_agent
from langchain_community.utilities import SQLDatabase
from langchain.schema import HumanMessage, AIMessage, SystemMessage
from dotenv import load_dotenv
from supabase import create_client, Client
import time

# Inicialización
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not OPENAI_API_KEY:
    print("⚠️ Advertencia: Faltan variables de entorno críticas")
    print(f"SUPABASE_URL: {'✅' if SUPABASE_URL else '❌'}")
    print(f"SUPABASE_KEY: {'✅' if SUPABASE_KEY else '❌'}")
    print(f"OPENAI_API_KEY: {'✅' if OPENAI_API_KEY else '❌'}")
    raise RuntimeError("❌ Faltan configurar SUPABASE_URL, SUPABASE_KEY u OPENAI_API_KEY en .env")

# Inicializar cliente de Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

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

# Configurar CORS para el router
router.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:5173",
        "http://localhost:8080",
        "https://preview--contia.lovable.app",
        "https://contia.lovable.app"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Modelos
class ConsultaRequest(BaseModel):
    pregunta: str
    año: int
    usuario: str
    tabla_datos: str

class Respuesta(BaseModel):
    respuesta: str

# Funciones auxiliares
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

# Endpoints
@router.post("/consultar", response_model=Respuesta)
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
        
        # Crear conexión SQL con reintentos
        try:
            sql_db = SQLDatabase.from_uri(
                f"postgresql://postgres:{SUPABASE_KEY}@{SUPABASE_URL.replace('https://', '')}/postgres",
                include_tables=[tabla_objetivo],
                engine_args={
                    'poolclass': QueuePool,
                    'pool_size': 5,
                    'max_overflow': 10,
                    'pool_timeout': 30,
                    'pool_recycle': 1800,
                    'pool_pre_ping': True
                }
            )
        except Exception as e:
            print(f"❌ Error creando conexión SQL: {e}")
            raise HTTPException(status_code=500, detail="Error de conexión a la base de datos")

        agent = create_sql_agent(
            llm=llm,
            db=sql_db,
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