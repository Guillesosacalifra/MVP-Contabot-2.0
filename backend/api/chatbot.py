import os
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel
from sqlalchemy import create_engine, inspect, text
# from langchain.chat_models import ChatOpenAI
from langchain_community.chat_models import ChatOpenAI
# from langchain.agents import create_sql_agent, AgentType
from langchain.agents import AgentType
from langchain_community.agent_toolkits.sql.base import create_sql_agent
# from langchain.sql_database import SQLDatabase
from langchain_community.utilities import SQLDatabase
from langchain.schema import HumanMessage, AIMessage, SystemMessage
from dotenv import load_dotenv

# Inicialización
load_dotenv()

SUPABASE_DB_URI = os.getenv("SUPABASE_URI")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not SUPABASE_DB_URI or not OPENAI_API_KEY:
    print("⚠️ Advertencia: Faltan variables de entorno críticas")
    print(f"SUPABASE_URI: {'✅' if SUPABASE_DB_URI else '❌'}")
    print(f"OPENAI_API_KEY: {'✅' if OPENAI_API_KEY else '❌'}")
    raise RuntimeError("❌ Faltan configurar SUPABASE_URI u OPENAI_API_KEY en .env")

try:
    engine = create_engine(SUPABASE_DB_URI)
    # Verificar conexión
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("✅ Conexión a base de datos establecida")
except Exception as e:
    print(f"❌ Error conectando a la base de datos: {e}")
    raise RuntimeError(f"Error de conexión a la base de datos: {e}")

router = APIRouter()

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
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT pregunta, respuesta
                    FROM historial_chat
                    WHERE usuario = :usuario
                    ORDER BY fecha DESC
                    LIMIT :limite
                """), {"usuario": usuario, "limite": limite}
            )
            return result.fetchall()
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
        # Validar tabla
        inspector = inspect(engine)
        if tabla_objetivo not in inspector.get_table_names():
            raise HTTPException(status_code=400, detail=f"No existe la tabla {tabla_objetivo} en Supabase")

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
            mensajes.append(HumanMessage(content=fila.pregunta))
            mensajes.append(AIMessage(content=fila.respuesta))

        mensajes.append(HumanMessage(content=request.pregunta))

        # Crear modelo y agente
        llm = ChatOpenAI(
            openai_api_key=OPENAI_API_KEY,
            model_name="gpt-4o-mini",
            temperature=0
        )
        sql_db = SQLDatabase.from_uri(SUPABASE_DB_URI, include_tables=[tabla_objetivo])
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
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO historial_chat (fecha, usuario, pregunta, respuesta)
                        VALUES (:fecha, :usuario, :pregunta, :respuesta)
                    """), {
                        "fecha": fecha_actual,
                        "usuario": request.usuario,
                        "pregunta": request.pregunta,
                        "respuesta": respuesta_final
                    }
                )
        except Exception as e:
            print(f"⚠️ Error guardando historial: {e}")

        return Respuesta(respuesta=respuesta_final)

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print("❌ Error interno:")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error procesando la consulta: {str(e)}")

@router.options("/consultar")
async def consultar_options():
    return JSONResponse(content={}, headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "*",
    }) 