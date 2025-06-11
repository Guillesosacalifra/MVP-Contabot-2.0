import os
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from langchain_community.chat_models import ChatOpenAI
from langchain.schema import HumanMessage, SystemMessage
from dotenv import load_dotenv
from supabase import create_client, Client
import time
from functools import wraps
from typing import List, Optional, Any, Dict
import json
import re
from dateutil import parser as date_parser

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
    raise RuntimeError("❌ Faltan configurar variables de entorno")

# Inicializar cliente de Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

router = APIRouter()

# Modelos
class ConsultaRequest(BaseModel):
    pregunta: str
    año: int
    usuario: str
    tabla_datos: str

class Respuesta(BaseModel):
    respuesta: str

# Templates SQL seguros - Define aquí todas las consultas posibles
SQL_TEMPLATES = {
    "gastos_por_categoria": """
        SELECT 
            categoria,
            SUM(monto) as total_gastado,
            COUNT(*) as cantidad_transacciones
        FROM {tabla}
        WHERE DATE(fecha) BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
        AND categoria ILIKE '%{categoria}%'
        GROUP BY categoria
        ORDER BY total_gastado DESC
    """,
    
    "gastos_por_periodo": """
        SELECT 
            DATE(fecha) as fecha,
            descripcion,
            categoria,
            monto
        FROM {tabla}
        WHERE DATE(fecha) BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
        ORDER BY fecha DESC, monto DESC
    """,
    
    "total_gastos_categoria": """
        SELECT 
            SUM(monto) as total
        FROM {tabla}
        WHERE DATE(fecha) BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
        AND categoria ILIKE '%{categoria}%'
    """,
    
    "gastos_mensuales": """
        SELECT 
            EXTRACT(MONTH FROM fecha) as mes,
            EXTRACT(YEAR FROM fecha) as año,
            SUM(monto) as total_mes
        FROM {tabla}
        WHERE EXTRACT(YEAR FROM fecha) = {año}
        AND categoria ILIKE '%{categoria}%'
        GROUP BY EXTRACT(YEAR FROM fecha), EXTRACT(MONTH FROM fecha)
        ORDER BY año, mes
    """,
    
    "top_gastos": """
        SELECT 
            fecha,
            descripcion,
            categoria,
            monto
        FROM {tabla}
        WHERE DATE(fecha) BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
        ORDER BY monto DESC
        LIMIT {limite}
    """,
    
    "buscar_descripcion": """
        SELECT 
            fecha,
            descripcion,
            categoria,
            monto
        FROM {tabla}
        WHERE descripcion ILIKE '%{termino}%'
        AND DATE(fecha) BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
        ORDER BY fecha DESC
    """,
    
    "gastos_por_proveedor": """
        SELECT 
            proveedor,
            SUM(monto) as total_gastado,
            COUNT(*) as cantidad_transacciones,
            AVG(monto) as promedio_gasto
        FROM {tabla}
        WHERE DATE(fecha) BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
        AND proveedor ILIKE '%{proveedor}%'
        GROUP BY proveedor
        ORDER BY total_gastado DESC
    """,
    
    "comparativa_mensual": """
        SELECT 
            EXTRACT(MONTH FROM fecha) as mes,
            EXTRACT(YEAR FROM fecha) as año,
            SUM(monto) as total_mes,
            LAG(SUM(monto)) OVER (ORDER BY EXTRACT(YEAR FROM fecha), EXTRACT(MONTH FROM fecha)) as mes_anterior,
            ROUND(((SUM(monto) - LAG(SUM(monto)) OVER (ORDER BY EXTRACT(YEAR FROM fecha), EXTRACT(MONTH FROM fecha))) / 
                   NULLIF(LAG(SUM(monto)) OVER (ORDER BY EXTRACT(YEAR FROM fecha), EXTRACT(MONTH FROM fecha)), 0) * 100), 2) as variacion_porcentual
        FROM {tabla}
        WHERE DATE(fecha) BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
        GROUP BY EXTRACT(YEAR FROM fecha), EXTRACT(MONTH FROM fecha)
        ORDER BY año, mes
    """,
    
    "comparativa_semanal": """
        SELECT 
            EXTRACT(WEEK FROM fecha) as semana,
            EXTRACT(YEAR FROM fecha) as año,
            DATE_TRUNC('week', fecha) as inicio_semana,
            SUM(monto) as total_semana,
            COUNT(*) as transacciones_semana
        FROM {tabla}
        WHERE DATE(fecha) BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
        GROUP BY EXTRACT(YEAR FROM fecha), EXTRACT(WEEK FROM fecha), DATE_TRUNC('week', fecha)
        ORDER BY año, semana
    """,
    
    "comparativa_diaria": """
        SELECT 
            DATE(fecha) as dia,
            SUM(monto) as total_dia,
            COUNT(*) as transacciones_dia,
            STRING_AGG(DISTINCT categoria, ', ') as categorias
        FROM {tabla}
        WHERE DATE(fecha) BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
        GROUP BY DATE(fecha)
        ORDER BY dia DESC
    """,
    
    "gastos_por_moneda": """
        SELECT 
            moneda,
            SUM(monto) as total_por_moneda,
            COUNT(*) as cantidad_transacciones,
            AVG(monto) as promedio_monto,
            MIN(monto) as monto_minimo,
            MAX(monto) as monto_maximo
        FROM {tabla}
        WHERE DATE(fecha) BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
        AND moneda ILIKE '%{moneda}%'
        GROUP BY moneda
        ORDER BY total_por_moneda DESC
    """,
    
    "top_proveedores": """
        SELECT 
            proveedor,
            SUM(monto) as total_gastado,
            COUNT(*) as cantidad_compras,
            AVG(monto) as ticket_promedio,
            MAX(fecha) as ultima_compra
        FROM {tabla}
        WHERE DATE(fecha) BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
        GROUP BY proveedor
        ORDER BY total_gastado DESC
        LIMIT {limite}
    """,
    
    "resumen_periodo": """
        SELECT 
            COUNT(*) as total_transacciones,
            SUM(monto) as total_gastado,
            AVG(monto) as promedio_transaccion,
            COUNT(DISTINCT categoria) as categorias_diferentes,
            COUNT(DISTINCT proveedor) as proveedores_diferentes,
            MIN(fecha) as fecha_inicio,
            MAX(fecha) as fecha_fin
        FROM {tabla}
        WHERE DATE(fecha) BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
    """
}

class QueryAnalyzer:
    """Analiza consultas y las mapea a templates SQL seguros"""
    
    def __init__(self, openai_api_key: str):
        self.llm = ChatOpenAI(
            openai_api_key=openai_api_key,
            model_name="gpt-3.5-turbo",
            temperature=0
        )
    
    def extraer_parametros(self, pregunta: str, año: int) -> Dict:
        """Extrae parámetros estructurados de la pregunta del usuario"""
        
        prompt = f"""
        Analiza esta consulta financiera y extrae los parámetros exactos.

        PREGUNTA: "{pregunta}"
        AÑO CONTEXTO: {año}

        Extrae ÚNICAMENTE estos parámetros en formato JSON:
        {{
            "template": "nombre_del_template_mas_apropiado",
            "categoria": "categoria_si_se_menciona_o_null",
            "proveedor": "proveedor_si_se_menciona_o_null",
            "moneda": "moneda_si_se_menciona_o_null",
            "fecha_inicio": "YYYY-MM-DD",
            "fecha_fin": "YYYY-MM-DD", 
            "termino_busqueda": "termino_si_busca_descripcion_o_null",
            "limite": numero_si_pide_top_o_10,
            "tipo_consulta": "suma|detalle|top|busqueda|mensual|semanal|diario|proveedor|moneda|comparativa"
        }}

        TEMPLATES DISPONIBLES:
        - gastos_por_categoria: Para agrupar gastos por categoría
        - gastos_por_periodo: Para listar gastos en un período
        - total_gastos_categoria: Para sumar total de una categoría
        - gastos_mensuales: Para gastos agrupados por mes
        - top_gastos: Para los gastos más altos
        - buscar_descripcion: Para buscar por descripción
        - gastos_por_proveedor: Para agrupar gastos por proveedor
        - comparativa_mensual: Para comparar gastos entre meses
        - comparativa_semanal: Para comparar gastos por semanas
        - comparativa_diaria: Para comparar gastos por días
        - gastos_por_moneda: Para agrupar gastos por moneda
        - top_proveedores: Para los proveedores con más gastos
        - resumen_periodo: Para resumen general de un período

        REGLAS:
        - Si menciona un mes (marzo, abril, etc), usa ese mes del año {año}
        - Si no especifica fechas, usa todo el año {año}
        - Para categorías como "combustible", usa "combustible" (sin tildes ni mayúsculas)
        - Fechas siempre en formato YYYY-MM-DD

        RESPONDE SOLO EL JSON:
        """
        
        try:
            response = self.llm.invoke([HumanMessage(content=prompt)])
            contenido = response.content.strip()
            
            # Limpiar respuesta
            if contenido.startswith('```json'):
                contenido = contenido[7:-3]
            elif contenido.startswith('```'):
                contenido = contenido[3:-3]
            
            return json.loads(contenido)
        except Exception as e:
            print(f"❌ Error extrayendo parámetros: {e}")
            # Fallback básico
            return {
                "template": "gastos_por_periodo",
                "categoria": None,
                "fecha_inicio": f"{año}-01-01",
                "fecha_fin": f"{año}-12-31",
                "termino_busqueda": None,
                "limite": 10,
                "tipo_consulta": "detalle"
            }

def construir_sql_seguro(template_name: str, parametros: Dict, tabla: str) -> str:
    """Construye SQL usando templates seguros"""
    
    if template_name not in SQL_TEMPLATES:
        template_name = "gastos_por_periodo"  # Fallback
    
    template = SQL_TEMPLATES[template_name]
    
    # Sanitizar parámetros
    params_seguros = {
        'tabla': tabla,  # Validado previamente
        'fecha_inicio': parametros.get('fecha_inicio', '2025-01-01'),
        'fecha_fin': parametros.get('fecha_fin', '2025-12-31'),
        'categoria': (parametros.get('categoria') or '').replace("'", "''"),  # Escape SQL
        'proveedor': (parametros.get('proveedor') or '').replace("'", "''"),
        'moneda': (parametros.get('moneda') or '').replace("'", "''"),
        'termino': (parametros.get('termino_busqueda') or '').replace("'", "''"),
        'año': int(parametros.get('año', 2025)),
        'limite': min(int(parametros.get('limite', 10)), 100)  # Máximo 100
    }
    
    try:
        sql = template.format(**params_seguros)
        print(f"[SQL] Consulta generada: {sql}")
        return sql
    except Exception as e:
        print(f"❌ Error construyendo SQL: {e}")
        # Fallback seguro
        return f"SELECT * FROM {tabla} LIMIT 10"

@retry_db_connection(max_retries=3, delay=2)
def ejecutar_sql_supabase(sql: str) -> List[Dict]:
    """Ejecuta SQL usando la función RPC de Supabase"""
    try:
        # Usar la función SQL personalizada de Supabase
        response = supabase.rpc('execute_sql', {'sql_query': sql}).execute()
        return response.data or []
    except Exception as e:
        print(f"❌ Error ejecutando SQL: {e}")
        return []

def formatear_respuesta_natural(datos: List[Dict], parametros: Dict, pregunta: str) -> str:
    """Convierte los datos SQL en respuesta natural"""
    
    if not datos:
        return "No se encontraron registros para esa consulta."
    
    llm = ChatOpenAI(
        openai_api_key=OPENAI_API_KEY,
        model_name="gpt-3.5-turbo",
        temperature=0
    )
    
    # Limitar datos para el prompt (para evitar tokens excesivos)
    datos_muestra = datos[:20] if len(datos) > 20 else datos
    total_registros = len(datos)
    
    prompt = f"""
    Responde esta consulta financiera de manera clara y profesional en español.

    PREGUNTA: "{pregunta}"
    DATOS OBTENIDOS: {json.dumps(datos_muestra, indent=2, default=str)}
    TOTAL DE REGISTROS: {total_registros}

    INSTRUCCIONES:
    1. Responde directamente la pregunta
    2. Formatea montos como $1,234.56
    3. Si hay muchos registros, resume los principales
    4. Usa fechas legibles (ej: "marzo 2025")
    5. Sé conciso pero completo
    6. NO menciones código SQL ni técnico

    RESPUESTA:
    """
    
    try:
        response = llm.invoke([HumanMessage(content=prompt)])
        return response.content.strip()
    except Exception as e:
        print(f"❌ Error formateando respuesta: {e}")
        return f"Se encontraron {total_registros} registros, pero hubo un error al formatear la respuesta."

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

# Endpoint principal
@router.post("/consultar", response_model=Respuesta)
def consultar_datos(request: ConsultaRequest):
    print(f"[Backend] Recibida consulta de {request.usuario}: {request.pregunta}")
    print(f"[Backend] Usando tabla: {request.tabla_datos}")
    
    try:
        # 1. Analizar la consulta y extraer parámetros
        analyzer = QueryAnalyzer(OPENAI_API_KEY)
        parametros = analyzer.extraer_parametros(request.pregunta, request.año)
        
        print(f"[Backend] Parámetros extraídos: {parametros}")
        
        # 2. Construir SQL seguro usando templates
        sql = construir_sql_seguro(
            parametros['template'], 
            parametros, 
            request.tabla_datos
        )
        
        # 3. Ejecutar SQL real
        datos = ejecutar_sql_supabase(sql)
        
        print(f"[Backend] Registros encontrados: {len(datos)}")
        
        # 4. Formatear respuesta natural
        respuesta_final = formatear_respuesta_natural(datos, parametros, request.pregunta)
        
        # 5. Guardar en historial
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
        return Respuesta(respuesta=f"Error procesando la consulta: {str(e)}")

@router.options("/consultar")
async def consultar_options():
    return JSONResponse(content={})