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

def validar_nombre_tabla(tabla: str) -> bool:
    """Valida que el nombre de tabla sea seguro"""
    patron = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
    return bool(patron.match(tabla))

def obtener_columnas_tabla(tabla: str) -> List[str]:
    """Obtiene las columnas de la tabla para generar SQL más precisos"""
    try:
        # Consulta simple para obtener la estructura
        response = supabase.rpc('execute_sql', {
            'sql_query': f'SELECT column_name FROM information_schema.columns WHERE table_name = \'{tabla}\' ORDER BY ordinal_position'
        }).execute()
        
        if response.data:
            columnas = [row['column_name'] for row in response.data]
            print(f"[INFO] Columnas de {tabla}: {columnas}")
            return columnas
        else:
            # Fallback: intentar obtener columnas con LIMIT 1
            response = supabase.rpc('execute_sql', {
                'sql_query': f'SELECT * FROM {tabla} LIMIT 1'
            }).execute()
            if response.data and len(response.data) > 0:
                return list(response.data[0].keys())
    except Exception as e:
        print(f"⚠️ Error obteniendo columnas: {e}")
    
    # Columnas por defecto comunes
    return ['id', 'fecha', 'descripcion', 'categoria', 'monto', 'proveedor', 'moneda']

class SQLGenerator:
    """Generador de consultas SQL inteligente"""
    
    def __init__(self, openai_api_key: str):
        self.llm = ChatOpenAI(
            openai_api_key=openai_api_key,
            model_name="gpt-4o-mini",
            temperature=0.1
        )
    
    def generar_consultas_sql(self, pregunta: str, tabla: str, año: int, columnas: List[str]) -> List[str]:
        """Genera una o más consultas SQL necesarias para responder la pregunta"""
        
        columnas_str = ", ".join(columnas)
        
        try:
            # Convertir datos a JSON de forma segura
            datos_json = json.dumps(contexto_datos, indent=2, default=str, ensure_ascii=False)
        except Exception as e:
            print(f"⚠️ Error serializando datos: {e}")
            # Fallback: crear representación manual
            datos_json = str(contexto_datos)
        
        prompt = f"""
Eres un experto en SQL. Genera las consultas SQL necesarias para responder esta pregunta sobre datos financieros.

PREGUNTA: "{pregunta}"
TABLA: {tabla}
COLUMNAS DISPONIBLES: {columnas_str}
AÑO CONTEXTO: {año}

REGLAS IMPORTANTES:
1. Genera SOLO las consultas SQL necesarias, una por línea
2. NO uses markdown ni explicaciones, solo SQL puro
3. Usa nombres de columna exactos de la lista proporcionada
4. Para fechas, usa formato YYYY-MM-DD
5. Si no se especifica período, usa todo el año {año}
6. Usa ILIKE para comparaciones de texto (insensible a mayúsculas)
7. Para montos, siempre usa SUM(), AVG(), etc. con nombres descriptivos
8. Usa LIMIT solo cuando sea apropiado (top, mejores, etc.)
9. Para períodos de tiempo, usa DATE() para comparaciones
10. Si necesitas múltiples consultas para una respuesta completa, sepáralas con líneas

EJEMPLOS DE CONSULTAS TÍPICAS:
- SELECT categoria, SUM(monto) as total FROM tabla WHERE EXTRACT(YEAR FROM fecha) = 2024 GROUP BY categoria ORDER BY total DESC;
- SELECT COUNT(*) as total_transacciones, SUM(monto) as total_gastado FROM tabla WHERE fecha >= '2024-01-01' AND fecha <= '2024-12-31';
- SELECT descripcion, monto, fecha FROM tabla WHERE categoria ILIKE '%combustible%' ORDER BY monto DESC LIMIT 5;

RESPONDE SOLO LAS CONSULTAS SQL:
"""
        
        try:
            response = self.llm.invoke([HumanMessage(content=prompt)])
            sql_content = response.content.strip()
            
            # Limpiar y separar consultas
            consultas = []
            for linea in sql_content.split('\n'):
                linea = linea.strip()
                if linea and not linea.startswith('--') and not linea.startswith('#'):
                    # Limpiar markdown si existe
                    if linea.startswith('```'):
                        continue
                    if linea.endswith('```'):
                        continue
                    
                    # Reemplazar placeholder de tabla
                    if '{tabla}' in linea:
                        linea = linea.replace('{tabla}', tabla)
                    elif 'tabla' in linea and tabla not in linea:
                        linea = linea.replace('tabla', tabla)
                    
                    if linea.upper().startswith('SELECT'):
                        consultas.append(linea)
            
            if not consultas:
                # Fallback: consulta básica
                consultas = [f"SELECT * FROM {tabla} WHERE EXTRACT(YEAR FROM fecha) = {año} LIMIT 10"]
            
            print(f"[SQL] Consultas generadas: {consultas}")
            return consultas
            
        except Exception as e:
            print(f"❌ Error generando SQL: {e}")
            return [f"SELECT * FROM {tabla} WHERE EXTRACT(YEAR FROM fecha) = {año} LIMIT 10"]

@retry_db_connection(max_retries=3, delay=2)
def ejecutar_consultas_sql(consultas: List[str]) -> List[Dict]:
    """Ejecuta múltiples consultas SQL y retorna todos los resultados"""
    todos_los_datos = []
    
    for i, sql in enumerate(consultas):
        try:
            print(f"[SQL] Ejecutando consulta {i+1}: {sql}")
            response = supabase.rpc('execute_sql', {'sql_query': sql}).execute()
            
            if response.data:
                datos = response.data
                todos_los_datos.append({
                    'consulta_num': i + 1,
                    'sql': sql,
                    'datos': datos,
                    'total_registros': len(datos)
                })
                print(f"[SQL] Consulta {i+1} retornó {len(datos)} registros")
            else:
                print(f"[SQL] Consulta {i+1} sin resultados")
                
        except Exception as e:
            print(f"❌ Error en consulta {i+1}: {e}")
            todos_los_datos.append({
                'consulta_num': i + 1,
                'sql': sql,
                'datos': [],
                'error': str(e)
            })
    
    return todos_los_datos

class ResponseFormatter:
    """Formateador de respuestas naturales"""
    
    def __init__(self, openai_api_key: str):
        self.llm = ChatOpenAI(
            openai_api_key=openai_api_key,
            model_name="gpt-4o-mini",
            temperature=0.3
        )
    
    def formatear_respuesta(self, pregunta: str, resultados_sql: List[Dict]) -> str:
        """Convierte resultados SQL en respuesta natural"""
        
        if not resultados_sql:
            return "No se pudieron obtener datos para responder tu consulta."
        
        # Verificar si hay datos válidos
        hay_datos = any(r.get('datos') and len(r['datos']) > 0 for r in resultados_sql)
        
        if not hay_datos:
            return "No se encontraron registros que coincidan con tu consulta."
        
        # Preparar contexto para el LLM
        contexto_datos = []
        for resultado in resultados_sql:
            datos = resultado.get('datos')
            if datos and isinstance(datos, list) and len(datos) > 0:
                try:
                    # Limitar datos para evitar exceso de tokens - usar slice seguro
                    datos_muestra = datos[0:15] if len(datos) > 15 else datos
                    contexto_datos.append({
                        'consulta': resultado.get('sql', ''),
                        'datos': datos_muestra,
                        'total': resultado.get('total_registros', 0)
                    })
                except Exception as e:
                    print(f"⚠️ Error procesando datos del resultado: {e}")
                    # Fallback seguro
                    contexto_datos.append({
                        'consulta': resultado.get('sql', ''),
                        'datos': [datos[0]] if datos else [],
                        'total': 1
                    })

        try:
            # Convertir datos a JSON de forma segura
            datos_json = json.dumps(contexto_datos, indent=2, default=str, ensure_ascii=False)
        except Exception as e:
            print(f"⚠️ Error serializando datos: {e}")
            # Fallback: crear representación manual
            datos_json = str(contexto_datos)
        
        prompt = f"""
Eres un asistente financiero experto. Responde esta pregunta basándote ÚNICAMENTE en los datos proporcionados.

PREGUNTA: "{pregunta}"

DATOS OBTENIDOS:
{datos_json}

INSTRUCCIONES:
1. Responde de manera natural y conversacional en español
2. Sé específico con números y fechas
3. Formatea montos como $1,234.56 (usar comas para miles)
4. Si hay múltiples consultas, integra toda la información coherentemente
5. No menciones "consultas SQL" ni aspectos técnicos
6. Si los datos muestran $0 o valores vacíos, indícalo claramente
7. Usa fechas legibles (ej: "enero 2024" en lugar de "2024-01-01")
8. Sé conciso pero completo
9. Si hay datos de diferentes períodos o categorías, organiza la información claramente

RESPUESTA:
"""
        
        try:
            response = self.llm.invoke([HumanMessage(content=prompt)])
            respuesta = response.content.strip()
            
            # Verificar si la respuesta es muy genérica y mejorarla
            if len(respuesta) < 50 or "no puedo" in respuesta.lower():
                return self._generar_respuesta_fallback(resultados_sql, pregunta)
            
            return respuesta
            
        except Exception as e:
            print(f"❌ Error formateando respuesta: {e}")
            return self._generar_respuesta_fallback(resultados_sql, pregunta)
    
    def _generar_respuesta_fallback(self, resultados_sql: List[Dict], pregunta: str) -> str:
        """Genera una respuesta básica cuando el LLM falla"""
        
        if not resultados_sql:
            return "No se encontraron datos para tu consulta."
        
        total_registros = sum(r.get('total_registros', 0) for r in resultados_sql if isinstance(r.get('total_registros'), int))
        
        if total_registros == 0:
            return "No se encontraron registros para tu consulta."
        
        # Intentar extraer información básica de forma segura
        respuesta_partes = [f"Encontré {total_registros} registros relacionados con tu consulta."]
        
        for resultado in resultados_sql:
            datos = resultado.get('datos', [])
            if isinstance(datos, list) and len(datos) > 0:
                try:
                    primer_registro = datos[0]
                    if isinstance(primer_registro, dict):
                        # Buscar campos de total
                        for key, value in primer_registro.items():
                            if 'total' in key.lower() and value is not None:
                                try:
                                    total = float(value)
                                    respuesta_partes.append(f"El total es ${total:,.2f}")
                                    break
                                except (ValueError, TypeError):
                                    continue
                        
                        # Si no hay total, buscar monto
                        if len(respuesta_partes) == 1 and 'monto' in primer_registro:
                            try:
                                monto = float(primer_registro['monto'] or 0)
                                respuesta_partes.append(f"Se encontró un monto de ${monto:,.2f}")
                            except (ValueError, TypeError):
                                pass
                except Exception as e:
                    print(f"⚠️ Error procesando primer registro: {e}")
                    continue
        
        return " ".join(respuesta_partes)

# Funciones principales
@retry_db_connection(max_retries=3, delay=2)
def guardar_historial(usuario: str, pregunta: str, respuesta: str):
    """Guarda la conversación en el historial"""
    try:
        fecha_actual = datetime.utcnow().isoformat()
        supabase.table('historial_chat').insert({
            'fecha': fecha_actual,
            'usuario': usuario,
            'pregunta': pregunta,
            'respuesta': respuesta
        }).execute()
    except Exception as e:
        print(f"⚠️ Error guardando historial: {e}")

# Endpoint principal
@router.post("/consultar", response_model=Respuesta)
def consultar_datos(request: ConsultaRequest):
    print(f"[Backend] Nueva consulta de {request.usuario}: {request.pregunta}")
    print(f"[Backend] Tabla: {request.tabla_datos}, Año: {request.año}")
    
    try:
        # 1. Validar tabla
        if not validar_nombre_tabla(request.tabla_datos):
            return Respuesta(respuesta="Nombre de tabla no válido")
        
        # 2. Obtener estructura de la tabla
        columnas = obtener_columnas_tabla(request.tabla_datos)
        print(f"[Backend] Columnas disponibles: {columnas}")
        
        # 3. Generar consultas SQL necesarias
        sql_generator = SQLGenerator(OPENAI_API_KEY)
        consultas_sql = sql_generator.generar_consultas_sql(
            request.pregunta, 
            request.tabla_datos, 
            request.año,
            columnas
        )
        
        # 4. Ejecutar todas las consultas
        resultados = ejecutar_consultas_sql(consultas_sql)
        
        # 5. Formatear respuesta natural
        formatter = ResponseFormatter(OPENAI_API_KEY)
        respuesta_final = formatter.formatear_respuesta(request.pregunta, resultados)
        
        # 6. Guardar en historial
        guardar_historial(request.usuario, request.pregunta, respuesta_final)
        
        print(f"[Backend] Respuesta generada: {respuesta_final[:100]}...")
        return Respuesta(respuesta=respuesta_final)
        
    except Exception as e:
        error_msg = f"Error procesando la consulta: {str(e)}"
        print(f"❌ {error_msg}")
        return Respuesta(respuesta="Lo siento, hubo un problema procesando tu consulta. Por favor intenta de nuevo.")

@router.options("/consultar")
async def consultar_options():
    return JSONResponse(content={})

def formatear_respuesta_natural(datos: List[Dict], parametros: Dict, pregunta: str) -> str:
    """Convierte los datos SQL en respuesta natural"""
    
    if not datos:
        return "No se encontraron registros para esa consulta."
    
    # Determinar si es una consulta de total
    es_consulta_total = parametros.get('template') == 'total_gastos_categoria'
    
    # Si es una consulta de total y tenemos el valor, dar una respuesta directa
    if es_consulta_total and datos and 'total' in datos[0]:
        total = float(datos[0]['total'] or 0)
        if total == 0:
            return f"No se registraron gastos en {parametros.get('categoria', 'la categoría')} para el período especificado."
        return f"El gasto total en {parametros.get('categoria', 'la categoría')} fue de ${total:,.2f}"
    
    # Si es una consulta de top gastos, formatear directamente
    if parametros.get('template') == 'top_gastos' and datos:
        if not any(float(gasto.get('monto', 0)) > 0 for gasto in datos):
            return "No se encontraron gastos significativos en el período especificado."
            
        gastos = []
        for gasto in datos:
            try:
                fecha = date_parser.parse(gasto['fecha']).strftime('%d/%m/%Y')
                monto = float(gasto['monto'] or 0)
                if monto > 0:  # Solo incluir gastos mayores a 0
                    descripcion = gasto.get('descripcion', 'Sin descripción')
                    categoria = gasto.get('categoria', 'Sin categoría')
                    gastos.append(f"- {fecha}: ${monto:,.2f} ({descripcion}) - {categoria}")
            except (ValueError, KeyError) as e:
                print(f"⚠️ Error formateando gasto: {e}")
                continue
        
        if not gastos:
            return "No se encontraron gastos significativos en el período especificado."
            
        return f"Las facturas más costosas son:\n" + "\n".join(gastos)
    
    llm = ChatOpenAI(
        openai_api_key=OPENAI_API_KEY,
        model_name="gpt-4o-mini",
        temperature=0
    )
    
    # Limitar datos para el prompt (para evitar tokens excesivos)
    datos_muestra = datos[:20] if len(datos) > 20 else datos
    total_registros = len(datos)
    
    prompt = f"""
    Responde esta consulta financiera de manera clara y profesional en español.

    PREGUNTA: "{pregunta}"
    DATOS OBTENIDOS: {json.dumps(datos_muestra, indent=2, default=str, ensure_ascii=False)}
    TOTAL DE REGISTROS: {total_registros}

    INSTRUCCIONES:
    1. Responde directamente la pregunta
    2. Formatea montos como $1,234.56
    3. Si hay muchos registros, resume los principales
    4. Usa fechas legibles (ej: "mayo 2025")
    5. Sé conciso pero completo
    6. NO menciones código SQL ni técnico
    7. Si el total es 0 o no hay gastos, indícalo claramente

    RESPUESTA:
    """
    
    try:
        response = llm.invoke([HumanMessage(content=prompt)])
        return response.content.strip()
    except Exception as e:
        print(f"❌ Error formateando respuesta: {e}")
        # Si tenemos datos pero el LLM falló, dar una respuesta básica
        if datos:
            # Verificar si todos los valores son 0 o nulos
            if all(float(d.get('monto', 0) or 0) == 0 for d in datos):
                return "No se registraron gastos en el período especificado."
            return f"Se encontraron {total_registros} registros. Los primeros registros son:\n" + \
                   json.dumps(datos_muestra, indent=2, default=str, ensure_ascii=False)
        return f"Se encontraron {total_registros} registros, pero hubo un error al formatear la respuesta."

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
        "limite": 10,
        "año": {año},
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
    - Si pregunta por total/cuanto gastó, usa "total_gastos_categoria"
    - Si pregunta por detalles/listado, usa "gastos_por_periodo"
    - SIEMPRE incluye limite con un número, no null

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
        
        parametros = json.loads(contenido)
        
        # Asegurar que año y limite siempre tengan valores válidos
        parametros['año'] = año
        if parametros.get('limite') is None:
            parametros['limite'] = 10
            
        return parametros
        
    except Exception as e:
        print(f"❌ Error extrayendo parámetros: {e}")
        # Fallback básico
        return {
            "template": "total_gastos_categoria",
            "categoria": "combustible",
            "fecha_inicio": f"{año}-01-01",
            "fecha_fin": f"{año}-12-31",
            "termino_busqueda": None,
            "limite": 10,
            "año": año,
            "tipo_consulta": "suma"
        }