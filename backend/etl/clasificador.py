# etl/clasificador.py

import json
import time
from typing import List, Dict
from openai import OpenAIError
from openai import OpenAI
from tqdm import tqdm
import re

# Inicialización del cliente OpenAI desde entorno
from dotenv import load_dotenv
import os
load_dotenv()

openai_api_key = os.getenv("OPENAI_API_KEY")
openai = OpenAI(api_key=openai_api_key)


def dividir_en_bloques(lista: List[dict], n: int) -> List[List[dict]]:
    """
    Divide una lista de ítems en bloques de tamaño n.
    """
    return [lista[i:i + n] for i in range(0, len(lista), n)]
def clasificar_items_por_lotes(items: List[dict], lote: int = 100) -> List[str]:
    """
    Clasifica los ítems usando OpenAI en lotes.

    Args:
        items: Lista de ítems (dicts con descripción, monto, etc).
        lote: Tamaño del bloque a enviar por consulta.

    Returns:
        Lista de dicts con 'rowid' y categorías clasificadas (una por ítem).
    """
    resultados = []
    bloques = dividir_en_bloques(items, lote)

    for i, bloque in enumerate(tqdm(bloques, desc="🤖 Clasificando")):
        try:
            prompt = generar_prompt_clasificacion(bloque)
            respuesta = openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "Tu siguiente output devuelve solamente el formato JSON. Sin texto adicional"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1
            )
            categorias = extraer_categorias_de_respuesta(respuesta.choices[0].message.content)
            for item, categoria in zip(bloque, categorias):
                resultados.append({
                    "rowid": item["rowid"],
                    "categoria": categoria
                })

        except Exception as e:
            print(f"❌ Error en lote {i}: {e}")
            for item in bloque:
                resultados.append({
                    "rowid": item["rowid"],
                    "categoria": "error"
                })
        time.sleep(1.5)

    return resultados
def generar_prompt_clasificacion(items: List[dict]) -> str:
    """
    Genera el prompt a enviar a OpenAI con los ítems.
    """
    texto = "Clasificá los siguientes ítems de gasto. Solo devolvé una lista de categorías, sin repetir ni explicar:\n"
    for item in items:
        descripcion = item.get("descripcion", "")
        texto += f"- {descripcion}\n"
    return texto
def extraer_categorias_de_respuesta(texto: str) -> List[str]:
    """
    Procesa la respuesta de OpenAI y devuelve las categorías como lista.
    """
    lineas = texto.strip().split("\n")
    categorias = [linea.strip("- ").strip() for linea in lineas if linea.strip()]
    return categorias
def limpiar_output_de_chatgpt(texto: str) -> str:
    """
    Limpia caracteres fuera del JSON que puedan estar antes o después.
    """
    match = re.search(r'\[.*\]', texto, re.DOTALL)
    return match.group(0) if match else texto
def intentar_parsear_json(texto: str) -> List[Dict]:
    """
    Intenta parsear el texto como JSON. Si falla, lanza excepción.
    """
    return json.loads(texto)

def clasificar_lote(lote_datos: List[Dict]) -> List[Dict]:
    """
    Clasifica un lote de ítems usando categorías personalizadas.

    Args:
        lote_datos: Lista de ítems con campos como 'rowid', 'descripcion', etc.

    Returns:
        Lista de dicts con 'rowid' y 'categoria'.
    """
    system_prompt = "Tu siguiente output devuelve solamente el formato JSON. Sin texto adicional"

    user_prompt = f"""
Dado el siguiente listado de ítems con sus datos, clasifícalos en una de las siguientes categorías:
A continuación listo las categorias seguido por / y palabras clave sobre cada una de la siguiente forma: categoria / palabras clave.

Dividendos fictos / extremadamente poco frecuente\n
Comisiones tarjetas / OCA S.A., PASS CARD; \n
Gastos por deudas incobrables / \n
Telepeaje / CVU, Corporación vial del uruguay; \n
Flete costo de mercaderías / DAC, encomiendas, bersal group \n
Gastos Varios / último recurso, cuando no sepas donde clasificar usa esta categoría \n
Uniformes / camisa, zapatos, casco;\n
Patente vehículos / \n
Sueldos y Jornales / liquidación, aguinaldo; \n
Gastos de importación / Carlos Piaggio, Carlos A. Piaggio Zibechi; \n
Viáticos / viáticos\n
Adelanto de sueldos / adelanto de sueldos\n
Salario vacacional / licencia; \n
Cargas Sociales / BPS, Banco previsión social; \n
Seguros / BSE, banco seguro estado, mapfre; \n
Papelería / tijera, papel, cuaderno, cuadernola, lapiz, lápices, colores\n
Combustible / nafta, super 95, gasoil, gas, oil, ancap, paraje, marimar; \n
Gastos varios compartidos / poco frecuente\n
Mantenimiento Vehículos / tireshop, roda, accesorios, ruedas, neumáticos, aceite; \n
Alquiler de vehículos / poco frecuente \n
Mantenimiento Local / relacionado a arreglos domésticos \n
Mantenimiento de equipos / luces, servicio técnico, computadora, cpu, disco, memoria, ram, acondicionado; \n
Honorarios Profesionales / estudio contable asociados asesoramiento legal; \n
Servicios Contratados / zeta software punta traking acqua life; \n
Energía Eléctrica y Aguas Corrientes / UTE, U.T.E., Administración Nacional de Usinas y Transmisiones Eléctricas, ADMINISTRACION DE LAS OBRAS SANITARIAS DEL ESTADO, OSE; \n
Comunicaciones y Servicios Telefónicos / ADMINISTRACION NACIONAL DE TELECOMUNICACIONES, ANTEL, ETHERNET, DEDICADO, NETGATE, CLARO, MOVISTAR \n
Alquileres / alquiler maldonado, alquiler melo; \n
Publicidad / radio melo fm, televisión, la voz, canal, pautas; \n
Representación / expo, agro, prado, rural; \n
Comisiones por ventas / comisiones \n
Costos de Servicios / abitab; \n
Intereses y Gastos Bancarios / préstamo, diferencia, cargo, tasa; \n
Diferencias de Cambio perdidas / poco frecuente\n
Retiro socios / ana, diego; \n
Pérdida por diferencia de efectivo / poco frecuente\n
Costos de ventas/ cervinia, barraca, ferreteria, servicios en acero, materiales de construcción, herramientas, consumidor final; \n

Aclaraciones:
- si no estas seguro de tu respuesta, busca detenidamente palabras clave en el campo descripcion o proveedor
- Devuélvelo como una lista JSON donde cada objeto tenga 'rowid' y 'categoria'.

Ejemplo de output:
[
  {{"rowid": 1, "categoria": "Combustible"}},
  ...
]

Datos:
{json.dumps(lote_datos, ensure_ascii=False, indent=2)}
"""
    respuesta = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0
    )
    try:
        output = respuesta.choices[0].message.content
        output = limpiar_output_de_chatgpt(output)
        resultado = intentar_parsear_json(output)
        return resultado
    except json.JSONDecodeError:
        print("❌ Error: la respuesta de GPT no es JSON válido.")
        raise
