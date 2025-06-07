from backend.utils import obtener_rango_de_fechas_por_mes, MESES_ES
from backend.etl.xml_parser import limpiar_xmls_en_carpeta, parsear_xmls_en_carpeta
from backend.etl.clasificador import clasificar_items_por_lotes, clasificar_lote, dividir_en_bloques
from backend.etl.supabase_client import subir_dataframe
from backend.etl.exportadores import exportar_json_mes_desde_supabase, exportar_xls_dgi_a_json
from backend.etl.comparacion_dgi import comparar_datalogic_vs_dgi, procesar_comparacion_dgi
from backend.config import get_db_path, get_datalogic_credentials, get_carpeta_descarga, get_carpeta_procesados
from backend.etl.datalogic_downloader import descargar_xml_cfe, descargar_y_descomprimir
from backend.etl.supabase_client import obtener_historico
from backend.etl.red_de_pescadores import normalizar_texto, aplicar_red_de_pescadores

import pandas as pd
import os
from tqdm import tqdm
###################################################################################################
''' 
descargo los datos de datalogic, los limpio, los parseo, y separo los que puedo clasificar con la
red de pescadores y los que debo clasificar con IA
'''

def probar_red_de_pescadores():
    
    carpeta = get_carpeta_descarga()
    creds = get_datalogic_credentials()
       
    mes, anio, empresa = descargar_y_descomprimir(carpeta, creds)
    tabla_nombre = f"{empresa}_{anio}"

    limpiar_xmls_en_carpeta(carpeta)

    df_nuevos = parsear_xmls_en_carpeta("data/datalogic/archivos-XML")
    
    historico = obtener_historico(años=[2025])
    
    # Clasificar con red de pescadores
    df_verificados, df_no_verificados = aplicar_red_de_pescadores(df_nuevos, historico)

    # Clasificar con IA los no verificados
    if not df_no_verificados.empty:
        print(f"🤖 Clasificando {len(df_no_verificados)} ítems nuevos con IA...")
        resultados = []
        for lote in tqdm(dividir_en_bloques(df_no_verificados.to_dict(orient="records"), 100)):
            resultados += clasificar_lote(lote)

        df_clasificacion = pd.DataFrame(resultados)

        # Merge con sufijos controlados para evitar categoria_x/y
        df_no_verificados = df_no_verificados.merge(
            df_clasificacion, on="rowid", how="left", suffixes=("", "_clasificada")
        )

        # Usar la categoría clasificada si está presente
        if "categoria_clasificada" in df_no_verificados.columns:
            df_no_verificados["categoria"] = df_no_verificados["categoria_clasificada"].fillna(df_no_verificados["categoria"])
            df_no_verificados.drop(columns=["categoria_clasificada"], inplace=True)

    # Unir los verificados y los no verificados ya clasificados con IA y subir
    df_final = pd.concat([df_verificados, df_no_verificados], ignore_index=True)
    df_final["fecha"] = pd.to_datetime(df_final["fecha"], errors="coerce")

    subir_dataframe(df_final, tabla_nombre)

    print("🎉 Proceso completo.")
    return
###################################################################################################
''' 
descargo los datos de datalogic, los limpio, los parseo, los clasifico y los subo a supabase 
'''
def ejecutar_pipeline_completo_para_mes():

    carpeta = get_carpeta_descarga()
    creds = get_datalogic_credentials()
       
    mes, anio, empresa = descargar_y_descomprimir(carpeta, creds)
    tabla_nombre = f"{empresa}_{anio}"

    limpiar_xmls_en_carpeta(carpeta)

    registros = parsear_xmls_en_carpeta(carpeta)

    print(f"🤖 Clasificando {len(registros)} ítems nuevos con IA...")
    resultados = []
    for lote in tqdm(dividir_en_bloques(registros.to_dict(orient="records"), 100)):
        resultados += clasificar_lote(lote)

    df_clasificacion = pd.DataFrame(resultados)
    df_final = registros.merge(df_clasificacion, on="rowid", how="left")

    print(f"✅ Total ítems clasificados: {len(df_final)}")
    print(df_final[["fecha", "proveedor", "descripcion", "monto_item", "categoria"]].head())

    subir_dataframe(df_final, tabla_nombre)
    return    

###################################################################################################
'''
exporto los datos de datalogic y los comparo con los de la DGI 
'''
def ejecutar_pipeline_comparacion():
    mes, anio, fecha_desde, fecha_hasta = obtener_rango_de_fechas_por_mes()
    print(f"🔍 Comparando datos desde {fecha_desde} hasta {fecha_hasta}")
    

    # 1. Exportar desde Supabase
    mes_lower = mes.lower()
    json_datalogic = f"data/datalogic/{mes_lower}_{anio}.json"
    exportar_json_mes_desde_supabase(mes, anio)

    # 2. Buscar archivo XLS en carpeta crudo
    archivos_crudos = os.listdir("data/dgi/crudo")
    archivo_xls = next(
        (f for f in archivos_crudos if f.lower().endswith(".xls") and f"Periodo-{anio}_" in f and f"_{MESES_ES[mes_lower]}_" in f),
        None
    )
    if not archivo_xls:
        raise FileNotFoundError(f"❌ No se encontró XLS de DGI en crudo para {mes_lower} {anio}")
    
    path_xls = f"data/dgi/crudo/{archivo_xls}"
    exportar_xls_dgi_a_json(path_xls)

    # 3. Comparar y subir resultado
    comparar_datalogic_vs_dgi(mes_lower, anio)

    return

def ejecutar_pipeline(mes: str, anio: int, empresa: str):
    """
    Ejecuta el pipeline completo de procesamiento de datos.
    
    Args:
        mes (str): Mes a procesar (ej: "enero", "febrero", etc.)
        anio (int): Año a procesar
        empresa (str): Nombre de la empresa para las tablas
    """
    print(f"🚀 Iniciando pipeline para {mes}/{anio} - Empresa: {empresa}")
    
    # Descargar XMLs de CFE
    creds = get_datalogic_credentials()
    descargar_xml_cfe(creds, mes, anio)
    
    # Procesar XMLs
    tabla_nombre = f"{empresa}_{anio}"
    procesar_xmls(mes, anio, tabla_nombre, empresa)
    
    # Comparar con DGI
    comparar_datalogic_vs_dgi(mes, anio, empresa)

def procesar_xmls(mes: str, anio: int, tabla_nombre: str, empresa: str):
    """
    Procesa los XMLs descargados y los sube a Supabase.
    """
    print(f"📦 Procesando XMLs para {mes}/{anio}")
    
    # Descargar y descomprimir archivos
    creds = get_datalogic_credentials()
    descargar_y_descomprimir(creds, mes, anio)
    
    # Procesar archivos
    df = procesar_archivos(mes, anio)
    
    # Subir a Supabase
    subir_dataframe(df, tabla_nombre, empresa)