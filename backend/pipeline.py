from backend.utils import obtener_rango_de_fechas_por_mes, MESES_ES
from backend.etl.xml_parser import limpiar_xmls_en_carpeta, parsear_xmls_en_carpeta
from backend.etl.data_processor import clasificar_items_por_lotes, clasificar_lote, dividir_en_bloques
from backend.etl.supabase_client import subir_dataframe
from backend.etl.exportadores import exportar_json_mes_desde_supabase, exportar_xls_dgi_a_json
from backend.etl.comparacion_dgi import comparar_datalogic_vs_dgi, procesar_comparacion_dgi
from backend.config import get_db_path, get_datalogic_credentials, get_carpeta_descarga, get_carpeta_procesados
from backend.etl.datalogic_downloader import descargar_xml_cfe, descargar_y_descomprimir
import pandas as pd
import os
from backend.etl.supabase_client import obtener_historico
from backend.etl.red_de_pescadores import normalizar_texto, aplicar_red_de_pescadores

''' descargo los datos de datalogic, los limpio, los parseo, y separo los que puedo clasificar con la red de pescadores y los que debo clasificar con IA '''
def probar_red_de_pescadores():
    
    carpeta = get_carpeta_descarga()
    creds = get_datalogic_credentials()
       
    mes, anio = descargar_y_descomprimir(carpeta, creds)

    limpiar_xmls_en_carpeta(carpeta)

    df_nuevos = parsear_xmls_en_carpeta("data/datalogic/archivos-XML")
    
    historico = obtener_historico(años=[2025])
    
    aplicar_red_de_pescadores(df_nuevos, historico)

''' descargo los datos de datalogic, los limpio, los parseo, los clasifico y los subo a supabase '''
def ejecutar_pipeline_completo_para_mes():

    carpeta = get_carpeta_descarga()
    creds = get_datalogic_credentials()
       
    mes, anio = descargar_y_descomprimir(carpeta, creds)

    print("🧼 Limpiando XMLs...")
    limpiar_xmls_en_carpeta(carpeta)

    print("📄 Parseando XMLs...")
    registros = parsear_xmls_en_carpeta(carpeta)

    for i, r in enumerate(registros):
        r["rowid"] = i + 1

    df_items = pd.DataFrame(registros)

    print("🤖 Clasificando ítems...")
    resultados = []
    for lote in dividir_en_bloques(registros, 100):
        resultados += clasificar_lote(lote)

    df_clasificacion = pd.DataFrame(resultados)
    df_final = df_items.merge(df_clasificacion, on="rowid", how="left")

    print(f"✅ Total ítems clasificados: {len(df_final)}")
    print(df_final[["fecha", "proveedor", "descripcion", "monto_item", "categoria"]].head())

    subir_dataframe(df_final)

''' exporto los datos de datalogic y los comparo con los de la DGI '''
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

