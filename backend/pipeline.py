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
from backend.etl.xml_parser import parsear_xmls_en_carpeta
from backend.etl.supabase_client import obtener_historico
from backend.etl.red_de_pescadores import normalizar_texto, aplicar_red_de_pescadores

def probar_red_de_pescadores():
    
    carpeta = get_carpeta_descarga()
    creds = get_datalogic_credentials()
       
    mes, anio = descargar_y_descomprimir(carpeta, creds)

    print("🧼 Limpiando XMLs...")
    limpiar_xmls_en_carpeta(carpeta)

    print("📂 Cargando XMLs desde carpeta local...")
    registros = parsear_xmls_en_carpeta("data/datalogic/archivos-XML")
    if not registros:
        print("❌ No se encontraron XMLs.")
        return

    df_nuevos = pd.DataFrame(registros)
    df_nuevos["rowid"] = df_nuevos.index + 1

    print(f"📄 Gastos nuevos detectados: {len(df_nuevos)}")

    print("🧠 Descargando histórico desde Supabase...")
    historico = obtener_historico(años=[2025])
    if historico.empty:
        print("⚠️ Histórico vacío, no se aplicará la red.")
        return

    historico["proveedor_norm"] = historico["proveedor"].apply(normalizar_texto)
    historico["descripcion_norm"] = historico["descripcion"].apply(normalizar_texto)

    print("🎣 Aplicando red de pescadores...")
    df_resultado = aplicar_red_de_pescadores(df_nuevos, historico)

    check_verde = df_resultado[df_resultado["origen"] == "por_historial"]
    check_amarillo = df_resultado[df_resultado["origen"] == "nueva"]

    print(f"✅ Categorizados automáticamente por historial: {len(check_verde)}")
    print(f"🟨 Nuevos a clasificar por IA: {len(check_amarillo)}")

    # Visualizar primeros casos
    print("\n🧾 Ejemplos categorizados:")
    print(check_verde[["proveedor", "descripcion", "categoria"]].head(5))

    print("\n❓ Ejemplos nuevos (sin categoría):")
    print(check_amarillo[["proveedor", "descripcion"]].head(5))
    df_resultado.to_csv("data/resultados/red_de_pescadores_resultado.csv", index=False)


def ejecutar_pipeline_completo_para_mes():

    carpeta = get_carpeta_descarga()
    creds = get_datalogic_credentials()
       
    mes, anio = descargar_y_descomprimir(carpeta, creds)

    print("🧼 Limpiando XMLs...")
    limpiar_xmls_en_carpeta(carpeta)

    print("📄 Parseando XMLs...")
    registros = parsear_xmls_en_carpeta(carpeta)

    if not registros:
        print("❌ No se encontraron registros.")
        return

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

    print("⬆️ Subiendo a Supabase...")
    subir_dataframe(df_final)

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

