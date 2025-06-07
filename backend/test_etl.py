from backend.etl.xml_parser import limpiar_xmls_en_carpeta, parsear_xmls_en_carpeta
from backend.etl.clasificador import dividir_en_bloques, clasificar_lote
from backend.etl.supabase_client import subir_dataframe
from backend.config import get_directories
import pandas as pd

def test_pipeline_completo():
    """
    Ejecuta el flujo completo:
    1. Limpia XMLs
    2. Parsea ítems con metadata
    3. Clasifica usando GPT
    4. Une clasificación
    5. Sube a Supabase
    """
    carpeta_descarga, _ = get_directories()

    print("🧼 Limpiando XMLs...")
    limpiar_xmls_en_carpeta(carpeta_descarga)

    print("📄 Parseando XMLs...")
    registros = parsear_xmls_en_carpeta(carpeta_descarga)
    if not registros:
        print("❌ No se encontraron registros.")
        return

    for i, r in enumerate(registros):
        r["rowid"] = i + 1

    print("🤖 Clasificando ítems...")
    clasificados = []
    for bloque in dividir_en_bloques(registros, 100):
        clasificados.extend(clasificar_lote(bloque))

    df_items = pd.DataFrame(registros)
    df_clasificacion = pd.DataFrame(clasificados)
    df_final = df_items.merge(df_clasificacion, on="rowid", how="left")

    print(f"✅ Total ítems clasificados: {len(df_final)}")
    print(df_final[["fecha", "proveedor", "descripcion", "monto_item", "categoria"]].head())

    print("⬆️ Subiendo a Supabase...")
    subir_dataframe(df_final)

if __name__ == "__main__":
    test_pipeline_completo()
