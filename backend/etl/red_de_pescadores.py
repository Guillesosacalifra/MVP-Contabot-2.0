import pandas as pd
import re
from difflib import SequenceMatcher

def normalizar_texto(texto: str) -> str:
    """
    Normaliza el texto para mejorar la comparación:
    - Convierte a minúsculas
    - Elimina tildes y caracteres especiales
    - Elimina espacios redundantes
    """
    if not isinstance(texto, str):
        return ''
    texto = texto.lower()
    texto = re.sub(r'[áàäâ]', 'a', texto)
    texto = re.sub(r'[éèëê]', 'e', texto)
    texto = re.sub(r'[íìïî]', 'i', texto)
    texto = re.sub(r'[óòöô]', 'o', texto)
    texto = re.sub(r'[úùüû]', 'u', texto)
    texto = re.sub(r'[^a-z0-9\s]', '', texto)
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto

def es_similar(a: str, b: str, umbral: float = 0.9) -> bool:
    """
    Compara dos textos normalizados y devuelve True si son suficientemente similares.
    """
    a_norm = normalizar_texto(a)
    b_norm = normalizar_texto(b)
    return SequenceMatcher(None, a_norm, b_norm).ratio() >= umbral

def preparar_historico_para_red(df_historico: pd.DataFrame) -> pd.DataFrame:
    """
    Prepara el histórico: filtra solo verificados y normaliza campos necesarios.
    Si el DataFrame está vacío o no tiene la columna verificado, devuelve un DataFrame vacío.
    """
    if df_historico.empty:
        print("⚠️ El histórico está vacío")
        return pd.DataFrame(columns=["proveedor", "descripcion", "categoria", "proveedor_norm", "descripcion_norm"])

    df = df_historico.copy()
    
    # Si no existe la columna verificado, asumimos que todos los registros están verificados
    if "verificado" not in df.columns:
        print("⚠️ No se encontró columna 'verificado' en el histórico, asumiendo todos verificados")
        df["verificado"] = True
    else:
        df = df[df["verificado"] == True]

    if df.empty:
        print("⚠️ No hay registros verificados en el histórico")
        return pd.DataFrame(columns=["proveedor", "descripcion", "categoria", "proveedor_norm", "descripcion_norm"])

    df["proveedor_norm"] = df["proveedor"].apply(normalizar_texto)
    df["descripcion_norm"] = df["descripcion"].apply(normalizar_texto)

    return df


def aplicar_red_de_pescadores(df_nuevos: pd.DataFrame, historico_completo: pd.DataFrame) -> pd.DataFrame:
    """
    Asigna categorías desde el histórico a nuevos registros si son suficientemente similares.
    Los categorizados automáticamente se marcan como verificado = True.
    """
    print("🎣 Aplicando red de pescadores...")

    historico = preparar_historico_para_red(historico_completo)
    resultados = []

    for _, row in df_nuevos.iterrows():
        proveedor_n = normalizar_texto(row.get("proveedor", ""))
        descripcion_n = normalizar_texto(row.get("descripcion", ""))

        posibles = historico[historico["proveedor_norm"] == proveedor_n].copy()
        posibles["similitud"] = posibles["descripcion_norm"].apply(
            lambda x: SequenceMatcher(None, x, descripcion_n).ratio()
        )
        posibles = posibles[posibles["similitud"] >= 0.70]

        if not posibles.empty:
            mejor = posibles.sort_values("similitud", ascending=False).iloc[0]
            row["categoria"] = mejor["categoria"]
            # row["origen"] = "por_historial"
            row["verificado"] = True
        else:
            # row["origen"] = "nueva"
            row["verificado"] = False

        resultados.append(row)

    df_resultado = pd.DataFrame(resultados)

    df_verificados = df_resultado[df_resultado["verificado"] == True].copy()
    df_no_verificados = df_resultado[df_resultado["verificado"] == False].copy()

    print(f"✅ Reconocidos y categorizados automáticamente por historial: {len(df_verificados)}")
    print(f"🟨 Nuevos a clasificar por modelo IA: {len(df_no_verificados)}")

    # print("\n🧾 Ejemplos categorizados:")
    # print(df_verificados[["proveedor", "descripcion", "categoria"]].head(10))
    # print("\n❓ Ejemplos nuevos (sin categoría):")
    # print(df_no_verificados[["proveedor", "descripcion"]].head(10))

    df_resultado.to_csv("data/resultados/red_de_pescadores_resultado.csv", index=False)

    return df_verificados, df_no_verificados

