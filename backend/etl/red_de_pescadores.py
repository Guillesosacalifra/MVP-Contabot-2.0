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

def aplicar_red_de_pescadores(df_nuevos: pd.DataFrame, historico: pd.DataFrame) -> pd.DataFrame:
    """
    Recorre cada fila de df_nuevos, y si encuentra en el histórico una coincidencia por
    proveedor + item (con similitud), asigna la categoría del histórico y marca como 'por_historial'.
    """
    resultados = []
    for _, row in df_nuevos.iterrows():
        proveedor_n = normalizar_texto(row.get("proveedor", ""))
        descripcion_n = normalizar_texto(row.get("descripcion", ""))
        match = historico[
            (historico["proveedor_norm"] == proveedor_n)
        ].copy()

        match["similitud"] = match["descripcion_norm"].apply(lambda x: SequenceMatcher(None, x, descripcion_n).ratio())
        match = match[match["similitud"] >= 0.9]

        if not match.empty:
            mejor_match = match.sort_values("similitud", ascending=False).iloc[0]
            row["categoria"] = mejor_match["categoria"]
            row["origen"] = "por_historial"
        else:
            row["origen"] = "nueva"

        resultados.append(row)

    return pd.DataFrame(resultados)
