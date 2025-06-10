import pandas as pd
import numpy as np
import os
from datetime import datetime
from backend.etl.supabase_client import subir_dataframe

def procesar_comparacion_dgi(path_json_datalogic: str, path_json_dgi: str) -> pd.DataFrame:
    """
    Lee dos JSON (uno generado desde datalogic, otro desde el XLS de DGI convertido),
    los compara, genera un DataFrame con las diferencias y lo devuelve listo para subir a Supabase.
    """
    df_cfe = pd.read_json(path_json_datalogic)
    df_dgi = pd.read_json(path_json_dgi)

    # Normalización
    df_cfe["ruc"] = df_cfe["ruc"].astype(str).str.strip()
    df_dgi["rut_emisor"] = df_dgi["rut_emisor"].astype(str).str.strip()

    df_cfe["monto_item"] = pd.to_numeric(df_cfe["monto_item"], errors="coerce")
    df_dgi["monto_total"] = pd.to_numeric(df_dgi["monto_total"], errors="coerce")
    df_dgi["monto_neto"] = pd.to_numeric(df_dgi["monto_neto"], errors="coerce")

    df_cfe = df_cfe[df_cfe["monto_item"].notna()]
    df_dgi = df_dgi[df_dgi["monto_total"].notna() | df_dgi["monto_neto"].notna()]

    df_cfe["fecha"] = pd.to_datetime(df_cfe["fecha"], errors="coerce")

    # Agrupamiento
    df_cfe_group = df_cfe.groupby("ruc", as_index=False).agg({
        "monto_item": "sum",
        "fecha": "max"
    }).rename(columns={"monto_item": "suma_datalogic"})

    df_dgi_group = df_dgi.groupby("rut_emisor", as_index=False).agg({
        "monto_total": "sum",
        "monto_neto": "sum"
    }).rename(columns={"rut_emisor": "ruc", "monto_total": "suma_total", "monto_neto": "suma_neto"})

    comparacion = pd.merge(df_cfe_group, df_dgi_group, on="ruc", how="outer").fillna(0)

    # Cálculo de diferencias
    comparacion["dif_total"] = (comparacion["suma_datalogic"] - comparacion["suma_total"]).abs()
    comparacion["dif_neto"] = (comparacion["suma_datalogic"] - comparacion["suma_neto"]).abs()
    comparacion["diferencia"] = comparacion[["dif_total", "dif_neto"]].min(axis=1).round(2)

    tol = 0.01
    comparacion["coincide_total"] = comparacion["dif_total"] <= (tol * comparacion["suma_datalogic"])
    comparacion["coincide_neto"] = comparacion["dif_neto"] <= (tol * comparacion["suma_datalogic"])

    comparacion["resultado"] = np.where(
        comparacion["coincide_total"] | comparacion["coincide_neto"],
        "coincide",
        "difiere"
    )

    comparacion["contempla_iva"] = np.where(comparacion["coincide_total"], "Sí",
                                     np.where(comparacion["coincide_neto"], "No", None))

    comparacion["monto_es_negativo"] = np.where(
        (comparacion["suma_total"] < 0) | (comparacion["suma_neto"] < 0), "Sí", "No"
    )

    def aclaracion(row):
        if row["resultado"] != "difiere":
            return None
        elif row["suma_datalogic"] == 0:
            return "no están en Datalogic"
        else:
            return "distinto monto que Datalogic"

    comparacion["aclaracion"] = comparacion.apply(aclaracion, axis=1)

    return comparacion


def comparar_datalogic_vs_dgi(mes: str, anio: int, empresa: str):
    """
    Compara los datos de Datalogic con los de DGI.
    Lee de la tabla {empresa}_{año} y sube los resultados a DGI_{empresa}_{año}
    """
    print(f"🔍 Comparando datos de {empresa} con DGI para {mes}/{anio}")
    
    # Rutas a los archivos JSON
    path_datalogic = f"data/{empresa}/{mes.lower()}_{anio}.json"
    path_dgi = f"data/dgi/dgi_{mes.lower()}_{anio}.json"

    # Verificación
    if not os.path.exists(path_datalogic):
        raise FileNotFoundError(f"❌ Falta el archivo: {path_datalogic}")
    if not os.path.exists(path_dgi):
        raise FileNotFoundError(f"❌ Falta el archivo: {path_dgi}")

    print(f"🔄 Procesando comparación para {mes.lower()} {anio}...")

    # Procesar
    df = procesar_comparacion_dgi(path_datalogic, path_dgi)
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.strftime("%Y-%m-%d")

    # Subir a la tabla DGI_{empresa}_{año}
    tabla_dgi = f"DGI_{empresa}_{anio}"
    subir_dataframe(df, tabla_nombre=tabla_dgi)

    print(f"✅ Comparación subida correctamente a Supabase en la tabla {tabla_dgi}")

