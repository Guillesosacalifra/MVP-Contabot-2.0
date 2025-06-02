# etl/xml_parser.py
import os
import zipfile
import tempfile
import shutil
import xml.etree.ElementTree as ET
import re
from tqdm import tqdm
from typing import List, Dict

def descomprimir_archivos_zip_en(carpeta_destino: str) -> None:
    """
    Busca archivos .zip en la carpeta destino, los descomprime en una carpeta temporal,
    y mueve los archivos extraídos al mismo nivel que los ZIP originales. 
    Luego elimina los archivos ZIP.
    """
    zip_encontrados = False

    for archivo in os.listdir(carpeta_destino):
        if archivo.lower().endswith(".zip"):
            zip_encontrados = True
            ruta_zip = os.path.join(carpeta_destino, archivo)
            try:
                with tempfile.TemporaryDirectory() as carpeta_temp:
                    with zipfile.ZipFile(ruta_zip, 'r') as zip_ref:
                        zip_ref.extractall(carpeta_temp)

                    for raiz, _, archivos in os.walk(carpeta_temp):
                        for f in archivos:
                            origen = os.path.join(raiz, f)
                            destino = os.path.join(carpeta_destino, f)
                            shutil.move(origen, destino)

                os.remove(ruta_zip)
                print(f"✅ Descomprimido y eliminado: {archivo}")
            except Exception as e:
                print(f"❌ Error al descomprimir {archivo}: {e}")

    if not zip_encontrados:
        print("ℹ️ No se encontraron archivos .zip para descomprimir.")

def limpiar_xmls_en_carpeta(carpeta_descargas: str) -> None:
    """
    Limpia todos los XML en la carpeta: elimina caracteres especiales y encapsula correctamente el contenido.
    """
    for nombre_archivo in tqdm(os.listdir(carpeta_descargas), desc="🔧 Limpiando XMLs"):
        if nombre_archivo.lower().endswith(".xml"):
            ruta_original = os.path.join(carpeta_descargas, nombre_archivo)

            with open(ruta_original, "r", encoding="utf-8") as f:
                contenido = f.read().strip()

            contenido = contenido.replace("\ufeff", "")

            if "<Adenda>" in contenido:
                match_cfe = re.search(r"<([a-zA-Z0-9:]*CFE)(\s[^>]*)?>.*?</\1>", contenido, re.DOTALL)
                contenido_limpio = match_cfe.group(0) if match_cfe else contenido
            else:
                contenido_limpio = f"<FacturaCompleta>\n{contenido}\n</FacturaCompleta>"

            with open(ruta_original, "w", encoding="utf-8") as f:
                f.write(contenido_limpio)

def parsear_xmls_en_carpeta(carpeta_descargas: str) -> List[Dict]:
    """
    Parsea los archivos XML en la carpeta, extrayendo ítems de facturas con metadatos completos.
    Devuelve una lista de diccionarios listos para subir a Supabase.
    Elimina los archivos XML ya parseados
    """
    ns = {"dgicfe": "http://cfe.dgi.gub.uy"}
    registros = []
    archivos_xml = [f for f in os.listdir(carpeta_descargas) if f.lower().endswith(".xml")]

    if not archivos_xml:
        print("⚠️ No se encontraron archivos XML.")
        return []

    for archivo in archivos_xml:
        ruta = os.path.join(carpeta_descargas, archivo)
        try:
            tree = ET.parse(ruta)
            root = tree.getroot()

            # Datos del emisor
            fecha = root.findtext(".//dgicfe:FchEmis", "", namespaces=ns)
            proveedor = root.findtext(".//dgicfe:RznSoc", "", namespaces=ns)
            ruc = root.findtext(".//dgicfe:RUCEmisor", "", namespaces=ns)
            nombre_comercial = root.findtext(".//dgicfe:NomComercial", "", namespaces=ns)
            giro = root.findtext(".//dgicfe:GiroEmis", "", namespaces=ns)
            telefono = root.findtext(".//dgicfe:Telefono", "", namespaces=ns)
            sucursal = root.findtext(".//dgicfe:EmiSucursal", "", namespaces=ns)
            codigo_sucursal = root.findtext(".//dgicfe:CdgDGISucur", "", namespaces=ns)
            direccion = root.findtext(".//dgicfe:DomFiscal", "", namespaces=ns)
            ciudad = root.findtext(".//dgicfe:Ciudad", "", namespaces=ns)
            departamento = root.findtext(".//dgicfe:Departamento", "", namespaces=ns)

            tipo_moneda = root.findtext(".//dgicfe:TpoMoneda", "UYU", namespaces=ns)
            tipo_cambio_str = root.findtext(".//dgicfe:TpoCambio", "1", namespaces=ns)
            tipo_cambio = float(tipo_cambio_str) if tipo_cambio_str else 1.0

            for item in root.findall(".//dgicfe:Item", namespaces=ns):
                descripcion = item.findtext("dgicfe:NomItem", "", namespaces=ns)
                cantidad = float(item.findtext("dgicfe:Cantidad", "1", namespaces=ns))
                precio_unitario = float(item.findtext("dgicfe:PrecioUnitario", "0", namespaces=ns))
                monto_item = float(item.findtext("dgicfe:MontoItem", "0", namespaces=ns))
                monto_uyu = monto_item * tipo_cambio if tipo_moneda != "UYU" else monto_item

                registros.append({
                    "fecha": fecha,
                    "proveedor": proveedor,
                    "ruc": ruc,
                    "nombre_comercial": nombre_comercial,
                    "giro": giro,
                    "telefono": telefono,
                    "sucursal": sucursal,
                    "codigo_sucursal": codigo_sucursal,
                    "direccion": direccion,
                    "ciudad": ciudad,
                    "departamento": departamento,
                    "descripcion": descripcion,
                    "cantidad": cantidad,
                    "precio_unitario": precio_unitario,
                    "monto_item": monto_item,
                    "moneda": tipo_moneda,
                    "tipo_cambio": tipo_cambio,
                    "monto_uyu": monto_uyu,
                    "archivo": archivo,
                })


            # ✅ Eliminar solo si se parseó correctamente
            os.remove(ruta)

        except Exception as e:
            print(f"❌ Error procesando {archivo}: {e}")

    print(f"✅ {len(registros)} ítems extraídos desde {len(archivos_xml)} archivos XML.")
    return registros

