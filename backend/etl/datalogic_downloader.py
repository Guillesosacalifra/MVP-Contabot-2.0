# etl/datalogic_downloader.py

import os
import time
import glob
import tempfile
import zipfile
import shutil
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from backend.utils import obtener_rango_de_fechas_por_mes
from backend.config import get_carpeta_descarga, get_carpeta_procesados, get_datalogic_credentials
from backend.etl.xml_parser import descomprimir_archivos_zip_en  # definiremos esto luego

def descargar_y_descomprimir(carpeta_base, creds_list):
    """
    Downloads and decompresses XML files for multiple clients.
    
    Args:
        carpeta_base: Base directory where client folders will be created
        creds_list: List of client credentials dictionaries
    """
    mes, anio, fecha_desde, fecha_hasta = obtener_rango_de_fechas_por_mes()
    print(f"📥 Buscando XMLs desde {fecha_desde} hasta {fecha_hasta}...")
    
    for creds in creds_list:
        client_id = creds["client_id"]
        empresa_datalogic = creds["empresa"]
        empresa = input("📆 Ingresá el nombre de la EMPRESA (ej. NIKE): ").strip().lower()
        # Create client-specific folder
        carpeta_cliente = os.path.join(carpeta_base, f"cliente_{client_id}_{empresa_datalogic}")
        os.makedirs(carpeta_cliente, exist_ok=True)
        
        print(f"\n🔄 Procesando cliente {client_id} - {empresa_datalogic}")
        
        try:
            descargar_xml_cfe(
                carpeta_descarga=carpeta_cliente,
                usuario=creds["usuario"],
                contrasena=creds["contrasena"],
                empresa=creds["empresa"],
                url_login=creds["url_login"],
                fecha_desde_str=fecha_desde,
                fecha_hasta_str=fecha_hasta
            )
            
            descomprimir_archivos_zip_en(carpeta_cliente)
            print(f"✅ Cliente {client_id} - {empresa_datalogic} procesado exitosamente")
            
        except Exception as e:
            print(f"❌ Error procesando cliente {client_id} - {empresa_datalogic}: {str(e)}")
            continue
    
    return mes, anio, empresa

def esperar_descarga_completa(carpeta, timeout=60):
    """
    Espera a que aparezca y luego desaparezca un archivo .crdownload
    indicando que la descarga está en progreso y luego finaliza.
    """
    inicio = time.time()
    crdownload_detectado = False

    while (time.time() - inicio) < timeout:
        archivos = os.listdir(carpeta)
        tiene_temp = any(f.endswith(".crdownload") for f in archivos)
        tiene_zip = any(f.endswith(".zip") for f in archivos)

        if tiene_temp:
            crdownload_detectado = True
            # hay algo en curso, esperamos
            time.sleep(1)
        elif crdownload_detectado:
            # ya había empezado y ahora terminó
            return True
        elif tiene_zip:
            # se descargó directamente sin pasar por .crdownload (raro, pero puede)
            return True
        else:
            # aún no arrancó la descarga, esperamos un poco más
            time.sleep(1)

    return False


def descargar_xml_cfe(carpeta_descarga, usuario, contrasena, empresa, url_login, fecha_desde_str, fecha_hasta_str):
    """
    Automatiza el ingreso a Datalogic, descarga comprobantes CFE en ZIP, y los guarda en la carpeta indicada.
    """
    print('\n🔄 Iniciando descarga automática de CFE...\n')

    prefs = {
        "download.default_directory": carpeta_descarga,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
        # "profile.default_content_settings.popups": 0,
        # "profile.default_content_setting_values.automatic_downloads": 1
    }

    options = webdriver.ChromeOptions()
    options.add_experimental_option("prefs", prefs)
    options.add_argument("--headless")  # Mantenelo comentado para debug visual
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    try:
        wait = WebDriverWait(driver, 10)

        # 🔹 Paso: Login
        driver.get(url_login)

        campo_usuario = wait.until(EC.element_to_be_clickable((By.ID, "vUSUARIO")))
        campo_usuario.clear()
        campo_usuario.click()
        campo_usuario.send_keys(usuario)

        campo_contra = wait.until(EC.element_to_be_clickable((By.ID, "vPASSWORD")))
        campo_contra.clear()
        campo_contra.click()
        campo_contra.send_keys(contrasena)

        boton_login = wait.until(EC.element_to_be_clickable((By.ID, "BTNUSUARIOLOGIN_LOGIN")))
        driver.execute_script("gx.evt.execEvt('', false, \"E'USUARIOLOGIN_LOGIN'.\", arguments[0]);", boton_login)

        print("✅ Login exitoso")

        # 🔹 Paso: Selección de empresa
        try:
            # Intentar encontrar el dropdown de empresa
            dropdown = WebDriverWait(driver, 3).until(
                EC.presence_of_element_located((By.ID, "vEMPRESA"))
            )
            
            print("🔹 Se requiere seleccionar empresa.")
            dropdown.click()
            time.sleep(1)

            driver.execute_script(f"""
                const select = arguments[0];
                select.value = '{empresa}';
                select.dispatchEvent(new Event('change', {{ bubbles: true }}));
                select.dispatchEvent(new Event('blur', {{ bubbles: true }}));
            """, dropdown)

            boton_empresa = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.ID, "BTNEMPRESALOGIN_CONTINUAR"))
            )
            boton_empresa.click()
            driver.execute_script("gx.evt.execEvt('', false, \"E'EMPRESALOGIN_CONTINUAR'.\", arguments[0]);", boton_empresa)

            print(f"🚀 Empresa seleccionada: {empresa}")
            time.sleep(0.5)

        except TimeoutException:
            # No apareció el dropdown de empresa: continuar normalmente
            print("ℹ️ No se requiere selección de empresa.")

        # 🔹 Paso: (opcional) Cambio de empresa
        try:
            empresa_menu = wait.until(EC.presence_of_element_located(
                (By.XPATH, f"//ul[@class='dropdown-menu']//a[contains(text(), '{empresa.split()[0]}')]")
            ))
            driver.execute_script("arguments[0].click();", empresa_menu)
            time.sleep(0.5)
            print("🔁 Cambio de empresa ejecutado.")
        except Exception as e:
            print("⚠️ No se mostró el menú de cambio de empresa (puede ser normal).")

        # 🔹 Paso: Menú Facturación
        facturacion_btn = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//p[contains(text(), 'Facturación Electrónica')]/ancestor::button")
        ))
        facturacion_btn.click()

        gfe_opcion = wait.until(EC.presence_of_element_located(
            (By.XPATH, "//a[contains(text(), 'GFE - StandAlone')]")
        ))
        driver.execute_script("arguments[0].click();", gfe_opcion)
        time.sleep(0.5)

        # 🔹 Paso: Consultas
        menu_consultas = wait.until(EC.element_to_be_clickable((By.ID, "OpcMen20000")))
        driver.execute_script("arguments[0].click();", menu_consultas)
        
        link_cfe_recibidos = wait.until(EC.element_to_be_clickable((By.ID, "OpcMen20200")))
        driver.execute_script("arguments[0].click();", link_cfe_recibidos)
        time.sleep(0.5)

        # 🔹 Paso: IFrame y fechas
        wait.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "IFDLPortal")))
        campo_desde = wait.until(EC.presence_of_element_located((By.ID, "vFCHCFERECDESDE")))
        time.sleep(0.2)
        campo_hasta = wait.until(EC.presence_of_element_located((By.ID, "vFCHCFERECHASTA")))

        for campo, valor in [(campo_desde, fecha_desde_str), (campo_hasta, fecha_hasta_str)]:
            campo.click()
            time.sleep(0.4)
            campo.send_keys(Keys.CONTROL + "a")
            campo.send_keys(Keys.DELETE)
            campo.send_keys(valor)

        # 🔹 Paso: Marcar y descargar
        wait.until(EC.element_to_be_clickable((By.ID, "SEARCHBUTTON"))).click()
        time.sleep(4)

        wait.until(EC.element_to_be_clickable((By.ID, "MARCARTODOS"))).click()
        time.sleep(2)

        btn_descargar = wait.until(EC.element_to_be_clickable((By.ID, "GENERARXML")))
        btn_descargar.click()

        print("📥 Esperando descarga...")
        # ESPERAR ZIP
        timeout = 30
        esperados = glob.glob(os.path.join(carpeta_descarga, "*.zip"))
        inicio = time.time()
        while not esperados and (time.time() - inicio) < timeout:
            time.sleep(1)
            esperados = glob.glob(os.path.join(carpeta_descarga, "*.zip"))

        if esperados:
            print(f"✅ ZIP descargado: {os.path.basename(esperados[0])}")
        else:
            print("❌ Tiempo de espera agotado. No se detectó descarga ZIP.")

    except Exception as e:
        print(f"❌ Error durante proceso: {e}")

    finally:
        driver.quit()


