import locale
import calendar
from datetime import datetime
from backend.config import LOCALE_ES, MESES_ES


def obtener_rango_de_fechas_por_mes() -> tuple[str, str]:
    """
    Solicita al usuario un mes en español y devuelve la fecha_desde y fecha_hasta
    en formato dd/mm/aaaa correspondientes al mes completo.
    
    Returns:
        Tuple[str, str]: (fecha_desde_str, fecha_hasta_str)
    """
    locale.setlocale(locale.LC_TIME, LOCALE_ES)

    while True:
        try:
            mes_str = input("🗓️ Ingresá el MES (ej. abril): ").strip().lower()
            año_str = input("📆 Ingresá el AÑO (ej. 2025): ").strip()

            if mes_str not in MESES_ES:
                print("❌ Mes inválido. Usá el nombre completo en español (ej. marzo).")
                continue

            mes_num = MESES_ES[mes_str]
            año = int(año_str)
            _, ultimo_dia = calendar.monthrange(año, mes_num)

            fecha_desde_str = f"01/{mes_num:02d}/{año}"
            fecha_hasta_str = f"{ultimo_dia:02d}/{mes_num:02d}/{año}"

            return mes_str, año, fecha_desde_str, fecha_hasta_str
        except Exception as e:
            print(f"❌ Error: {e}")

def obtener_numero_mes(mes: str) -> int:
    meses = {
        "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
        "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
        "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12
    }
    return meses[mes.lower()]

def obtener_nombre_mes(mes: int) -> str:
    nombres = [
        "enero", "febrero", "marzo", "abril", "mayo", "junio",
        "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"
    ]
    return nombres[mes - 1]