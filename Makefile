# Instalar dependencias
install:
	uv pip install -r requirements.txt

# Crear entorno virtual (opcional)
venv:
	py -m venv .venv

# Paso 1: Descargar XMLs
descargar:
	py backend/scripts/descargar_xmls.py --mes $(MES) --año $(AÑO)

# Paso 2: Limpiar XMLs en carpeta
limpiar:
	py backend/scripts/limpiar_xmls.py --carpeta $(CARPETA)

# Paso 3: Parsear XMLs a ítems
parsear:
	py backend/scripts/parsear_xmls.py --carpeta $(CARPETA)

# Paso 4: Clasificar ítems con GPT
clasificar:
	py backend/scripts/clasificar_items.py --input $(ARCHIVO_JSON) --output $(ARCHIVO_OUT)

# Paso 5: Subir a Supabase
subir:
	py backend/scripts/subir_supabase.py --archivo $(ARCHIVO_OUT) --tabla $(TABLA)

# Paso 6: Ejecutar pipeline completo para un mes
PY := py

pipeline:
	set PYTHONPATH=. && $(PY) backend/main.py

# Ejecutar comparación con DGI
comparacion:
	set PYTHONPATH=. && $(PY) run_comparison.py

