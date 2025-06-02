# FastAPI o script principal

import argparse
from backend.pipeline import ejecutar_pipeline_completo_para_mes, ejecutar_pipeline_comparacion, probar_red_de_pescadores
from backend.etl.exportadores import exportar_json_mes_desde_supabase, exportar_xls_dgi_a_json
from backend.etl.comparacion_dgi import comparar_datalogic_vs_dgi
from backend.utils import obtener_rango_de_fechas_por_mes
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.api.actualizar_categoria import router as actualizar_router

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Podés restringir a tu dominio si querés
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Registrar rutas
app.include_router(actualizar_router, prefix="/api")

@app.get("/health")
def health_check():
    return {"status": "ok"}



if __name__ == "__main__":
    
    probar_red_de_pescadores()
