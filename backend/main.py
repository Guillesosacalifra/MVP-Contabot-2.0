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

# Configure CORS for your frontend domain
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://www.contia.dev"],  # Your frontend domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routes
app.include_router(actualizar_router, prefix="/api")

@app.get("/health")
def health_check():
    return {"status": "ok"}

# Keep the existing code for local development
if __name__ == "__main__":
    ejecutar_pipeline_completo_para_mes()
    # probar_red_de_pescadores()
