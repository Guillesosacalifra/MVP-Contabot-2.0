# FastAPI o script principal

import argparse
from backend.pipeline import ejecutar_pipeline_completo_para_mes, ejecutar_pipeline_comparacion, probar_red_de_pescadores
from backend.etl.exportadores import exportar_json_mes_desde_supabase, exportar_xls_dgi_a_json
from backend.etl.comparacion_dgi import comparar_datalogic_vs_dgi
from backend.utils import obtener_rango_de_fechas_por_mes
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from backend.api.actualizar_categoria import router as actualizar_router
from backend.api.chatbot import router as chatbot_router
import logging
import time
from datetime import datetime
import os
import sys

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.StreamHandler(sys.stdout)
#     ]
# )
# logger = logging.getLogger(__name__)

# # Log startup
# logger.info("Starting application...")
# logger.info(f"Python version: {sys.version}")
# logger.info(f"Current working directory: {os.getcwd()}")

# app = FastAPI()

# # Configure CORS for your frontend domain
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=[
#         "https://www.contia.dev",
#         "https://contia.dev",
#         "https://*.lovableproject.com",
#         "https://mvp-contabot-2-0.onrender.com",
#         "http://localhost:3000",
#         "http://localhost:5173",
#         "http://localhost:8080"
#     ],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # Add logging middleware
# @app.middleware("http")
# async def log_requests(request: Request, call_next):
#     start_time = time.time()
#     try:
#         response = await call_next(request)
#         duration = time.time() - start_time
#         logger.info(f"{request.method} {request.url.path} - Status: {response.status_code} - Duration: {duration:.2f}s")
#         return response
#     except Exception as e:
#         logger.error(f"Error processing request: {str(e)}")
#         logger.exception("Full traceback:")
#         return JSONResponse(
#             status_code=500,
#             content={
#                 "status": "error",
#                 "message": f"Internal server error: {str(e)}",
#                 "timestamp": datetime.utcnow().isoformat()
#             }
#         )

# # Exception handler
# @app.exception_handler(RequestValidationError)
# async def validation_exception_handler(request: Request, exc: RequestValidationError):
#     logger.error(f"Validation error: {exc.errors()}")
#     return JSONResponse(
#         status_code=422,
#         content={"detail": exc.errors()}
#     )

# # Register routes
# app.include_router(actualizar_router, prefix="/api")
# app.include_router(chatbot_router, prefix="/api")

# @app.get("/")
# async def root():
#     logger.info("Root endpoint called")
#     return {"status": "starting", "message": "Application is starting up"}

# @app.get("/health")
# async def health_check():
#     logger.info("Health check called")
#     try:
#         # Verificar variables de entorno críticas
#         env_vars = {
#             "SUPABASE_URI": os.getenv("SUPABASE_URI"),
#             "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY"),
#             "SUPABASE_URL": os.getenv("SUPABASE_URL"),
#             "SUPABASE_API_KEY": os.getenv("SUPABASE_API_KEY")
#         }
        
#         # Log variables (sin mostrar valores sensibles)
#         for key in env_vars:
#             logger.info(f"{key} is {'set' if env_vars[key] else 'not set'}")
#             if not env_vars[key]:
#                 logger.error(f"Missing environment variable: {key}")
        
#         missing_vars = [k for k, v in env_vars.items() if not v]
#         if missing_vars:
#             error_msg = f"Missing environment variables: {', '.join(missing_vars)}"
#             logger.error(error_msg)
#             return JSONResponse(
#                 status_code=500,
#                 content={
#                     "status": "error",
#                     "message": error_msg,
#                     "timestamp": datetime.utcnow().isoformat()
#                 }
#             )

#         # Intentar conexión a la base de datos
#         try:
#             from sqlalchemy import create_engine, text
#             logger.info("Attempting database connection...")
#             db_url = os.getenv("SUPABASE_URI")
#             if not db_url:
#                 raise ValueError("SUPABASE_URI is not set")
            
#             logger.info(f"Database URL format: {'postgresql://' in db_url}")
#             engine = create_engine(db_url)
            
#             with engine.connect() as conn:
#                 logger.info("Executing test query...")
#                 result = conn.execute(text("SELECT 1"))
#                 logger.info(f"Query result: {result.scalar()}")
            
#             logger.info("Database connection successful")
#             db_status = "connected"
#         except Exception as e:
#             error_msg = f"Database connection error: {str(e)}"
#             logger.error(error_msg)
#             logger.error(f"Database URL format: {'postgresql://' in os.getenv('SUPABASE_URI', '')}")
#             db_status = f"error: {str(e)}"
#             return JSONResponse(
#                 status_code=500,
#                 content={
#                     "status": "error",
#                     "message": error_msg,
#                     "database_status": db_status,
#                     "timestamp": datetime.utcnow().isoformat()
#                 }
#             )

#         return {
#             "status": "ok",
#             "version": "1.0.0",
#             "message": "Backend funcionando correctamente",
#             "database": db_status,
#             "timestamp": datetime.utcnow().isoformat()
#         }
#     except Exception as e:
#         error_msg = f"Health check error: {str(e)}"
#         logger.error(error_msg)
#         logger.exception("Full traceback:")
#         return JSONResponse(
#             status_code=500,
#             content={
#                 "status": "error",
#                 "message": error_msg,
#                 "timestamp": datetime.utcnow().isoformat()
#             }
#         )

# Keep the existing code for local development
if __name__ == "__main__":
    # ejecutar_pipeline_completo_para_mes()
    # probar_red_de_pescadores()
    ejecutar_pipeline_comparacion()