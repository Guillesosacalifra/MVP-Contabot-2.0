from pydantic import BaseModel
from fastapi import APIRouter, HTTPException
from supabase import create_client
from datetime import datetime
import os

router = APIRouter()

class CategoriaEditada(BaseModel):
    id: int
    nueva_categoria: str
    motivo: str
    usuario: str

# Inicializar Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_API_KEY)

@router.post("/actualizar_categoria")
def actualizar_categoria(data: CategoriaEditada):
    try:
        # Obtener la categoría actual antes de actualizar
        res = supabase.table("datalogic_2025").select("categoria").eq("id", data.id).execute()
        if not res.data or len(res.data) == 0:
            raise HTTPException(status_code=404, detail="No se encontró el registro")

        categoria_anterior = res.data[0]["categoria"]

        # Actualizar categoría en datalogic_2025
        supabase.table("datalogic_2025").update({
            "categoria": data.nueva_categoria
        }).eq("id", data.id).execute()

        # Insertar en cambios_en_categorias
        supabase.table("cambios_en_categorias").insert({
            # "id": data.id,
            "fecha_cambio": datetime.now().isoformat(),
            "categoria_anterior": categoria_anterior,
            "categoria_nueva": data.nueva_categoria,
            "motivo": data.motivo,
            "usuario": data.usuario
        }).execute()

        return {"status": "ok", "mensaje": "Categoría actualizada y cambio registrado"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en el servidor: {e}")
