from backend.etl.supabase_client import supabase

def actualizar_proveedor(ruc: str, nombre_proveedor: str) -> bool:
    """
    Actualiza o inserta un nuevo proveedor en la tabla base_de_rucs.
    
    Args:
        ruc (str): El RUC del proveedor
        nombre_proveedor (str): El nombre del proveedor a guardar
        
    Returns:
        bool: True si la operación fue exitosa, False en caso contrario
    """
    try:
        # Intentar actualizar primero
        result = supabase.table("base_de_rucs") \
            .update({"nombre": nombre_proveedor}) \
            .eq("ruc", ruc) \
            .execute()
            
        # Si no se actualizó ningún registro, insertar uno nuevo
        if len(result.data) == 0:
            result = supabase.table("base_de_rucs") \
                .insert({"ruc": ruc, "nombre": nombre_proveedor}) \
                .execute()
                
        return True
    except Exception as e:
        print(f"❌ Error al actualizar proveedor: {e}")
        return False 