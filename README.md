# MVP-Contabot-2.0

# 🧠 Clasificador de Gastos con GPT + Supabase

Este proyecto automatiza la clasificación de ítems de gasto provenientes de facturas XML o Excel, utilizando **OpenAI GPT**, almacenamiento en **Supabase**, y un frontend en **React/Vite** para visualizar y gestionar los datos.

---

## 📁 Estructura del Proyecto

clasificador_gastos/
├── backend/ # Procesamiento, FastAPI y lógica de negocio
│ ├── etl/ # Scripts de ETL, clasificación, subida a Supabase
│ ├── config.py
│ └── main.py # Pipeline principal
│
├── frontend/ # Interfaz web (React + Tailwind)
│ └── src/
│
├── data/ # Archivos fuente (XML, JSON, XLSX)
│
├── shared/ # Categorías, esquemas y constantes comunes
├── utils/ # Funciones auxiliares (fechas, logs, etc.)
├── tests/ # Tests automatizados
├── .env # Variables de entorno (NO se sube)
├── .env.example # Ejemplo para desarrollo local
├── .gitignore # Archivos ignorados por git
├── requirements.txt # Dependencias para instalación rápida
├── pyproject.toml # Metadata del proyecto (opcional para poetry/uv)
└── Makefile # Atajos útiles de desarrollo


