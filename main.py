from typing import Any, Dict
from fastapi import FastAPI, UploadFile

from src.utils import get_df_from_pdf_exam


app = FastAPI()


@app.post("/upload/")
async def create_upload_file(file: UploadFile) -> Dict[str, Any]:
    if not file.filename or not file.filename.endswith(".pdf"):
        raise ValueError("Arquivo inválido! É necessário um arquivo pdf")
    df = get_df_from_pdf_exam(file.file.read())

    return {
        "info": "Tabelas extraídas com sucesso!",
        "tabelas": df.to_dict(orient="records"),
    }
