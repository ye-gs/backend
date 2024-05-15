from typing import Any, Dict
from pdfquery import PDFQuery
from fastapi import FastAPI, UploadFile

app = FastAPI()


@app.post("/upload/")
async def create_upload_file(file: UploadFile) -> Dict[str, Any]:
    if not file.filename or not file.filename.endswith(".pdf"):
        raise ValueError("Arquivo inválido! É necessário um arquivo pdf")
    pdf = PDFQuery(file.file)
    pdf.load()
    textos = "\n".join(pdf.tree.xpath("//text()"))
    with open(file.filename.replace(".pdf", ".txt"), "wb") as converted_file:
        converted_file.write(textos.encode())
    return {"info": "Arquivo convertido com sucesso!"}
