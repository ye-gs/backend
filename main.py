from typing import Dict
from fastapi import FastAPI, UploadFile

app = FastAPI()


@app.post("/upload/")
async def create_upload_file(file: UploadFile) -> Dict[str, str]:
    return {"filename": file.filename}
