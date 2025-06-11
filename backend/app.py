from fastapi import FastAPI, UploadFile, Form
import shutil
import os
from ttl_to_neo4j_uploader import run_conversion

app = FastAPI()

@app.post("/convert")
async def convert(
    file: UploadFile,
    root_label: str = Form(...),
    neo4j_uri: str = Form(...),
    neo4j_user: str = Form(...),
    neo4j_pass: str = Form(...),
    preview_only: bool = Form(False)
):
    # Save uploaded file temporarily
    temp_dir = "/tmp" if os.name != 'nt' else os.getenv('TEMP', 'C:\\Temp')
    temp_path = os.path.join(temp_dir, file.filename)

    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # Run your conversion logic
    result = run_conversion(
        ttl_path=temp_path,
        root_label=root_label,
        neo4j_uri=neo4j_uri,
        username=neo4j_user,
        password=neo4j_pass,
        preview_only=preview_only
    )

    return result
