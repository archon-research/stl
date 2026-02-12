from fastapi import FastAPI

from app.api.v1 import status

app = FastAPI(title="stl-verify")

app.include_router(status.router, prefix="/v1")
