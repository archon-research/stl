from fastapi import FastAPI

from app.api.v1 import allocations, status

app = FastAPI(title="stl-verify")

app.include_router(status.router, prefix="/v1")
app.include_router(allocations.router, prefix="/v1")
