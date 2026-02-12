import os
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession
from .db import engine, SessionLocal, Base
from .api import router, get_session as get_session_dep

app = FastAPI(title="pgAI server")

async def get_session() -> AsyncSession:
    async with SessionLocal() as session:
        yield session

# подменяем dependency
app.dependency_overrides[get_session_dep] = get_session

@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

app.include_router(router, prefix="/api")
