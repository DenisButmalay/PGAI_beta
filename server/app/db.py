from __future__ import annotations

import os
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

# Один источник правды для URL (по умолчанию можно оставить любой, какой тебе нужен)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:postgres@db:5432/pgai",
)


class Base(DeclarativeBase):
    pass


def make_engine(url: str | None = None) -> AsyncEngine:
    """Создаёт AsyncEngine для SQLAlchemy."""
    return create_async_engine(
        url or DATABASE_URL,
        echo=False,
        pool_pre_ping=True,
    )


def make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Создаёт фабрику async-сессий."""
    return async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )


# (опционально) готовые объекты, если где-то захочешь импортировать напрямую
engine: AsyncEngine = make_engine(DATABASE_URL)
SessionLocal: async_sessionmaker[AsyncSession] = make_session_factory(engine)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency: выдаёт AsyncSession."""
    async with SessionLocal() as session:
        yield session
