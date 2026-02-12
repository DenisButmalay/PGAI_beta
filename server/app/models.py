from __future__ import annotations

import uuid
from datetime import datetime
from typing import List, Any, Dict

from sqlalchemy import String, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base


def gen_id() -> str:
    return str(uuid.uuid4())


class Server(Base):
    __tablename__ = "servers"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_id)
    name: Mapped[str] = mapped_column(String, nullable=False)
    ip: Mapped[str] = mapped_column(String, nullable=False)
    agent_url: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False, default="unknown")
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


class Report(Base):
    __tablename__ = "reports"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_id)
    server_id: Mapped[str] = mapped_column(String, ForeignKey("servers.id"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    # сохраняем выбор и результат в JSONB (простое и универсальное)
    databases: Mapped[List[str]] = mapped_column(JSONB, nullable=False, default=list)
    blocks: Mapped[List[str]] = mapped_column(JSONB, nullable=False, default=list)
    report: Mapped[Dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
