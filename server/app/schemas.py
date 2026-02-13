from __future__ import annotations

from datetime import datetime
from typing import Optional, Literal, List, Dict, Any
from pydantic import BaseModel, Field


class ServerCreate(BaseModel):
    name: str
    ip: str
    agent_url: Optional[str] = None

    def resolved_agent_url(self) -> str:
        return self.agent_url or f"http://{self.ip}:8010"


class ServerOut(BaseModel):
    id: str
    name: str
    ip: str
    agent_url: str
    status: str
    created_at: datetime

    class Config:
        from_attributes = True


class DatabasesOut(BaseModel):
    databases: List[str]


class CollectReq(BaseModel):
    databases: List[str] = Field(default_factory=lambda: ["all"])
    blocks: List[str] = Field(default_factory=lambda: ["all"])


class ReportOut(BaseModel):
    id: str
    server_id: str
    created_at: datetime
    databases: List[str]
    blocks: List[str]
    payload: Dict[str, Any]
    report: Dict[str, Any] | None = None

    class Config:
        from_attributes = True


# SSH install (оставим на будущее, чтобы UI не ломать)
class SSHAuth(BaseModel):
    type: Literal["password", "private_key"] = "password"
    password: Optional[str] = None
    private_key: Optional[str] = None


class InstallAgentReq(BaseModel):
    ssh_user: str
    ssh_port: int = 22
    ssh_auth: SSHAuth

    pg_host: str = "127.0.0.1"
    pg_port: int = 5432
    pg_user: str = "postgres"
    pg_password: str = "postgres"
    pg_database: str = "postgres"

    agent_port: int = 8010
    model: str = "gpt-4o-mini"
    max_statements: int = 50

    openai_api_key: str = Field(..., min_length=10)
