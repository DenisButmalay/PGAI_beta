from __future__ import annotations

from datetime import datetime
from typing import List, Dict, Any

import httpx
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from .db import get_session
from .models import Server, Report
from .schemas import (
    ServerCreate, ServerOut,
    InstallAgentReq,
    DatabasesOut,
    CollectReq,
    ReportOut,
)
from sqlalchemy import select
from .models import Server
from .services.runner import install_agent_via_ssh  # если нет — ниже скажу
from .services.runner import agent_list_databases, agent_collect  # если нет — ниже скажу

router = APIRouter(tags=["default"])


# -----------------------
# Servers
# -----------------------

@router.get("/servers", response_model=List[ServerOut])
async def list_servers(session: AsyncSession = Depends(get_session)):
    result = await session.execute(select(Server).order_by(Server.created_at.desc()))
    servers = result.scalars().all()
    return [ServerOut.model_validate(s) for s in servers]

@router.post("/servers", response_model=ServerOut)
async def create_server(payload: ServerCreate, session: AsyncSession = Depends(get_session)):
    srv = Server(
        name=payload.name,
        ip=payload.ip,
        agent_url=payload.resolved_agent_url(),
        status="unknown",
        created_at=datetime.utcnow(),
    )
    session.add(srv)
    await session.commit()
    await session.refresh(srv)
    return ServerOut.model_validate(srv)


@router.post("/servers/{server_id}/install-agent", response_model=ServerOut)
async def install_agent(server_id: str, payload: InstallAgentReq, session: AsyncSession = Depends(get_session)):
    srv: Server | None = await session.get(Server, server_id)
    if not srv:
        raise HTTPException(status_code=404, detail="server not found")

    # Попытка установки агента через SSH
    try:
        agent_url = await install_agent_via_ssh(server_ip=srv.ip, req=payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ssh install failed: {e}")

    srv.agent_url = agent_url
    srv.status = "ok"
    await session.commit()
    await session.refresh(srv)
    return ServerOut.model_validate(srv)


# -----------------------
# Databases
# -----------------------

@router.get("/servers/{server_id}/databases", response_model=DatabasesOut)
async def get_server_databases(server_id: str, session: AsyncSession = Depends(get_session)):
    srv: Server | None = await session.get(Server, server_id)
    if not srv:
        raise HTTPException(status_code=404, detail="server not found")

    try:
        dbs = await agent_list_databases(srv.agent_url)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"agent error: {e}")

    return DatabasesOut(databases=dbs)


# -----------------------
# Collect + report
# -----------------------

@router.post("/servers/{server_id}/collect", response_model=ReportOut)
async def collect_server(server_id: str, payload: CollectReq, session: AsyncSession = Depends(get_session)):
    srv: Server | None = await session.get(Server, server_id)
    if not srv:
        raise HTTPException(status_code=404, detail="server not found")

    try:
        report = await agent_collect(
            srv.agent_url,
            databases=payload.databases,
            blocks=payload.blocks,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"agent error: {e}")

    rep = Report(
        server_id=server_id,
        created_at=datetime.utcnow(),
        databases=payload.databases,
        blocks=payload.blocks,
        report=report,
    )
    session.add(rep)
    await session.commit()
    await session.refresh(rep)

    return ReportOut(
        server_id=server_id,
        created_at=rep.created_at,
        databases=rep.databases,
        blocks=rep.blocks,
        report=rep.report,
    )


@router.get("/servers/{server_id}/reports/latest", response_model=ReportOut)
async def latest_report(server_id: str, session: AsyncSession = Depends(get_session)):
    # самый свежий report по server_id
    q = Report.__table__.select().where(Report.server_id == server_id).order_by(Report.created_at.desc()).limit(1)
    rows = (await session.execute(q)).fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail="no reports yet")
    rep: Report = rows[0][0]

    return ReportOut(
        server_id=server_id,
        created_at=rep.created_at,
        databases=rep.databases,
        blocks=rep.blocks,
        report=rep.report,
    )
