from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

import json
from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .db import get_session
from .models import Report, Server
from .schemas import (
    CollectReq,
    DatabasesOut,
    InstallAgentReq,
    ReportOut,
    ServerCreate,
    ServerOut,
)
from .services.runner import (
    agent_collect,
    agent_list_databases,
    install_agent_via_ssh,
)

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

    # 1) collect from agent
    try:
        report_payload = await agent_collect(
            srv.agent_url,
            databases=payload.databases,
            blocks=payload.blocks,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"agent error: {e}")

    # 2) persist report
    rep = Report(
        server_id=server_id,
        created_at=datetime.utcnow(),
        databases=payload.databases,
        blocks=payload.blocks,
        report=report_payload,
    )
    session.add(rep)
    await session.commit()
    await session.refresh(rep)

    # 3) return for UI
    return ReportOut(
        id=rep.id,
        server_id=server_id,
        created_at=rep.created_at,
        databases=rep.databases,
        blocks=rep.blocks,
        payload=rep.report,
        # если в schemas ReportOut всё ещё требует report — добавь строку ниже:
        # report=rep.report,
    )

@router.get("/servers/{server_id}/reports/latest", response_model=ReportOut)
async def latest_report(server_id: str, session: AsyncSession = Depends(get_session)):
    q = (
        Report.__table__
        .select()
        .where(Report.server_id == server_id)
        .order_by(Report.created_at.desc())
        .limit(1)
    )
    rows = (await session.execute(q)).fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail="no reports yet")
    rep: Report = rows[0][0]

    return ReportOut(
        id=rep.id,
        server_id=server_id,
        created_at=rep.created_at,
        databases=rep.databases,
        blocks=rep.blocks,
        payload=rep.report,  # fixed: removed stray comma
    )


# -----------------------
# Report view helpers (for UI)
# -----------------------

def _mk_target(action: Dict[str, Any]) -> str:
    t = action.get("type") or "UNKNOWN"
    if t == "ALTER_SYSTEM":
        setting = action.get("setting") or ""
        return f"setting:{setting}" if setting else "setting"
    schema = action.get("schema") or ""
    table = action.get("table") or ""
    column = action.get("column") or ""
    if schema and table and column:
        return f"{schema}.{table}({column})"
    if schema and table:
        return f"{schema}.{table}"
    return action.get("target") or t


@router.get("/reports/{report_id}/actions")
async def report_actions(report_id: str, session: AsyncSession = Depends(get_session)):
    """
    UI expects:
      GET /api/reports/{id}/actions -> ReportAction[]
    We derive actions from agent payload:
      payload.actions (preferred) OR payload.blocks[].actions (fallback)
    """
    rep: Report | None = await session.get(Report, report_id)
    if not rep:
        raise HTTPException(status_code=404, detail="report not found")

    payload = rep.report or {}

    actions = payload.get("actions")
    if not actions:
        # fallback: scan blocks
        blocks = payload.get("blocks") or []
        if isinstance(blocks, dict):
            blocks = [blocks]
        actions = []
        for b in blocks:
            if isinstance(b, dict):
                actions.extend(b.get("actions") or [])

    out: List[Dict[str, Any]] = []
    for i, a in enumerate(actions or []):
        if not isinstance(a, dict):
            continue
        out.append(
            {
                "id": f"{report_id}:{i}",
                "report_id": report_id,
                "type": a.get("type", "NOOP"),
                "target": _mk_target(a),
                "risk": a.get("risk", "low"),
                "reason": a.get("reason", ""),
                "raw": a,
            }
        )
    return out


@router.get("/reports/{report_id}/download")
async def download_report(report_id: str, session: AsyncSession = Depends(get_session)):
    """
    UI expects:
      GET /api/reports/{id}/download -> attachment JSON
    """
    rep: Report | None = await session.get(Report, report_id)
    if not rep:
        raise HTTPException(status_code=404, detail="report not found")

    content = json.dumps(rep.report or {}, ensure_ascii=False, indent=2).encode("utf-8")
    return Response(
        content,
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="report_{report_id}.json"'},
    )
