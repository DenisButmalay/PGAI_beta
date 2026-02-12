import os
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Literal, Optional

from collector import (
    collect_once_and_analyze,
    list_databases,
)

app = FastAPI(title="pgAI agent")

class RunReq(BaseModel):
    # ["all"] или ["db1","db2"]
    databases: List[str] = ["all"]

    # какие блоки собирать (под UI чекбоксы)
    # если пусто/не задано — соберём всё
    blocks: Optional[List[str]] = None

    mode: Literal["recommendation", "execute"] = "recommendation"

@app.get("/databases")
async def databases():
    try:
        return {"databases": await list_databases()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/run")
async def run(req: RunReq):
    try:
        plan = await collect_once_and_analyze(
            databases=req.databases,
            blocks=req.blocks,
            mode=req.mode,
        )
        return plan
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    port = int(os.environ.get("AGENT_PORT", "8010"))
    uvicorn.run(app, host="0.0.0.0", port=port)
