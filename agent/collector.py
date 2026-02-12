# collector.py
# Полноценный сборщик метрик и рекомендаций от LLM (agent side)
# - Конфиг через ENV (PG_DSN, OPENAI_API_KEY, MODEL, MAX_STATEMENTS)
# - Выбор баз (all или список)
# - Выбор блоков метрик
# - Гарантированный JSON-ответ от LLM
# - Нормализация типов для JSON

import os
import json
import psutil
import asyncpg
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any, List, Optional
import ipaddress

from openai import OpenAI

# ===== ENV Config =====
PG_DSN = os.environ["PG_DSN"]  # postgresql://user:pass@host:5432/postgres
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
MODEL = os.environ.get("MODEL", "gpt-4o-mini")
MAX_STATEMENTS = int(os.environ.get("MAX_STATEMENTS", "50"))
# ======================

client = OpenAI(api_key=OPENAI_API_KEY)

# UI/Server will pass blocks from this set:
ALL_BLOCKS = [
    "system",
    "buffers_bgwriter",
    "wal_replication",
    "temp_files",
    "checkpoints_bgwriter",
    "sizes",
    "connections_activity",
    "indexes_tables_statements",
]


def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def normalize(obj):
    """Рекурсивно преобразует проблемные типы в сериализуемые значения."""
    if isinstance(obj, dict):
        return {k: normalize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [normalize(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, bytes):
        return obj.hex()
    if isinstance(obj, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
        return str(obj)
    if isinstance(obj, set):
        return list(obj)
    if obj is None:
        return None
    try:
        json.dumps(obj)
        return obj
    except Exception:
        return str(obj)


async def fetch_rows(conn, sql: str) -> List[Dict[str, Any]]:
    """Безопасный fetch: не падаем, а возвращаем ошибку в данных."""
    try:
        rows = await conn.fetch(sql)
        return [normalize(dict(r)) for r in rows]
    except Exception as e:
        return [{"error": str(e), "sql": sql}]


# -------- Системные метрики --------
def collect_system_metrics() -> Dict[str, Any]:
    cpu_pct = psutil.cpu_percent(interval=1)
    cpu_times = psutil.cpu_times_percent()
    mem = psutil.virtual_memory()
    swap = psutil.swap_memory()
    cpu_stats = psutil.cpu_stats()
    disk_io = psutil.disk_io_counters()
    root_fs = psutil.disk_usage("/")
    return {
        "timestamp": now_iso(),
        "cpu": {
            "utilization_pct": cpu_pct,
            "iowait_pct": getattr(cpu_times, "iowait", None),
            "context_switches": getattr(cpu_stats, "ctx_switches", None),
        },
        "memory": {
            "free_bytes": mem.available,
            "swap_used_bytes": swap.used,
            "os_page_cache_bytes": getattr(mem, "cached", None),
        },
        "disk": {
            "iops_read": getattr(disk_io, "read_count", None),
            "iops_write": getattr(disk_io, "write_count", None),
            "latency_read_ms": getattr(disk_io, "read_time", None),
            "latency_write_ms": getattr(disk_io, "write_time", None),
            "throughput_read_bytes": getattr(disk_io, "read_bytes", None),
            "throughput_write_bytes": getattr(disk_io, "write_bytes", None),
            "filesystem_root_used_pct": root_fs.percent,
        },
    }


# -------- DB list --------
async def list_databases() -> List[str]:
    """Список доступных БД (исключая template)."""
    conn = await asyncpg.connect(PG_DSN)
    try:
        rows = await conn.fetch(
            """
            SELECT datname
            FROM pg_database
            WHERE datallowconn
              AND datname NOT IN ('template0','template1')
            ORDER BY datname;
            """
        )
        return [r["datname"] for r in rows]
    finally:
        await conn.close()


def _dsn_with_db(dsn: str, dbname: str) -> str:
    """Грубая, но практичная подмена БД в DSN вида postgresql://.../db."""
    base = dsn.rsplit("/", 1)[0]
    return f"{base}/{dbname}"


# -------- Блоки метрик PostgreSQL --------
async def collect_pg_buffers_bgwriter(conn) -> Dict[str, Any]:
    bgwriter = await fetch_rows(conn, "SELECT * FROM pg_stat_bgwriter;")
    dbstats = await fetch_rows(
        conn,
        """
        SELECT datid, datname, blks_read, blks_hit, tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted,
               temp_files, temp_bytes
        FROM pg_stat_database;
        """,
    )
    return {"timestamp": now_iso(), "bgwriter": bgwriter, "database_stats": dbstats}


async def collect_pg_wal_replication(conn) -> Dict[str, Any]:
    # pg_stat_wal есть начиная с PG14; если нет — fetch_rows вернёт error в данных
    stat_wal = await fetch_rows(conn, "SELECT * FROM pg_stat_wal;")
    replication = await fetch_rows(conn, "SELECT * FROM pg_stat_replication;")
    wal_receiver = await fetch_rows(conn, "SELECT * FROM pg_stat_wal_receiver;")
    archiver = await fetch_rows(conn, "SELECT * FROM pg_stat_archiver;")
    return {
        "timestamp": now_iso(),
        "stat_wal": stat_wal,
        "replication": replication,
        "wal_receiver": wal_receiver,
        "archiver": archiver,
    }


async def collect_pg_temp_files(conn) -> Dict[str, Any]:
    db_temp = await fetch_rows(
        conn,
        """
        SELECT datname, temp_files, temp_bytes
        FROM pg_stat_database;
        """,
    )
    statements = await fetch_rows(
        conn,
        f"""
        SELECT queryid, calls, rows, total_exec_time, mean_exec_time,
               shared_blks_hit, shared_blks_read, local_blks_read, temp_blks_read, temp_blks_written, query
        FROM pg_stat_statements
        ORDER BY total_exec_time DESC
        LIMIT {MAX_STATEMENTS};
        """,
    )
    return {"timestamp": now_iso(), "database_temp": db_temp, "heavy_statements": statements}


async def collect_pg_checkpoints_bgwriter(conn) -> Dict[str, Any]:
    bgwriter = await fetch_rows(conn, "SELECT * FROM pg_stat_bgwriter;")
    return {"timestamp": now_iso(), "bgwriter": bgwriter}


async def collect_pg_sizes(conn) -> Dict[str, Any]:
    databases = await fetch_rows(
        conn,
        """
        SELECT d.oid as dbid, d.datname, pg_database_size(d.datname) AS database_size
        FROM pg_database d;
        """,
    )
    relsizes = await fetch_rows(
        conn,
        """
        SELECT
          n.nspname AS schema,
          c.relname AS relation,
          c.relkind,
          pg_total_relation_size(c.oid) AS total_size,
          pg_relation_size(c.oid) AS main_size,
          pg_indexes_size(c.oid) AS index_size,
          pg_total_relation_size(c.oid) - pg_relation_size(c.oid) - pg_indexes_size(c.oid) AS toast_size
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname NOT IN ('pg_catalog','information_schema')
          AND c.relkind IN ('r','m')
        ORDER BY total_size DESC
        LIMIT 200;
        """,
    )
    return {"timestamp": now_iso(), "databases": databases, "relations": relsizes}


async def collect_pg_connections_activity(conn) -> Dict[str, Any]:
    activity = await fetch_rows(
        conn,
        """
        SELECT datname, pid, usename, application_name, client_addr, backend_start, state,
               wait_event_type, wait_event, query
        FROM pg_stat_activity;
        """,
    )
    states = await fetch_rows(
        conn,
        """
        SELECT datname, state, COUNT(*) AS cnt
        FROM pg_stat_activity
        GROUP BY datname, state
        ORDER BY cnt DESC;
        """,
    )
    return {"timestamp": now_iso(), "activity": activity, "states": states}


async def collect_pg_indexes_tables_statements(conn) -> Dict[str, Any]:
    tables = await fetch_rows(
        conn,
        """
        SELECT n.nspname AS schema, c.relname AS table, s.seq_scan, s.idx_scan, s.n_live_tup, s.n_dead_tup,
               s.last_vacuum, s.last_autovacuum, s.last_analyze, s.last_autoanalyze
        FROM pg_stat_user_tables s
        JOIN pg_class c ON c.oid = s.relid
        JOIN pg_namespace n ON n.oid = c.relnamespace;
        """,
    )
    user_indexes = await fetch_rows(
        conn,
        """
        SELECT s.relname AS table, i.indexrelname AS index, i.idx_scan, i.idx_tup_read, i.idx_tup_fetch
        FROM pg_stat_user_indexes i
        JOIN pg_stat_user_tables s ON s.relid = i.relid
        ORDER BY i.idx_scan ASC;
        """,
    )
    statements = await fetch_rows(
        conn,
        f"""
        SELECT queryid, calls, rows, total_exec_time, mean_exec_time,
               shared_blks_hit, shared_blks_read, local_blks_read, temp_blks_read, temp_blks_written, query
        FROM pg_stat_statements
        ORDER BY calls DESC
        LIMIT {MAX_STATEMENTS};
        """,
    )
    return {"timestamp": now_iso(), "tables": tables, "indexes": user_indexes, "statements": statements}


# -------- Анализ блока через LLM --------
async def analyze_block(block_name: str, metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Формат ответа строго JSON:
    {
      "block": "<block_name>",
      "actions": [ ... ],
      "notes": [ ... ]
    }
    """
    prompt = f"""
Ты — DBA-ассистент PostgreSQL. Проанализируй блок метрик "{block_name}" и предложи улучшения.
Учитывай проблемы: индексы, vacuum/analyze, work_mem/temp files, checkpoints/bgwriter,
WAL/репликация, соединения, размеры, I/O. Отвечай строго JSON (без текста вне JSON).
Структура:
{{
  "block": "{block_name}",
  "actions": [
    {{
      "type": "CREATE_INDEX" | "VACUUM" | "ANALYZE" | "ALTER_SYSTEM" | "REINDEX" | "KILL_BACKEND" | "NOOP",
      "schema": "public",
      "table": "orders",
      "column": "customer_id",
      "indexname": "idx_orders_customer_id",
      "method": "btree",
      "where": "status = 'active'",
      "setting": "work_mem",
      "value": "64MB",
      "reason": "почему это нужно",
      "risk": "low|medium|high"
    }}
  ],
  "notes": ["краткие наблюдения"]
}}
Если улучшений нет, верни actions: [] и краткие notes.
"""

    response = client.chat.completions.create(
        model=MODEL,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": "Ты DBA-ассистент PostgreSQL. Отвечай строго в JSON."},
            {"role": "user", "content": prompt},
            {"role": "user", "content": json.dumps(metrics, ensure_ascii=False, indent=2)},
        ],
    )

    content = response.choices[0].message.content or "{}"
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        data = {"block": block_name, "actions": [], "notes": ["LLM ответ не JSON"]}

    data.setdefault("block", block_name)
    data.setdefault("actions", [])
    data.setdefault("notes", [])
    return data


# -------- Главная функция (вызывается агентом) --------
async def collect_once_and_analyze(
    databases: List[str] = ["all"],
    blocks: Optional[List[str]] = None,
    mode: str = "recommendation",
) -> Dict[str, Any]:
    blocks = blocks or ALL_BLOCKS
    want = set(blocks)

    # databases: ["all"] -> реально разворачиваем в список баз
    if "all" in databases:
        dbs = await list_databases()
    else:
        dbs = databases

    collected: List[tuple[str, Dict[str, Any]]] = []

    # 1) system (локально)
    if "system" in want:
        collected.append(("system", collect_system_metrics()))

    # 2) базовые PG блоки (достаточно соединения к одной базе, обычно postgres)
    # NOTE: если PG_DSN указывает не на postgres — это не критично для этих блоков.
    base_conn = await asyncpg.connect(PG_DSN)
    try:
        if "buffers_bgwriter" in want:
            collected.append(("buffers_bgwriter", await collect_pg_buffers_bgwriter(base_conn)))

        if "wal_replication" in want:
            collected.append(("wal_replication", await collect_pg_wal_replication(base_conn)))

        if "checkpoints_bgwriter" in want:
            collected.append(("checkpoints_bgwriter", await collect_pg_checkpoints_bgwriter(base_conn)))

        if "sizes" in want:
            collected.append(("sizes", await collect_pg_sizes(base_conn)))

        if "connections_activity" in want:
            collected.append(("connections_activity", await collect_pg_connections_activity(base_conn)))
    finally:
        await base_conn.close()

    # 3) DB-aware блоки (внутри каждой базы: pg_stat_user_tables, pg_stat_statements и т.п.)
    if "temp_files" in want:
        per_db_temp: List[Dict[str, Any]] = []
        for db in dbs:
            dsn = _dsn_with_db(PG_DSN, db)
            conn = await asyncpg.connect(dsn)
            try:
                d = await collect_pg_temp_files(conn)
                d["database"] = db
                per_db_temp.append(d)
            finally:
                await conn.close()
        collected.append(("temp_files", {"timestamp": now_iso(), "per_database": per_db_temp}))

    if "indexes_tables_statements" in want:
        per_db_ix: List[Dict[str, Any]] = []
        for db in dbs:
            dsn = _dsn_with_db(PG_DSN, db)
            conn = await asyncpg.connect(dsn)
            try:
                d = await collect_pg_indexes_tables_statements(conn)
                d["database"] = db
                per_db_ix.append(d)
            finally:
                await conn.close()
        collected.append(("indexes_tables_statements", {"timestamp": now_iso(), "per_database": per_db_ix}))

    # 4) Анализ блоков через LLM (не валим весь план, если один блок не анализируется)
    analyzed_blocks: List[Dict[str, Any]] = []
    for name, data in collected:
        try:
            analyzed_blocks.append(await analyze_block(name, data))
        except Exception as e:
            analyzed_blocks.append({"block": name, "actions": [], "notes": [f"analyze_block failed: {e}"]})

    # 5) Сводный план
    actions: List[Dict[str, Any]] = []
    notes: List[str] = []
    for blk in analyzed_blocks:
        actions.extend(blk.get("actions", []) or [])
        for n in blk.get("notes", []) or []:
            notes.append(f"[{blk.get('block','?')}] {n}")

    plan = {
        "timestamp": now_iso(),
        "mode": mode,
        "selected": {"databases": dbs, "blocks": list(want)},
        "actions": actions,
        "notes": notes,
        "blocks": analyzed_blocks,
    }

    return normalize(plan)
