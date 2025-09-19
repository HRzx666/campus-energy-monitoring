from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import threading

from .mqtt_handler import start_mqtt
from .db import get_connection
from .ml_forecast import forecast as ml_forecast  # ✅ 引入预测函数

# ---- 新增：时区支持 ----
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:  # 兜底（极少用到）
    ZoneInfo = None

TAIPEI_TZ = ZoneInfo("Asia/Taipei") if ZoneInfo else None

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup_event():
    threading.Thread(target=start_mqtt, daemon=True).start()


@app.get("/", response_class=HTMLResponse)
def index():
    html_path = Path(__file__).parent.parent / "frontend" / "index.html"
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"), status_code=200)


# ---------- 映射表 ----------
TABLE_FIELD_MAP = {
    "electric": ("electric_data", "power_kw"),
    "water": ("water_data", "flow_lpm"),
    "gas": ("gas_data", "flow_m3h"),
}


# ---------- 工具函数 ----------
def _parse_client_datetime(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip()
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        try:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f")

    if dt.tzinfo is not None:
        if TAIPEI_TZ:
            dt = dt.astimezone(TAIPEI_TZ).replace(tzinfo=None)
        else:
            dt = (dt.astimezone()).replace(tzinfo=None) + timedelta(hours=8)
    return dt


def _fmt_to_local_iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


# ---------- API ----------
@app.get("/api/latest/{type}")
def latest_data(type: str) -> List[Dict[str, Any]]:
    if type not in TABLE_FIELD_MAP:
        return []
    table, field = TABLE_FIELD_MAP[type]
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT timestamp, {field} FROM {table} ORDER BY timestamp DESC LIMIT 20"
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    rows = list(reversed(rows))
    return [{"timestamp": _fmt_to_local_iso(ts), "value": float(val)} for ts, val in rows]


@app.get("/api/history/{type}")
def history_data(
    type: str,
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    if type not in TABLE_FIELD_MAP:
        return []

    table, field = TABLE_FIELD_MAP[type]
    end_dt = _parse_client_datetime(end) if end else datetime.now()
    start_dt = _parse_client_datetime(start) if start else end_dt - timedelta(days=1)

    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT timestamp, {field}
            FROM {table}
            WHERE timestamp BETWEEN %s AND %s
            ORDER BY timestamp ASC
            """,
            (start_dt, end_dt),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    return [{"timestamp": _fmt_to_local_iso(r[0]), "value": float(r[1])} for r in rows]


# ---------- 预测 API ----------
@app.get("/api/forecast/{type}")
def forecast_api(
    type: str,
    horizon_hours: int = Query(24, description="预测时长（小时）"),
    freq: str = Query("H", description="步长：H 或 15min"),
    lookback_days: int = Query(30, description="训练使用最近天数"),
    mode: str = Query("series", description="预测模式：series=逐点预测; total=仅返回总量"),
):
    try:
        return ml_forecast(
            type,
            horizon_hours=horizon_hours,
            freq=freq,
            lookback_days=lookback_days,
            mode=mode,
        )
    except Exception as e:
        return {"error": str(e)}


# ---------- 新增：30天总用量 API ----------
@app.get("/api/summary/{type}")
def summary_data(type: str):
    """
    返回最近30天每天的总用量（给柱状图用）
    """
    if type not in TABLE_FIELD_MAP:
        return {"error": "invalid type"}

    table, field = TABLE_FIELD_MAP[type]
    conn = get_connection()
    try:
        cur = conn.cursor()
        if type == "electric":
            sql = """
                SELECT DATE(timestamp), SUM(power_kw/30) -- 假设每条记录是2分钟 (30条=1小时)
                FROM electric_data
                WHERE timestamp >= NOW() - INTERVAL 30 DAY
                GROUP BY DATE(timestamp)
                ORDER BY DATE(timestamp)
            """
        elif type == "water":
            sql = """
                SELECT DATE(timestamp), SUM(flow_lpm*60/1000)
                FROM water_data
                WHERE timestamp >= NOW() - INTERVAL 30 DAY
                GROUP BY DATE(timestamp)
                ORDER BY DATE(timestamp)
            """
        elif type == "gas":
            sql = """
                SELECT DATE(timestamp), SUM(flow_m3h)
                FROM gas_data
                WHERE timestamp >= NOW() - INTERVAL 30 DAY
                GROUP BY DATE(timestamp)
                ORDER BY DATE(timestamp)
            """
        cur.execute(sql)
        rows = cur.fetchall()
    finally:
        conn.close()

    return {"metric": type, "history": [{"date": str(r[0]), "value": float(r[1])} for r in rows]}
