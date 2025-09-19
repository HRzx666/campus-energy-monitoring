from __future__ import annotations
import math
from typing import Dict, Any
import pandas as pd
from prophet import Prophet
from .db import get_connection

# 三种能源的表与字段映射：用“速率”列建模（而不是累计总量）
METRIC_MAP: Dict[str, Dict[str, str]] = {
    "electric": {"table": "electric_data", "value_col": "power_kw", "total_unit": "kWh", "rate_per_hour": 1.0},
    "water":    {"table": "water_data",    "value_col": "flow_lpm", "total_unit": "L",   "rate_per_hour": 60.0},  # L/min * 60 = L/h
    "gas":      {"table": "gas_data",      "value_col": "flow_m3h", "total_unit": "m³",  "rate_per_hour": 1.0},
}

def load_series(metric: str, lookback_days: int = 30) -> pd.DataFrame:
    """从 MySQL 拉近 N 天数据 -> DataFrame[ds,y]"""
    meta = METRIC_MAP.get(metric)
    if not meta:
        raise ValueError("metric 需为 electric/water/gas")
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT timestamp, {meta['value_col']} FROM {meta['table']} "
                f"WHERE timestamp >= NOW() - INTERVAL %s DAY ORDER BY timestamp ASC",
                (lookback_days,),
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    if not rows:
        return pd.DataFrame(columns=["ds", "y"])
    df = pd.DataFrame(rows, columns=["ds", "y"])
    df["ds"] = pd.to_datetime(df["ds"])
    df["y"] = pd.to_numeric(df["y"], errors="coerce")
    df = df.dropna()
    return df

def resample(df: pd.DataFrame, freq: str = "H") -> pd.DataFrame:
    """按等间隔重采样（默认 1 小时），用平均值；小缺口做简单前后填充"""
    if df.empty:
        return df
    s = df.set_index("ds").sort_index()["y"].astype(float)
    y = s.resample(freq).mean()
    y = y.ffill(limit=2).bfill(limit=2).interpolate(limit=2)
    return y.reset_index().rename(columns={"index": "ds"})

def fit_and_predict(df: pd.DataFrame, horizon_hours: int, freq: str):
    """拟合 Prophet 并预测未来"""
    m = Prophet(weekly_seasonality=True, daily_seasonality=True, seasonality_mode="additive")
    m.fit(df)  # 需要列名 ds,y
    # 计算需要几个步数
    periods = horizon_hours if freq.lower() in ("h",) else int(math.ceil(horizon_hours * 4))  # 15min = 4 * hour
    future = m.make_future_dataframe(periods=periods, freq=freq, include_history=False)
    fcst = m.predict(future)[["ds", "yhat", "yhat_lower", "yhat_upper"]]
    # 🔒 强制非负
    fcst[["yhat", "yhat_lower", "yhat_upper"]] = fcst[["yhat", "yhat_lower", "yhat_upper"]].clip(lower=0)
    return fcst

def integrate_usage(fcst: pd.DataFrame, metric: str, freq: str) -> Dict[str, Any]:
    """把预测的速率按时间步长积分成总量（kWh/L/m³）"""
    step_h = 1.0 if freq.lower() in ("h",) else 0.25  # 15min
    meta = METRIC_MAP[metric]
    factor = meta["rate_per_hour"] * step_h
    total = float((fcst["yhat"] * factor).sum())
    lower = float((fcst["yhat_lower"] * factor).sum())
    upper = float((fcst["yhat_upper"] * factor).sum())
    return {"unit": meta["total_unit"], "value": round(total, 2), "lower": round(lower, 2), "upper": round(upper, 2)}

def forecast(
    metric: str,
    horizon_hours: int = 24,
    freq: str = "H",
    lookback_days: int = 30,
    mode: str = "series",   # ✅ 新增：series=逐点预测; total=只返回总量
) -> Dict[str, Any]:
    """对外主入口：根据 mode 返回逐点预测 或 30 天总量"""
    if metric not in METRIC_MAP:
        raise ValueError("metric 需为 electric/water/gas")
    raw = load_series(metric, lookback_days=lookback_days)
    if len(raw) < 20:
        return {
            "metric": metric,
            "points": [],
            "predicted_usage": {"unit": METRIC_MAP[metric]["total_unit"], "value": 0, "lower": 0, "upper": 0},
            "message": "数据太少，无法训练",
        }

    df = resample(raw, freq=freq).rename(columns={"ds": "ds", "y": "y"})
    fcst = fit_and_predict(df, horizon_hours=horizon_hours, freq=freq)
    usage = integrate_usage(fcst, metric, freq)

    if mode == "series":
        points = [
            {
                "timestamp": pd.Timestamp(r.ds).isoformat(),
                "yhat": float(r.yhat),
                "yhat_lower": float(r.yhat_lower),
                "yhat_upper": float(r.yhat_upper),
            }
            for r in fcst.itertuples()
        ]
        return {
            "metric": metric,
            "freq": freq,
            "horizon_hours": horizon_hours,
            "points": points,
            "predicted_usage": usage,
        }
    elif mode == "total":
        return {
            "metric": metric,
            "horizon_hours": horizon_hours,
            "predicted_usage": usage,
            "message": f"预测 {horizon_hours/24:.0f} 天总量",
        }
    else:
        raise ValueError("mode 参数需为 series 或 total")

