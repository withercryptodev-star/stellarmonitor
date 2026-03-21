"""
FastAPI — REST API для сайта StellarMonitor.

Эндпоинты:
  GET /              → health check
  GET /api/alerts    → последние алерты (лента)
  GET /api/stats     → статистика за 24h
  GET /api/pairs     → топ SDEX пары
  GET /api/whale     → только whale алерты

Запуск: uvicorn api:app --host 0.0.0.0 --port 8000
"""

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from db import get_recent_alerts, get_stats_24h, get_top_pairs, init_db
import json

app = FastAPI(title="StellarMonitor API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

init_db()


@app.get("/")
def health():
    return {"status": "ok", "service": "StellarMonitor API"}


@app.get("/api/alerts")
def alerts(
    limit: int = Query(50, le=200),
    type:  str  = Query(None),
    min_usd: float = Query(0),
):
    rows = get_recent_alerts(limit=limit, alert_type=type, min_usd=min_usd)
    # Парсим raw_json для полных данных
    result = []
    for r in rows:
        try:
            data = json.loads(r.get("raw_json", "{}"))
        except Exception:
            data = {}
        data["id"] = r["id"]
        data["created_at"] = r["created_at"]
        data["usd_value"] = r["usd_value"]
        result.append(data)
    return {"alerts": result, "count": len(result)}


@app.get("/api/stats")
def stats():
    s = get_stats_24h()
    return {
        "period": "24h",
        "total_alerts":  s.get("total", 0),
        "whale_count":   s.get("whales", 0),
        "sdex_count":    s.get("sdex", 0),
        "mass_count":    s.get("mass", 0),
        "meme_count":    s.get("meme", 0),
        "biggest_usd":   s.get("biggest_usd", 0),
        "usdc_volume":   s.get("usdc_volume", 0),
    }


@app.get("/api/pairs")
def pairs(limit: int = Query(5, le=20)):
    return {"pairs": get_top_pairs(limit=limit)}


@app.get("/api/whale")
def whale(limit: int = Query(20, le=100), min_usd: float = Query(100_000)):
    rows = get_recent_alerts(
        limit=limit,
        alert_type=None,
        min_usd=min_usd,
    )
    # Фильтруем только whale типы
    whale_types = {"WHALE_XLM", "WHALE_USDC", "AIRDROP_SUMMARY"}
    result = []
    for r in rows:
        if r.get("type") in whale_types:
            try:
                data = json.loads(r.get("raw_json", "{}"))
            except Exception:
                data = {}
            data["id"] = r["id"]
            data["created_at"] = r["created_at"]
            result.append(data)
    return {"alerts": result, "count": len(result)}
