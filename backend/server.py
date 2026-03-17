"""
PHARMA-AI FastAPI Server
Includes V-5 Analytics Collection and V-6 Data Export System.
"""

import os
import time
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

import motor.motor_asyncio
from beanie import init_beanie
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from analytics_schema import APIAnalytics, DailyAnalytics
from database_schema import AnalysisBlueprint
from export_schema import ExportRequest, ExportResponse
from export_service import (
    export_analytics_csv,
    export_analytics_json,
    export_analyses_csv,
    export_analyses_json,
    export_cache_stats_json,
    export_requests_csv,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "pharma_ai")


# ---------------------------------------------------------------------------
# Application lifespan (startup / shutdown)
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
    await init_beanie(
        database=client[DB_NAME],
        document_models=[AnalysisBlueprint, APIAnalytics, DailyAnalytics],
    )
    logger.info("Connected to MongoDB and initialized Beanie")
    yield
    client.close()
    logger.info("MongoDB connection closed")


app = FastAPI(
    title="PHARMA-AI API",
    version="1.0.0",
    description="Pharmaceutical drug analysis API with analytics and data export",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Analytics middleware (V-5)
# ---------------------------------------------------------------------------

@app.middleware("http")
async def analytics_middleware(request: Request, call_next):
    start = time.perf_counter()
    response: Response = await call_next(request)
    elapsed_ms = (time.perf_counter() - start) * 1000

    # Skip tracking for export routes to avoid noise
    if not request.url.path.startswith("/api/export"):
        try:
            log = APIAnalytics(
                endpoint=request.url.path,
                method=request.method,
                status_code=response.status_code,
                response_time_ms=round(elapsed_ms, 2),
                cache_hit=response.headers.get("X-Cache", "") == "HIT",
                user_agent=request.headers.get("User-Agent"),
                ip_address=request.client.host if request.client else None,
            )
            await log.insert()
        except Exception as exc:
            logger.warning("Failed to log analytics: %s", exc)

    return response


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat() + "Z"}


# ---------------------------------------------------------------------------
# Drug analysis endpoints
# ---------------------------------------------------------------------------

@app.get("/api/drugs/{drug_name}")
async def get_drug_analysis(drug_name: str):
    doc = await AnalysisBlueprint.find_one(
        AnalysisBlueprint.drug_name == drug_name
    )
    if not doc:
        raise HTTPException(status_code=404, detail=f"Drug '{drug_name}' not found")
    return doc.model_dump()


@app.get("/api/drugs")
async def list_drugs(
    limit: int = Query(default=50, ge=1, le=500),
    skip: int = Query(default=0, ge=0),
    bcs_class: Optional[str] = Query(default=None),
):
    query = AnalysisBlueprint.find()
    if bcs_class:
        query = AnalysisBlueprint.find(AnalysisBlueprint.bcs_class == bcs_class)
    total = await query.count()
    records = await query.skip(skip).limit(limit).to_list()
    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "results": [r.model_dump() for r in records],
    }


# ---------------------------------------------------------------------------
# Analytics endpoints (V-5)
# ---------------------------------------------------------------------------

@app.get("/api/analytics/daily")
async def get_daily_analytics(
    start_date: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    end_date: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
):
    query = DailyAnalytics.find()
    date_filter: dict = {}
    if start_date:
        date_filter["$gte"] = start_date
    if end_date:
        date_filter["$lte"] = end_date
    if date_filter:
        query = DailyAnalytics.find({"date": date_filter})
    records = await query.sort("date").to_list()
    return {"count": len(records), "results": [r.model_dump() for r in records]}


@app.get("/api/analytics/summary")
async def get_analytics_summary():
    all_records = await APIAnalytics.find().to_list()
    total = len(all_records)
    if total == 0:
        return {
            "total_requests": 0,
            "total_errors": 0,
            "cache_hit_rate": 0.0,
            "avg_response_time_ms": 0.0,
        }
    errors = sum(1 for r in all_records if r.status_code >= 400)
    hits = sum(1 for r in all_records if r.cache_hit)
    avg_rt = sum(r.response_time_ms for r in all_records) / total
    return {
        "total_requests": total,
        "total_errors": errors,
        "cache_hit_rate": round(hits / total, 4),
        "avg_response_time_ms": round(avg_rt, 2),
    }


# ---------------------------------------------------------------------------
# V-6: Data Export endpoints
# ---------------------------------------------------------------------------

@app.get(
    "/api/export/analytics/csv",
    summary="Export daily analytics as CSV",
    tags=["Export"],
)
async def export_analytics_as_csv(
    start_date: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    end_date: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    limit: Optional[int] = Query(default=None, ge=1, le=10000),
):
    """Export daily analytics summaries as a streaming CSV file."""
    return export_analytics_csv(start_date, end_date, limit)


@app.get(
    "/api/export/analytics/json",
    response_model=ExportResponse,
    summary="Export daily analytics as JSON",
    tags=["Export"],
)
async def export_analytics_as_json(
    start_date: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    end_date: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    limit: Optional[int] = Query(default=None, ge=1, le=10000),
):
    """Export daily analytics summaries as JSON with metadata."""
    return await export_analytics_json(start_date, end_date, limit)


@app.get(
    "/api/export/analyses/csv",
    summary="Export drug analyses as CSV",
    tags=["Export"],
)
async def export_analyses_as_csv(
    start_date: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    end_date: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    limit: Optional[int] = Query(default=None, ge=1, le=10000),
):
    """Export drug analysis records as a streaming CSV file."""
    return export_analyses_csv(start_date, end_date, limit)


@app.get(
    "/api/export/analyses/json",
    response_model=ExportResponse,
    summary="Export drug analyses as JSON",
    tags=["Export"],
)
async def export_analyses_as_json(
    start_date: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    end_date: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    limit: Optional[int] = Query(default=None, ge=1, le=10000),
):
    """Export drug analysis records as JSON with metadata."""
    return await export_analyses_json(start_date, end_date, limit)


@app.get(
    "/api/export/cache-stats/json",
    summary="Export cache statistics as JSON",
    tags=["Export"],
)
async def export_cache_stats_as_json(
    start_date: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    end_date: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
):
    """Export cache performance statistics as JSON with metadata."""
    return await export_cache_stats_json(start_date, end_date)


@app.get(
    "/api/export/requests/csv",
    summary="Export recent API requests as CSV",
    tags=["Export"],
)
async def export_requests_as_csv(
    start_date: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    end_date: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    limit: Optional[int] = Query(default=1000, ge=1, le=10000),
):
    """Export API request logs as a streaming CSV file."""
    return export_requests_csv(start_date, end_date, limit)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
