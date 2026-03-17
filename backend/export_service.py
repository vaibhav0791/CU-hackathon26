"""
Export service for PHARMA-AI (V-6).
Handles CSV and JSON export of analytics, drug analyses, and request logs.
Supports streaming responses for large datasets.
"""

import csv
import io
import json
from datetime import datetime, date
from typing import Optional, AsyncIterator, List, Dict, Any

from beanie.operators import GTE, LTE
from fastapi.responses import StreamingResponse

from analytics_schema import DailyAnalytics, APIAnalytics
from database_schema import AnalysisBlueprint
from export_schema import (
    ExportMetadata,
    ExportResponse,
    AnalyticsExportRow,
    AnalysisExportRow,
    RequestExportRow,
    CacheStatsExport,
)


def _now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _build_metadata(
    export_type: str,
    export_format: str,
    record_count: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> ExportMetadata:
    return ExportMetadata(
        export_type=export_type,
        export_format=export_format,
        generated_at=_now_iso(),
        record_count=record_count,
        start_date=start_date,
        end_date=end_date,
    )


# ---------------------------------------------------------------------------
# Helpers for building date filters
# ---------------------------------------------------------------------------

def _date_filter(start_date: Optional[str], end_date: Optional[str]) -> dict:
    """Return a Beanie query expression dict for date range filtering."""
    conditions = {}
    if start_date:
        conditions["$gte"] = start_date
    if end_date:
        conditions["$lte"] = end_date
    return conditions


def _ts_filter(start_date: Optional[str], end_date: Optional[str]) -> dict:
    """Return a Beanie query expression dict for timestamp filtering."""
    conditions = {}
    if start_date:
        try:
            conditions["$gte"] = datetime.fromisoformat(start_date)
        except ValueError:
            conditions["$gte"] = datetime.strptime(start_date, "%Y-%m-%d")
    if end_date:
        try:
            parsed = datetime.fromisoformat(end_date)
            # If the value is date-only (no time component), treat as end of day
            if "T" not in end_date and " " not in end_date:
                parsed = parsed.replace(hour=23, minute=59, second=59)
            conditions["$lte"] = parsed
        except ValueError:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59
            )
            conditions["$lte"] = end_dt
    return conditions


# ---------------------------------------------------------------------------
# Analytics export
# ---------------------------------------------------------------------------

async def get_analytics_data(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[AnalyticsExportRow]:
    """Fetch DailyAnalytics records and convert to export rows."""
    query = DailyAnalytics.find()
    date_cond = _date_filter(start_date, end_date)
    if date_cond:
        query = DailyAnalytics.find({"date": date_cond})
    query = query.sort("date")
    if limit:
        query = query.limit(limit)
    records = await query.to_list()
    return [
        AnalyticsExportRow(
            date=r.date,
            total_requests=r.total_requests,
            total_errors=r.total_errors,
            total_successes=r.total_successes,
            avg_response_time_ms=r.avg_response_time_ms,
            cache_hit_rate=r.cache_hit_rate,
            unique_drugs_analyzed=r.unique_drugs_analyzed,
        )
        for r in records
    ]


async def export_analytics_json(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None,
) -> ExportResponse:
    rows = await get_analytics_data(start_date, end_date, limit)
    metadata = _build_metadata(
        "analytics", "json", len(rows), start_date, end_date
    )
    return ExportResponse(
        metadata=metadata,
        data=[r.model_dump() for r in rows],
    )


async def _stream_analytics_csv(
    start_date: Optional[str],
    end_date: Optional[str],
    limit: Optional[int],
) -> AsyncIterator[str]:
    rows = await get_analytics_data(start_date, end_date, limit)
    output = io.StringIO()
    if rows:
        writer = csv.DictWriter(
            output, fieldnames=list(rows[0].model_dump().keys())
        )
        writer.writeheader()
        yield output.getvalue()
        output.truncate(0)
        output.seek(0)
        for row in rows:
            writer.writerow(row.model_dump())
            yield output.getvalue()
            output.truncate(0)
            output.seek(0)
    else:
        writer = csv.DictWriter(
            output,
            fieldnames=[
                "date", "total_requests", "total_errors", "total_successes",
                "avg_response_time_ms", "cache_hit_rate", "unique_drugs_analyzed",
            ],
        )
        writer.writeheader()
        yield output.getvalue()


def export_analytics_csv(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None,
) -> StreamingResponse:
    filename = f"analytics_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    return StreamingResponse(
        _stream_analytics_csv(start_date, end_date, limit),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# Drug analyses export
# ---------------------------------------------------------------------------

async def get_analyses_data(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[AnalysisExportRow]:
    query = AnalysisBlueprint.find()
    ts_cond = _ts_filter(start_date, end_date)
    if ts_cond:
        query = AnalysisBlueprint.find({"created_at": ts_cond})
    query = query.sort("-created_at")
    if limit:
        query = query.limit(limit)
    records = await query.to_list()
    return [
        AnalysisExportRow(
            drug_name=r.drug_name,
            smiles=r.smiles,
            canonical_smiles=r.canonical_smiles,
            bcs_class=r.bcs_class,
            solubility_score=r.solubility_score,
            permeability_score=r.permeability_score,
            overall_score=r.overall_score,
            analysis_status=r.analysis_status,
            pubchem_cid=r.pubchem_cid,
            created_at=r.created_at.isoformat() + "Z",
        )
        for r in records
    ]


async def export_analyses_json(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None,
) -> ExportResponse:
    rows = await get_analyses_data(start_date, end_date, limit)
    metadata = _build_metadata(
        "analyses", "json", len(rows), start_date, end_date
    )
    return ExportResponse(
        metadata=metadata,
        data=[r.model_dump() for r in rows],
    )


async def _stream_analyses_csv(
    start_date: Optional[str],
    end_date: Optional[str],
    limit: Optional[int],
) -> AsyncIterator[str]:
    rows = await get_analyses_data(start_date, end_date, limit)
    output = io.StringIO()
    fieldnames = [
        "drug_name", "smiles", "canonical_smiles", "bcs_class",
        "solubility_score", "permeability_score", "overall_score",
        "analysis_status", "pubchem_cid", "created_at",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    yield output.getvalue()
    output.truncate(0)
    output.seek(0)
    for row in rows:
        writer.writerow(row.model_dump())
        yield output.getvalue()
        output.truncate(0)
        output.seek(0)


def export_analyses_csv(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None,
) -> StreamingResponse:
    filename = f"analyses_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    return StreamingResponse(
        _stream_analyses_csv(start_date, end_date, limit),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# API requests export
# ---------------------------------------------------------------------------

async def get_requests_data(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[RequestExportRow]:
    query = APIAnalytics.find()
    ts_cond = _ts_filter(start_date, end_date)
    if ts_cond:
        query = APIAnalytics.find({"timestamp": ts_cond})
    query = query.sort("-timestamp")
    if limit:
        query = query.limit(limit)
    records = await query.to_list()
    return [
        RequestExportRow(
            endpoint=r.endpoint,
            method=r.method,
            status_code=r.status_code,
            response_time_ms=r.response_time_ms,
            cache_hit=r.cache_hit,
            drug_name=r.drug_name,
            error_message=r.error_message,
            timestamp=r.timestamp.isoformat() + "Z",
        )
        for r in records
    ]


async def _stream_requests_csv(
    start_date: Optional[str],
    end_date: Optional[str],
    limit: Optional[int],
) -> AsyncIterator[str]:
    rows = await get_requests_data(start_date, end_date, limit)
    output = io.StringIO()
    fieldnames = [
        "endpoint", "method", "status_code", "response_time_ms",
        "cache_hit", "drug_name", "error_message", "timestamp",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    yield output.getvalue()
    output.truncate(0)
    output.seek(0)
    for row in rows:
        writer.writerow(row.model_dump())
        yield output.getvalue()
        output.truncate(0)
        output.seek(0)


def export_requests_csv(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None,
) -> StreamingResponse:
    filename = f"requests_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    return StreamingResponse(
        _stream_requests_csv(start_date, end_date, limit),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# Cache statistics export
# ---------------------------------------------------------------------------

async def get_cache_stats(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> CacheStatsExport:
    query = APIAnalytics.find()
    ts_cond = _ts_filter(start_date, end_date)
    if ts_cond:
        query = APIAnalytics.find({"timestamp": ts_cond})
    records = await query.to_list()

    total = len(records)
    hits = sum(1 for r in records if r.cache_hit)
    misses = total - hits
    hit_rate = hits / total if total else 0.0

    cached_times = [r.response_time_ms for r in records if r.cache_hit]
    uncached_times = [r.response_time_ms for r in records if not r.cache_hit]

    avg_cached = sum(cached_times) / len(cached_times) if cached_times else 0.0
    avg_uncached = sum(uncached_times) / len(uncached_times) if uncached_times else 0.0

    return CacheStatsExport(
        total_requests=total,
        cache_hits=hits,
        cache_misses=misses,
        hit_rate=round(hit_rate, 4),
        avg_response_time_cached_ms=round(avg_cached, 2),
        avg_response_time_uncached_ms=round(avg_uncached, 2),
        generated_at=_now_iso(),
    )


async def export_cache_stats_json(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Dict[str, Any]:
    stats = await get_cache_stats(start_date, end_date)
    metadata = _build_metadata(
        "cache_stats", "json", 1, start_date, end_date
    )
    return {
        "metadata": metadata.model_dump(),
        "data": stats.model_dump(),
    }
