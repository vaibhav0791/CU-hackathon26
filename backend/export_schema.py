"""
Export schema definitions for PHARMA-AI (V-6).
Defines models for data export requests and responses.
"""

from datetime import datetime
from typing import Optional, List, Any, Dict
from enum import Enum
from pydantic import BaseModel, Field


class ExportFormat(str, Enum):
    CSV = "csv"
    JSON = "json"


class ExportType(str, Enum):
    ANALYTICS = "analytics"
    ANALYSES = "analyses"
    REQUESTS = "requests"
    CACHE_STATS = "cache_stats"


class ExportRequest(BaseModel):
    """Request model for scheduled or parameterized exports."""

    export_type: ExportType
    export_format: ExportFormat
    start_date: Optional[str] = None   # YYYY-MM-DD
    end_date: Optional[str] = None     # YYYY-MM-DD
    limit: Optional[int] = Field(default=None, ge=1, le=10000)
    include_metadata: bool = True


class ExportMetadata(BaseModel):
    """Metadata included in every export file."""

    export_type: str
    export_format: str
    generated_at: str
    record_count: int
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    api_version: str = "1.0.0"


class AnalyticsExportRow(BaseModel):
    """Single row for analytics CSV/JSON export."""

    date: str
    total_requests: int
    total_errors: int
    total_successes: int
    avg_response_time_ms: float
    cache_hit_rate: float
    unique_drugs_analyzed: int


class AnalysisExportRow(BaseModel):
    """Single row for drug analysis CSV/JSON export."""

    drug_name: str
    smiles: Optional[str]
    canonical_smiles: Optional[str]
    bcs_class: Optional[str]
    solubility_score: Optional[float]
    permeability_score: Optional[float]
    overall_score: Optional[float]
    analysis_status: str
    pubchem_cid: Optional[int]
    created_at: str


class RequestExportRow(BaseModel):
    """Single row for API request log CSV/JSON export."""

    endpoint: str
    method: str
    status_code: int
    response_time_ms: float
    cache_hit: bool
    drug_name: Optional[str]
    error_message: Optional[str]
    timestamp: str


class CacheStatsExport(BaseModel):
    """Cache statistics export model."""

    total_requests: int
    cache_hits: int
    cache_misses: int
    hit_rate: float
    avg_response_time_cached_ms: float
    avg_response_time_uncached_ms: float
    generated_at: str


class ExportResponse(BaseModel):
    """Response envelope for JSON exports."""

    metadata: ExportMetadata
    data: List[Dict[str, Any]]
