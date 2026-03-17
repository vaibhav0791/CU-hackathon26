"""
Analytics schema definitions for PHARMA-AI (V-5).
Tracks API request/response metrics and drug analysis usage.
"""

from datetime import datetime, date
from typing import Optional, Dict, List
from beanie import Document
from pydantic import BaseModel, Field


class EndpointMetrics(BaseModel):
    endpoint: str
    total_requests: int = 0
    total_errors: int = 0
    total_successes: int = 0
    avg_response_time_ms: float = 0.0
    min_response_time_ms: float = 0.0
    max_response_time_ms: float = 0.0
    cache_hits: int = 0
    cache_misses: int = 0


class DailyAnalytics(Document):
    """Daily aggregated analytics summary."""

    date: str  # YYYY-MM-DD format
    total_requests: int = 0
    total_errors: int = 0
    total_successes: int = 0
    avg_response_time_ms: float = 0.0
    cache_hit_rate: float = 0.0
    unique_drugs_analyzed: int = 0
    endpoint_metrics: List[EndpointMetrics] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        collection = "daily_analytics"
        indexes = ["date"]


class APIAnalytics(Document):
    """Individual API request log."""

    endpoint: str
    method: str = "GET"
    status_code: int
    response_time_ms: float
    cache_hit: bool = False
    drug_name: Optional[str] = None
    smiles_analyzed: Optional[str] = None
    error_message: Optional[str] = None
    user_agent: Optional[str] = None
    ip_address: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        collection = "api_requests"
        indexes = ["endpoint", "timestamp", "status_code"]
