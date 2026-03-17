"""
Tests for V-6: Data Export System.
Tests all export endpoints, formats, error scenarios, and data integrity.
"""

import csv
import io
import json
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------

def _make_daily_analytics_doc(date_str: str, requests: int = 100):
    doc = MagicMock()
    doc.date = date_str
    doc.total_requests = requests
    doc.total_errors = 5
    doc.total_successes = requests - 5
    doc.avg_response_time_ms = 42.0
    doc.cache_hit_rate = 0.75
    doc.unique_drugs_analyzed = 10
    return doc


def _make_analysis_doc(drug_name: str):
    doc = MagicMock()
    doc.drug_name = drug_name
    doc.smiles = "CC(=O)Oc1ccccc1C(=O)O"
    doc.canonical_smiles = "CC(=O)Oc1ccccc1C(=O)O"
    doc.bcs_class = "II"
    doc.solubility_score = 0.65
    doc.permeability_score = 0.80
    doc.overall_score = 0.72
    doc.analysis_status = "completed"
    doc.pubchem_cid = 2244
    doc.created_at = datetime(2026, 3, 1, 12, 0, 0)
    return doc


def _make_request_doc(endpoint: str = "/api/drugs/aspirin", cache_hit: bool = False):
    doc = MagicMock()
    doc.endpoint = endpoint
    doc.method = "GET"
    doc.status_code = 200
    doc.response_time_ms = 35.5
    doc.cache_hit = cache_hit
    doc.drug_name = "aspirin"
    doc.error_message = None
    doc.timestamp = datetime(2026, 3, 17, 10, 0, 0)
    return doc


# ---------------------------------------------------------------------------
# Export schema tests
# ---------------------------------------------------------------------------

class TestExportSchema:
    def test_export_metadata_fields(self):
        from export_schema import ExportMetadata
        meta = ExportMetadata(
            export_type="analytics",
            export_format="json",
            generated_at="2026-03-17T10:00:00Z",
            record_count=5,
        )
        assert meta.export_type == "analytics"
        assert meta.export_format == "json"
        assert meta.record_count == 5
        assert meta.api_version == "1.0.0"

    def test_analytics_export_row(self):
        from export_schema import AnalyticsExportRow
        row = AnalyticsExportRow(
            date="2026-03-17",
            total_requests=100,
            total_errors=5,
            total_successes=95,
            avg_response_time_ms=42.0,
            cache_hit_rate=0.75,
            unique_drugs_analyzed=10,
        )
        d = row.model_dump()
        assert d["date"] == "2026-03-17"
        assert d["total_requests"] == 100
        assert d["cache_hit_rate"] == 0.75

    def test_analysis_export_row(self):
        from export_schema import AnalysisExportRow
        row = AnalysisExportRow(
            drug_name="aspirin",
            smiles="CC(=O)Oc1ccccc1C(=O)O",
            canonical_smiles="CC(=O)Oc1ccccc1C(=O)O",
            bcs_class="II",
            solubility_score=0.65,
            permeability_score=0.80,
            overall_score=0.72,
            analysis_status="completed",
            pubchem_cid=2244,
            created_at="2026-03-01T12:00:00Z",
        )
        assert row.drug_name == "aspirin"
        assert row.bcs_class == "II"

    def test_request_export_row(self):
        from export_schema import RequestExportRow
        row = RequestExportRow(
            endpoint="/api/drugs/aspirin",
            method="GET",
            status_code=200,
            response_time_ms=35.5,
            cache_hit=True,
            drug_name="aspirin",
            error_message=None,
            timestamp="2026-03-17T10:00:00Z",
        )
        assert row.cache_hit is True
        assert row.status_code == 200

    def test_cache_stats_export(self):
        from export_schema import CacheStatsExport
        stats = CacheStatsExport(
            total_requests=200,
            cache_hits=150,
            cache_misses=50,
            hit_rate=0.75,
            avg_response_time_cached_ms=12.5,
            avg_response_time_uncached_ms=85.0,
            generated_at="2026-03-17T10:00:00Z",
        )
        assert stats.hit_rate == 0.75
        assert stats.cache_hits + stats.cache_misses == stats.total_requests

    def test_export_request_validation(self):
        from export_schema import ExportRequest, ExportType, ExportFormat
        req = ExportRequest(
            export_type=ExportType.ANALYTICS,
            export_format=ExportFormat.CSV,
        )
        assert req.export_type == ExportType.ANALYTICS
        assert req.export_format == ExportFormat.CSV
        assert req.include_metadata is True

    def test_export_request_limit_validation(self):
        from export_schema import ExportRequest, ExportType, ExportFormat
        import pydantic
        with pytest.raises(pydantic.ValidationError):
            ExportRequest(
                export_type=ExportType.ANALYTICS,
                export_format=ExportFormat.CSV,
                limit=0,  # must be >= 1
            )

    def test_export_response_structure(self):
        from export_schema import ExportResponse, ExportMetadata
        meta = ExportMetadata(
            export_type="analyses",
            export_format="json",
            generated_at="2026-03-17T10:00:00Z",
            record_count=2,
        )
        resp = ExportResponse(
            metadata=meta,
            data=[{"drug_name": "aspirin"}, {"drug_name": "ibuprofen"}],
        )
        assert len(resp.data) == 2
        assert resp.metadata.record_count == 2


# ---------------------------------------------------------------------------
# Export service unit tests (mocked DB)
# ---------------------------------------------------------------------------

class TestExportService:
    @pytest.mark.asyncio
    async def test_get_analytics_data_empty(self):
        from export_service import get_analytics_data
        from analytics_schema import DailyAnalytics

        mock_query = MagicMock()
        mock_query.sort.return_value = mock_query
        mock_query.limit.return_value = mock_query
        mock_query.to_list = AsyncMock(return_value=[])

        with patch.object(DailyAnalytics, "find", return_value=mock_query):
            rows = await get_analytics_data()
        assert rows == []

    @pytest.mark.asyncio
    async def test_get_analytics_data_returns_rows(self):
        from export_service import get_analytics_data
        from analytics_schema import DailyAnalytics

        docs = [_make_daily_analytics_doc("2026-03-17", 100)]

        mock_query = MagicMock()
        mock_query.sort.return_value = mock_query
        mock_query.to_list = AsyncMock(return_value=docs)

        with patch.object(DailyAnalytics, "find", return_value=mock_query):
            rows = await get_analytics_data()

        assert len(rows) == 1
        assert rows[0].date == "2026-03-17"
        assert rows[0].total_requests == 100
        assert rows[0].cache_hit_rate == 0.75

    @pytest.mark.asyncio
    async def test_export_analytics_json_structure(self):
        from export_service import export_analytics_json
        from analytics_schema import DailyAnalytics

        docs = [
            _make_daily_analytics_doc("2026-03-16", 80),
            _make_daily_analytics_doc("2026-03-17", 100),
        ]

        mock_query = MagicMock()
        mock_query.sort.return_value = mock_query
        mock_query.to_list = AsyncMock(return_value=docs)

        with patch.object(DailyAnalytics, "find", return_value=mock_query):
            result = await export_analytics_json()

        assert result.metadata.record_count == 2
        assert result.metadata.export_type == "analytics"
        assert result.metadata.export_format == "json"
        assert len(result.data) == 2
        assert result.data[0]["date"] == "2026-03-16"

    @pytest.mark.asyncio
    async def test_export_analytics_json_with_date_range(self):
        from export_service import export_analytics_json
        from analytics_schema import DailyAnalytics

        docs = [_make_daily_analytics_doc("2026-03-17", 50)]

        mock_query = MagicMock()
        mock_query.sort.return_value = mock_query
        mock_query.to_list = AsyncMock(return_value=docs)

        with patch.object(DailyAnalytics, "find", return_value=mock_query):
            result = await export_analytics_json(
                start_date="2026-03-17", end_date="2026-03-17"
            )

        assert result.metadata.start_date == "2026-03-17"
        assert result.metadata.end_date == "2026-03-17"
        assert result.metadata.record_count == 1

    @pytest.mark.asyncio
    async def test_get_analyses_data(self):
        from export_service import get_analyses_data
        from database_schema import AnalysisBlueprint

        docs = [_make_analysis_doc("aspirin"), _make_analysis_doc("ibuprofen")]

        mock_query = MagicMock()
        mock_query.sort.return_value = mock_query
        mock_query.to_list = AsyncMock(return_value=docs)

        with patch.object(AnalysisBlueprint, "find", return_value=mock_query):
            rows = await get_analyses_data()

        assert len(rows) == 2
        assert rows[0].drug_name == "aspirin"
        assert rows[0].bcs_class == "II"
        assert rows[0].created_at.endswith("Z")

    @pytest.mark.asyncio
    async def test_export_analyses_json_structure(self):
        from export_service import export_analyses_json
        from database_schema import AnalysisBlueprint

        docs = [_make_analysis_doc("aspirin")]

        mock_query = MagicMock()
        mock_query.sort.return_value = mock_query
        mock_query.to_list = AsyncMock(return_value=docs)

        with patch.object(AnalysisBlueprint, "find", return_value=mock_query):
            result = await export_analyses_json()

        assert result.metadata.export_type == "analyses"
        assert result.metadata.record_count == 1
        assert result.data[0]["drug_name"] == "aspirin"

    @pytest.mark.asyncio
    async def test_get_requests_data(self):
        from export_service import get_requests_data
        from analytics_schema import APIAnalytics

        docs = [
            _make_request_doc("/api/drugs/aspirin", cache_hit=True),
            _make_request_doc("/api/drugs/ibuprofen", cache_hit=False),
        ]

        mock_query = MagicMock()
        mock_query.sort.return_value = mock_query
        mock_query.limit.return_value = mock_query
        mock_query.to_list = AsyncMock(return_value=docs)

        with patch.object(APIAnalytics, "find", return_value=mock_query):
            rows = await get_requests_data()

        assert len(rows) == 2
        assert rows[0].cache_hit is True
        assert rows[1].cache_hit is False

    @pytest.mark.asyncio
    async def test_get_cache_stats_empty(self):
        from export_service import get_cache_stats
        from analytics_schema import APIAnalytics

        mock_query = MagicMock()
        mock_query.to_list = AsyncMock(return_value=[])

        with patch.object(APIAnalytics, "find", return_value=mock_query):
            stats = await get_cache_stats()

        assert stats.total_requests == 0
        assert stats.hit_rate == 0.0
        assert stats.cache_hits == 0

    @pytest.mark.asyncio
    async def test_get_cache_stats_with_data(self):
        from export_service import get_cache_stats
        from analytics_schema import APIAnalytics

        docs = [
            _make_request_doc(cache_hit=True),   # hit,  35.5ms
            _make_request_doc(cache_hit=True),   # hit,  35.5ms
            _make_request_doc(cache_hit=False),  # miss, 35.5ms
        ]

        mock_query = MagicMock()
        mock_query.to_list = AsyncMock(return_value=docs)

        with patch.object(APIAnalytics, "find", return_value=mock_query):
            stats = await get_cache_stats()

        assert stats.total_requests == 3
        assert stats.cache_hits == 2
        assert stats.cache_misses == 1
        assert abs(stats.hit_rate - 0.6667) < 0.001
        assert stats.avg_response_time_cached_ms == 35.5
        assert stats.avg_response_time_uncached_ms == 35.5

    @pytest.mark.asyncio
    async def test_export_cache_stats_json_structure(self):
        from export_service import export_cache_stats_json
        from analytics_schema import APIAnalytics

        docs = [_make_request_doc(cache_hit=True)]

        mock_query = MagicMock()
        mock_query.to_list = AsyncMock(return_value=docs)

        with patch.object(APIAnalytics, "find", return_value=mock_query):
            result = await export_cache_stats_json()

        assert "metadata" in result
        assert "data" in result
        assert result["metadata"]["export_type"] == "cache_stats"
        assert result["data"]["total_requests"] == 1
        assert result["data"]["cache_hits"] == 1

    @pytest.mark.asyncio
    async def test_export_analytics_csv_streaming(self):
        from export_service import export_analytics_csv
        from analytics_schema import DailyAnalytics

        docs = [
            _make_daily_analytics_doc("2026-03-16", 80),
            _make_daily_analytics_doc("2026-03-17", 100),
        ]

        mock_query = MagicMock()
        mock_query.sort.return_value = mock_query
        mock_query.to_list = AsyncMock(return_value=docs)

        with patch.object(DailyAnalytics, "find", return_value=mock_query):
            response = export_analytics_csv()

        assert response.media_type == "text/csv"
        assert "attachment" in response.headers["content-disposition"]
        assert ".csv" in response.headers["content-disposition"]

    @pytest.mark.asyncio
    async def test_export_analyses_csv_streaming(self):
        from export_service import export_analyses_csv
        from database_schema import AnalysisBlueprint

        docs = [_make_analysis_doc("aspirin")]

        mock_query = MagicMock()
        mock_query.sort.return_value = mock_query
        mock_query.to_list = AsyncMock(return_value=docs)

        with patch.object(AnalysisBlueprint, "find", return_value=mock_query):
            response = export_analyses_csv()

        assert response.media_type == "text/csv"
        assert "analyses_" in response.headers["content-disposition"]

    @pytest.mark.asyncio
    async def test_export_requests_csv_streaming(self):
        from export_service import export_requests_csv
        from analytics_schema import APIAnalytics

        docs = [_make_request_doc()]

        mock_query = MagicMock()
        mock_query.sort.return_value = mock_query
        mock_query.limit.return_value = mock_query
        mock_query.to_list = AsyncMock(return_value=docs)

        with patch.object(APIAnalytics, "find", return_value=mock_query):
            response = export_requests_csv()

        assert response.media_type == "text/csv"
        assert "requests_" in response.headers["content-disposition"]


# ---------------------------------------------------------------------------
# CSV content integrity tests
# ---------------------------------------------------------------------------

class TestCSVIntegrity:
    @pytest.mark.asyncio
    async def test_analytics_csv_has_correct_headers(self):
        from export_service import _stream_analytics_csv
        from analytics_schema import DailyAnalytics

        docs = [_make_daily_analytics_doc("2026-03-17", 100)]

        mock_query = MagicMock()
        mock_query.sort.return_value = mock_query
        mock_query.to_list = AsyncMock(return_value=docs)

        with patch.object(DailyAnalytics, "find", return_value=mock_query):
            chunks = []
            async for chunk in _stream_analytics_csv(None, None, None):
                chunks.append(chunk)

        content = "".join(chunks)
        reader = csv.DictReader(io.StringIO(content))
        rows = list(reader)
        assert len(rows) == 1
        assert "date" in reader.fieldnames
        assert "total_requests" in reader.fieldnames
        assert "cache_hit_rate" in reader.fieldnames
        assert rows[0]["date"] == "2026-03-17"
        assert rows[0]["total_requests"] == "100"

    @pytest.mark.asyncio
    async def test_analyses_csv_has_correct_headers(self):
        from export_service import _stream_analyses_csv
        from database_schema import AnalysisBlueprint

        docs = [_make_analysis_doc("aspirin")]

        mock_query = MagicMock()
        mock_query.sort.return_value = mock_query
        mock_query.to_list = AsyncMock(return_value=docs)

        with patch.object(AnalysisBlueprint, "find", return_value=mock_query):
            chunks = []
            async for chunk in _stream_analyses_csv(None, None, None):
                chunks.append(chunk)

        content = "".join(chunks)
        reader = csv.DictReader(io.StringIO(content))
        rows = list(reader)
        assert len(rows) == 1
        assert "drug_name" in reader.fieldnames
        assert "bcs_class" in reader.fieldnames
        assert rows[0]["drug_name"] == "aspirin"
        assert rows[0]["bcs_class"] == "II"

    @pytest.mark.asyncio
    async def test_requests_csv_has_correct_headers(self):
        from export_service import _stream_requests_csv
        from analytics_schema import APIAnalytics

        docs = [_make_request_doc("/api/drugs/aspirin", cache_hit=True)]

        mock_query = MagicMock()
        mock_query.sort.return_value = mock_query
        mock_query.limit.return_value = mock_query
        mock_query.to_list = AsyncMock(return_value=docs)

        with patch.object(APIAnalytics, "find", return_value=mock_query):
            chunks = []
            async for chunk in _stream_requests_csv(None, None, None):
                chunks.append(chunk)

        content = "".join(chunks)
        reader = csv.DictReader(io.StringIO(content))
        rows = list(reader)
        assert len(rows) == 1
        assert "endpoint" in reader.fieldnames
        assert "status_code" in reader.fieldnames
        assert "cache_hit" in reader.fieldnames
        assert rows[0]["endpoint"] == "/api/drugs/aspirin"
        assert rows[0]["cache_hit"] == "True"

    @pytest.mark.asyncio
    async def test_analytics_csv_empty_dataset(self):
        from export_service import _stream_analytics_csv
        from analytics_schema import DailyAnalytics

        mock_query = MagicMock()
        mock_query.sort.return_value = mock_query
        mock_query.to_list = AsyncMock(return_value=[])

        with patch.object(DailyAnalytics, "find", return_value=mock_query):
            chunks = []
            async for chunk in _stream_analytics_csv(None, None, None):
                chunks.append(chunk)

        content = "".join(chunks)
        lines = [l for l in content.split("\n") if l.strip()]
        # Should still have header row
        assert len(lines) == 1
        assert "date" in lines[0]


# ---------------------------------------------------------------------------
# Date filter helper tests
# ---------------------------------------------------------------------------

class TestDateFilters:
    def test_date_filter_no_dates(self):
        from export_service import _date_filter
        result = _date_filter(None, None)
        assert result == {}

    def test_date_filter_start_only(self):
        from export_service import _date_filter
        result = _date_filter("2026-03-01", None)
        assert result == {"$gte": "2026-03-01"}

    def test_date_filter_end_only(self):
        from export_service import _date_filter
        result = _date_filter(None, "2026-03-31")
        assert result == {"$lte": "2026-03-31"}

    def test_date_filter_both(self):
        from export_service import _date_filter
        result = _date_filter("2026-03-01", "2026-03-31")
        assert result == {"$gte": "2026-03-01", "$lte": "2026-03-31"}

    def test_ts_filter_no_dates(self):
        from export_service import _ts_filter
        result = _ts_filter(None, None)
        assert result == {}

    def test_ts_filter_with_dates(self):
        from export_service import _ts_filter
        result = _ts_filter("2026-03-01", "2026-03-31")
        assert "$gte" in result
        assert "$lte" in result
        assert result["$gte"] == datetime(2026, 3, 1)

    def test_ts_filter_end_date_is_end_of_day(self):
        from export_service import _ts_filter
        result = _ts_filter(None, "2026-03-31")
        assert result["$lte"].hour == 23
        assert result["$lte"].minute == 59
        assert result["$lte"].second == 59


# ---------------------------------------------------------------------------
# Metadata generation tests
# ---------------------------------------------------------------------------

class TestMetadataGeneration:
    def test_build_metadata_basic(self):
        from export_service import _build_metadata
        meta = _build_metadata("analytics", "csv", 42)
        assert meta.export_type == "analytics"
        assert meta.export_format == "csv"
        assert meta.record_count == 42
        assert meta.start_date is None
        assert meta.end_date is None
        assert meta.generated_at.endswith("Z")

    def test_build_metadata_with_dates(self):
        from export_service import _build_metadata
        meta = _build_metadata(
            "analyses", "json", 10, "2026-03-01", "2026-03-31"
        )
        assert meta.start_date == "2026-03-01"
        assert meta.end_date == "2026-03-31"

    def test_now_iso_format(self):
        from export_service import _now_iso
        ts = _now_iso()
        assert ts.endswith("Z")
        # Should be parseable
        datetime.fromisoformat(ts.rstrip("Z"))
