from pydantic import BaseModel

class ExportMetadata(BaseModel):
    filename: str
    file_format: str
    export_time: str

class CSVExportResponse(BaseModel):
    success: bool
    metadata: ExportMetadata
    rows_exported: int

class JSONExportResponse(BaseModel):
    success: bool
    metadata: ExportMetadata
    records: list