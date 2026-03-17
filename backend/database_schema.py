"""
Database schema definitions for PHARMA-AI.
Defines MongoDB document models using Beanie ORM.
"""

from datetime import datetime
from typing import Optional, List
from beanie import Document
from pydantic import BaseModel, Field


class MolecularProperties(BaseModel):
    molecular_weight: Optional[float] = None
    logp: Optional[float] = None
    hbd: Optional[int] = None  # Hydrogen bond donors
    hba: Optional[int] = None  # Hydrogen bond acceptors
    tpsa: Optional[float] = None  # Topological polar surface area
    rotatable_bonds: Optional[int] = None


class AnalysisBlueprint(Document):
    """Stores drug analysis results."""

    drug_name: str
    smiles: Optional[str] = None
    canonical_smiles: Optional[str] = None
    bcs_class: Optional[str] = None  # Biopharmaceutics Classification System
    solubility_score: Optional[float] = None
    permeability_score: Optional[float] = None
    overall_score: Optional[float] = None
    molecular_properties: Optional[MolecularProperties] = None
    analysis_status: str = "pending"  # pending, completed, failed
    error_message: Optional[str] = None
    pubchem_cid: Optional[int] = None
    synonyms: List[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        collection = "drug_analyses"
        indexes = ["drug_name", "bcs_class", "created_at"]
