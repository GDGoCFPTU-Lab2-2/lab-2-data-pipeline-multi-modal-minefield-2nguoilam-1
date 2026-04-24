from pydantic import BaseModel, Field
from typing import Any, Dict, Optional
from datetime import datetime

# ==========================================
# ROLE 1: LEAD DATA ARCHITECT
# ==========================================
# Your task is to define the Unified Schema for all sources.
# This is v1. Note: A breaking change is coming at 11:00 AM!

class UnifiedDocument(BaseModel):
    """Canonical document contract shared by every pipeline source."""

    document_id: str = Field(..., description="Unique document identifier")
    content: str = Field(..., description="Normalized text or structured content")
    source_type: str = Field(..., description="Origin of the document, for example PDF, Video, HTML, CSV, or Code")
    author: Optional[str] = Field(default="Unknown", description="Author or owner when available")
    timestamp: Optional[datetime] = Field(default=None, description="Source timestamp when available")
    source_metadata: Dict[str, Any] = Field(default_factory=dict, description="Source-specific metadata")

    class Config:
        extra = "allow"

    @property
    def metadata(self) -> Dict[str, Any]:
        """Compatibility alias for code that expects a generic metadata field."""
        return self.source_metadata
