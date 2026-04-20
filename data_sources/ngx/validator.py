# from pydantic import BaseModel, Field, field_validator
# from typing import Annotated

# class LightEnrichment(BaseModel):
#     summary: str
#     signal_type: str
#     tags: Annotated[list[str], Field(min_length=1, max_length=5)]
#     signals: Annotated[list[str], Field(min_length=1, max_length=5)]
#     data_density: int = Field(..., ge=1, le=10)

# class SectionEnrichment(BaseModel):
#     refined_title: str
#     summary: str
#     tags: list[str] = Field(..., max_length=5)
#     data_density: int = Field(..., ge=1, le=10)
#     contains_metrics: bool

#     @field_validator('tags', mode='before')
#     def ensure_list(cls, v):
#         if isinstance(v, str):
#             return [t.strip() for t in v.split(",")]
#         return v
    
# class TableEnrichment(BaseModel):
#     table_name: str
#     tags: list[str]
#     summary: str

from typing import Annotated, Literal
from pydantic import BaseModel, Field, field_validator

SectionType = Literal[
    "financial_statement", "management_discussion", "audit_report",
    "governance", "risk_factors", "notes_to_accounts", "operational_review",
    "forward_guidance", "boilerplate", "other"
]

TableType = Literal[
    "income_statement", "balance_sheet", "cash_flow_statement",
    "statement_of_changes_in_equity", "segmental_breakdown",
    "loan_portfolio", "capital_adequacy", "five_year_summary",
    "directors_shareholding", "operational_kpi", "other"
]

SignalType = Literal[
    "filing_delay", "dividend", "board_change", "agm_notice",
    "earnings_release", "regulatory_action", "rights_issue",
    "name_change", "suspension", "general"
]

Tags = Annotated[list[str], Field(min_length=1, max_length=5)]


class SectionEnrichment(BaseModel):
    refined_title: str = Field(..., max_length=80)
    summary: str
    tags: Tags
    data_density: int = Field(..., ge=0, le=10)
    contains_metrics: bool
    section_type: SectionType = "other"

    @field_validator("tags", mode="before")
    @classmethod
    def coerce_tags(cls, v):
        if isinstance(v, str):
            return [t.strip().lower() for t in v.split(",")]
        if isinstance(v, list):
            return [t.strip().lower() for t in v]
        return v

    @field_validator("refined_title", mode="before")
    @classmethod
    def clean_title(cls, v):
        if isinstance(v, str) and "_" in v and " " not in v:
            return v.replace("_", " ").title()
        return v


class TableEnrichment(BaseModel):
    table_name: str = Field(..., pattern=r'^[a-z0-9_]+$')
    table_type: TableType = "other"
    summary: str
    tags: Tags
    reporting_period: str | None = None

    @field_validator("tags", mode="before")
    @classmethod
    def coerce_tags(cls, v):
        if isinstance(v, str):
            return [t.strip().lower() for t in v.split(",")]
        if isinstance(v, list):
            return [t.strip().lower() for t in v]
        return v

    @field_validator("table_name", mode="before")
    @classmethod
    def clean_table_name(cls, v):
        if isinstance(v, str):
            return v.strip().lower().replace(" ", "_").replace("-", "_")
        return v

    @field_validator("reporting_period", mode="before")
    @classmethod
    def clean_period(cls, v):
        if not v or str(v).strip().lower() in ("null", "none", "n/a", ""):
            return None
        return str(v).strip()


class LightEnrichment(BaseModel):
    summary: str
    signal_type: SignalType = "general"
    signals: Annotated[list[str], Field(min_length=1, max_length=5)]
    tags: Tags
    data_density: int = Field(..., ge=1, le=10)

    @field_validator("signals", "tags", mode="before")
    @classmethod
    def coerce_list(cls, v):
        if isinstance(v, str):
            return [t.strip().lower() for t in v.split(",")]
        if isinstance(v, list):
            return [t.strip().lower() for t in v]
        return v