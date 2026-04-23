from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class IntentType(str, Enum):
    CREATE_CUBE = "create_cube"
    ADD_MEASURE = "add_measure"
    MODIFY_DIMENSION = "modify_dimension"
    PREVIEW_CUBE = "preview_cube"
    DEPLOY_CUBE = "deploy_cube"
    UNKNOWN = "unknown"


class ColumnSchema(BaseModel):
    name: str
    data_type: str
    nullable: bool = True
    is_pk: bool = False
    is_fk: bool = False
    references_table: Optional[str] = None
    references_column: Optional[str] = None


class TableSchema(BaseModel):
    name: str
    schema_name: str = "dbo"
    columns: List[ColumnSchema] = Field(default_factory=list)
    row_count: Optional[int] = None


class SchemaSnapshot(BaseModel):
    database_name: str
    tables: List[TableSchema] = Field(default_factory=list)


class DetectedAttribute(BaseModel):
    name: str
    source_table: str
    source_column: str
    description: Optional[str] = None


class HierarchyLevel(BaseModel):
    name: str
    source_column: str


class DetectedHierarchy(BaseModel):
    name: str
    levels: List[HierarchyLevel] = Field(default_factory=list)


class CubeDimension(BaseModel):
    name: str
    source_table: str
    key_attribute: str
    attributes: List[DetectedAttribute] = Field(default_factory=list)
    hierarchies: List[DetectedHierarchy] = Field(default_factory=list)
    description: Optional[str] = None


class CubeMeasure(BaseModel):
    name: str
    source_table: str
    source_column: str
    aggregation: str = "sum"
    expression: Optional[str] = None
    is_calculated: bool = False
    description: Optional[str] = None


class CubeFact(BaseModel):
    name: str
    source_table: str
    measures: List[CubeMeasure] = Field(default_factory=list)
    description: Optional[str] = None


class CubeModel(BaseModel):
    cube_name: str
    description: str
    facts: List[CubeFact] = Field(default_factory=list)
    dimensions: List[CubeDimension] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class UserIntent(BaseModel):
    intent: IntentType
    cube_name: Optional[str] = None
    description_hint: Optional[str] = None
    requested_measures: List[str] = Field(default_factory=list)
    requested_dimensions: List[str] = Field(default_factory=list)
    requested_hierarchies: List[str] = Field(default_factory=list)
    extra_instructions: Optional[str] = None
    max_facts: Optional[int] = None
    max_dimensions: Optional[int] = None
    confidence: float = 0.0


class ValidationIssue(BaseModel):
    level: str
    code: str
    message: str


class ValidationResult(BaseModel):
    is_valid: bool
    issues: List[ValidationIssue] = Field(default_factory=list)


class PromptRequest(BaseModel):
    dw: str
    prompt: str


class CubeActionResponse(BaseModel):
    status: str
    intent: IntentType
    cube_model: Optional[CubeModel] = None
    validation: Optional[ValidationResult] = None
    xmla_script: Optional[str] = None
    preview: Optional[Dict[str, Any]] = None
    message: Optional[str] = None