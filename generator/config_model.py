"""
Pydantic models for the JSON config schema.

Each column is specified in terms of Parquet-native types (physical + logical),
not Pinot types. This ensures we can generate Parquet files that exercise every
type combination the Iceberg-to-Pinot pipeline might encounter.
"""
from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, model_validator


class PhysicalType(str, Enum):
    BOOLEAN = "BOOLEAN"
    INT32 = "INT32"
    INT64 = "INT64"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    BINARY = "BINARY"
    FIXED_LEN_BYTE_ARRAY = "FIXED_LEN_BYTE_ARRAY"
    INT96 = "INT96"
    # Complex pseudo-types (not physical in Parquet, but simplifies config)
    LIST = "LIST"
    MAP = "MAP"
    STRUCT = "STRUCT"


class LogicalType(str, Enum):
    # Integer subtypes
    INT8 = "INT8"
    INT16 = "INT16"
    INT32 = "INT32"
    INT64 = "INT64"
    UINT8 = "UINT8"
    UINT16 = "UINT16"
    UINT32 = "UINT32"
    UINT64 = "UINT64"
    # String / binary
    STRING = "STRING"
    ENUM = "ENUM"
    JSON = "JSON"
    BSON = "BSON"
    UUID = "UUID"
    # Temporal
    DATE = "DATE"
    TIME_MILLIS = "TIME_MILLIS"
    TIME_MICROS = "TIME_MICROS"
    TIME_NANOS = "TIME_NANOS"
    TIMESTAMP_MILLIS = "TIMESTAMP_MILLIS"
    TIMESTAMP_MICROS = "TIMESTAMP_MICROS"
    TIMESTAMP_NANOS = "TIMESTAMP_NANOS"
    # Decimal
    DECIMAL = "DECIMAL"


class Repetition(str, Enum):
    REQUIRED = "REQUIRED"
    OPTIONAL = "OPTIONAL"


class LogicalTypeParams(BaseModel):
    """Parameters for logical types that need extra configuration."""
    # DECIMAL
    precision: Optional[int] = None
    scale: Optional[int] = None
    # TIMESTAMP -- whether the timestamp is adjusted to UTC
    is_adjusted_to_utc: Optional[bool] = None


class ColumnConfig(BaseModel):
    """Configuration for a single Parquet column."""
    name: str
    physicalType: PhysicalType = Field(alias="physicalType")
    logicalType: Optional[LogicalType] = Field(default=None, alias="logicalType")
    logicalTypeParams: Optional[LogicalTypeParams] = Field(default=None, alias="logicalTypeParams")
    repetition: Repetition = Repetition.OPTIONAL
    nullRatio: Optional[float] = Field(default=None, alias="nullRatio", ge=0.0, le=1.0)
    # FIXED_LEN_BYTE_ARRAY length
    fixedLength: Optional[int] = Field(default=None, alias="fixedLength", gt=0)

    # LIST element type
    element: Optional[ColumnConfig] = None
    # MAP key/value types
    key: Optional[ColumnConfig] = None
    value: Optional[ColumnConfig] = None
    # STRUCT child fields
    fields: Optional[list[ColumnConfig]] = None

    # Allow specifying exact values for sentinel-collision testing
    fixedValues: Optional[list] = Field(default=None, alias="fixedValues")

    model_config = {"populate_by_name": True}

    @model_validator(mode="after")
    def validate_complex_types(self) -> "ColumnConfig":
        if self.physicalType == PhysicalType.LIST and self.element is None:
            raise ValueError(f"LIST column '{self.name}' must have 'element' defined")
        if self.physicalType == PhysicalType.MAP:
            if self.key is None or self.value is None:
                raise ValueError(f"MAP column '{self.name}' must have 'key' and 'value' defined")
        if self.physicalType == PhysicalType.STRUCT and not self.fields:
            raise ValueError(f"STRUCT column '{self.name}' must have 'fields' defined")
        if self.physicalType == PhysicalType.FIXED_LEN_BYTE_ARRAY and self.fixedLength is None:
            if self.logicalType == LogicalType.UUID:
                self.fixedLength = 16
            elif self.logicalType == LogicalType.DECIMAL:
                # Auto-calculate from precision if not specified
                if self.logicalTypeParams and self.logicalTypeParams.precision:
                    import math
                    self.fixedLength = max(1, math.ceil(self.logicalTypeParams.precision * math.log2(10) / 8) + 1)
                else:
                    raise ValueError(
                        f"FIXED_LEN_BYTE_ARRAY column '{self.name}' must have 'fixedLength' "
                        f"or DECIMAL logicalTypeParams.precision"
                    )
            else:
                raise ValueError(f"FIXED_LEN_BYTE_ARRAY column '{self.name}' must have 'fixedLength'")
        if self.logicalType == LogicalType.DECIMAL:
            if not self.logicalTypeParams or self.logicalTypeParams.precision is None:
                raise ValueError(f"DECIMAL column '{self.name}' requires logicalTypeParams.precision")
            if self.logicalTypeParams.scale is None:
                self.logicalTypeParams.scale = 0
        return self


class WriterOptions(BaseModel):
    """Options for the Parquet writer."""
    compression: str = "SNAPPY"
    rowGroupSize: Optional[int] = Field(default=None, alias="rowGroupSize")
    useDictionary: bool = Field(default=True, alias="useDictionary")
    writeStatistics: bool = Field(default=True, alias="writeStatistics")
    useDeprecatedInt96Timestamps: bool = Field(default=False, alias="useDeprecatedInt96Timestamps")

    model_config = {"populate_by_name": True}


class DatasetConfig(BaseModel):
    """Top-level configuration for a Parquet dataset."""
    name: str
    numRows: int = Field(default=1000, alias="numRows", gt=0)
    seed: int = 42
    defaultNullRatio: float = Field(default=0.1, alias="defaultNullRatio", ge=0.0, le=1.0)
    columns: list[ColumnConfig]
    writerOptions: WriterOptions = Field(default_factory=WriterOptions, alias="writerOptions")

    model_config = {"populate_by_name": True}
