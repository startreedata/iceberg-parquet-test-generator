"""
Generate Pinot schema and table config JSON from a DatasetConfig.

Mapping logic mirrors ParquetToPinotTypeMapper (direct Parquet ingestion path)
with additional awareness of IcebergSchemaConverter behavior for complex types.

Two ingestion paths produce slightly different mappings:
  - "parquet" (ParquetToPinotTypeMapper): DECIMAL -> STRING, DATE -> LONG
  - "iceberg" (IcebergSchemaConverter):   DECIMAL -> BIG_DECIMAL, list<prim> -> MV,
                                          simple map -> MAP ComplexFieldSpec

This module defaults to "parquet" since the project uses raw S3 catalog table type
(Parquet files in S3 without Iceberg catalog metadata).
"""
from __future__ import annotations

import json
import os
from enum import Enum
from typing import Any

from .config_model import (
    ColumnConfig,
    DatasetConfig,
    LogicalType,
    PhysicalType,
)


class MappingMode(str, Enum):
    PARQUET = "parquet"
    ICEBERG = "iceberg"


# ---------------------------------------------------------------------------
# Pinot type mapping
# ---------------------------------------------------------------------------

_PARQUET_PRIMITIVE_MAP: dict[PhysicalType, str] = {
    PhysicalType.BOOLEAN: "BOOLEAN",
    PhysicalType.INT32: "INT",
    PhysicalType.INT64: "LONG",
    PhysicalType.FLOAT: "FLOAT",
    PhysicalType.DOUBLE: "DOUBLE",
    PhysicalType.BINARY: "STRING",
    PhysicalType.FIXED_LEN_BYTE_ARRAY: "STRING",
    PhysicalType.INT96: "TIMESTAMP",
}

_PARQUET_LOGICAL_MAP: dict[LogicalType, str] = {
    LogicalType.INT8: "INT",
    LogicalType.INT16: "INT",
    LogicalType.INT32: "INT",
    LogicalType.INT64: "LONG",
    LogicalType.UINT8: "INT",
    LogicalType.UINT16: "INT",
    LogicalType.UINT32: "LONG",
    LogicalType.UINT64: "LONG",
    LogicalType.STRING: "STRING",
    LogicalType.ENUM: "STRING",
    LogicalType.JSON: "STRING",
    LogicalType.BSON: "BYTES",
    LogicalType.UUID: "STRING",
    LogicalType.DATE: "LONG",
    LogicalType.TIME_MILLIS: "INT",
    LogicalType.TIME_MICROS: "LONG",
    LogicalType.TIME_NANOS: "LONG",
    LogicalType.TIMESTAMP_MILLIS: "TIMESTAMP",
    LogicalType.TIMESTAMP_MICROS: "TIMESTAMP",
    LogicalType.TIMESTAMP_NANOS: "TIMESTAMP",
    LogicalType.DECIMAL: "BIG_DECIMAL",
}

_ICEBERG_LOGICAL_OVERRIDES: dict[LogicalType, str] = {
    LogicalType.JSON: "JSON",
}


def _map_primitive_pinot_type(col: ColumnConfig, mode: MappingMode) -> str:
    """Resolve a primitive column to its Pinot DataType string."""
    if col.logicalType is not None:
        base = _PARQUET_LOGICAL_MAP.get(col.logicalType)
        if mode == MappingMode.ICEBERG and col.logicalType in _ICEBERG_LOGICAL_OVERRIDES:
            base = _ICEBERG_LOGICAL_OVERRIDES[col.logicalType]
        if base is not None:
            return base

    return _PARQUET_PRIMITIVE_MAP.get(col.physicalType, "STRING")


def _is_primitive_element(col: ColumnConfig) -> bool:
    """True if the column represents a scalar (non-complex) type."""
    return col.physicalType not in (PhysicalType.LIST, PhysicalType.MAP, PhysicalType.STRUCT)


def _is_timestamp_type(col: ColumnConfig) -> bool:
    """INT96 is excluded: it's deprecated, has no Parquet column statistics,
    and causes IcebergWatcher to fail when used as timeColumnName."""
    return col.logicalType in (
        LogicalType.TIMESTAMP_MILLIS,
        LogicalType.TIMESTAMP_MICROS,
        LogicalType.TIMESTAMP_NANOS,
    )


def _timestamp_format_spec(col: ColumnConfig) -> tuple[str, str]:
    """Return (format, granularity) for a DateTimeFieldSpec."""
    if col.logicalType == LogicalType.TIMESTAMP_MILLIS or col.physicalType == PhysicalType.INT96:
        return "1:MILLISECONDS:EPOCH", "1:MILLISECONDS"
    if col.logicalType == LogicalType.TIMESTAMP_MICROS:
        return "1:MICROSECONDS:EPOCH", "1:MICROSECONDS"
    if col.logicalType == LogicalType.TIMESTAMP_NANOS:
        return "1:NANOSECONDS:EPOCH", "1:NANOSECONDS"
    return "1:MILLISECONDS:EPOCH", "1:MILLISECONDS"


# ---------------------------------------------------------------------------
# Complex type classification
# ---------------------------------------------------------------------------

class _FieldKind(str, Enum):
    DIMENSION_SV = "dimension_sv"
    DIMENSION_MV = "dimension_mv"
    DATETIME = "datetime"
    METRIC = "metric"
    JSON = "json"
    MAP = "map"


def _classify_column(col: ColumnConfig, mode: MappingMode) -> _FieldKind:
    """Determine how a column should appear in the Pinot schema."""
    if col.physicalType == PhysicalType.STRUCT:
        return _FieldKind.JSON

    if col.physicalType == PhysicalType.LIST:
        if mode == MappingMode.ICEBERG and col.element and _is_primitive_element(col.element):
            return _FieldKind.DIMENSION_MV
        return _FieldKind.JSON

    if col.physicalType == PhysicalType.MAP:
        if (
            mode == MappingMode.ICEBERG
            and col.key
            and col.value
            and _is_primitive_element(col.key)
            and _is_primitive_element(col.value)
        ):
            return _FieldKind.MAP
        return _FieldKind.JSON

    if _is_timestamp_type(col):
        return _FieldKind.DATETIME

    return _FieldKind.DIMENSION_SV


# ---------------------------------------------------------------------------
# Schema generation
# ---------------------------------------------------------------------------

def _build_dimension_field(col: ColumnConfig, pinot_type: str, single_value: bool) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "name": col.name,
        "dataType": pinot_type,
        "singleValueField": single_value,
    }
    if not single_value:
        spec["maxLength"] = 512
    if col.repetition.value == "OPTIONAL":
        spec["notNull"] = False
    return spec


def _build_datetime_field(col: ColumnConfig, mode: MappingMode) -> dict[str, Any]:
    fmt, gran = _timestamp_format_spec(col)
    pinot_type = _map_primitive_pinot_type(col, mode)
    return {
        "name": col.name,
        "dataType": pinot_type,
        "format": fmt,
        "granularity": gran,
    }


def _build_json_field(col: ColumnConfig) -> dict[str, Any]:
    return {
        "name": col.name,
        "dataType": "JSON",
        "singleValueField": True,
    }


def _build_map_field(col: ColumnConfig, mode: MappingMode) -> dict[str, Any]:
    """Build a MAP ComplexFieldSpec for simple map<primitive, primitive>."""
    key_type = _map_primitive_pinot_type(col.key, mode) if col.key else "STRING"
    val_type = _map_primitive_pinot_type(col.value, mode) if col.value else "STRING"
    return {
        "name": col.name,
        "dataType": "MAP",
        "singleValueField": True,
        "childFieldSpecs": {
            "key": {"name": "key", "dataType": key_type},
            "value": {"name": "value", "dataType": val_type},
        },
    }


def generate_pinot_schema(config: DatasetConfig, mode: MappingMode = MappingMode.PARQUET) -> dict[str, Any]:
    """
    Convert a DatasetConfig into a Pinot schema JSON dict.

    Returns a dict matching Pinot's Schema JSON format with
    dimensionFieldSpecs, metricFieldSpecs, dateTimeFieldSpecs, and
    complexFieldSpecs.
    """
    dimensions: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []
    date_times: list[dict[str, Any]] = []
    complex_fields: list[dict[str, Any]] = []

    for col in config.columns:
        kind = _classify_column(col, mode)

        if kind == _FieldKind.DATETIME:
            date_times.append(_build_datetime_field(col, mode))

        elif kind == _FieldKind.DIMENSION_SV:
            pinot_type = _map_primitive_pinot_type(col, mode)
            dimensions.append(_build_dimension_field(col, pinot_type, single_value=True))

        elif kind == _FieldKind.DIMENSION_MV:
            elem_type = _map_primitive_pinot_type(col.element, mode) if col.element else "STRING"
            dimensions.append(_build_dimension_field(col, elem_type, single_value=False))

        elif kind == _FieldKind.JSON:
            dimensions.append(_build_json_field(col))

        elif kind == _FieldKind.MAP:
            complex_fields.append(_build_map_field(col, mode))

    schema: dict[str, Any] = {
        "schemaName": config.name,
        "enableColumnBasedNullHandling": True,
    }
    if dimensions:
        schema["dimensionFieldSpecs"] = dimensions
    if metrics:
        schema["metricFieldSpecs"] = metrics
    if date_times:
        schema["dateTimeFieldSpecs"] = date_times
    if complex_fields:
        schema["complexFieldSpecs"] = complex_fields

    return schema


# ---------------------------------------------------------------------------
# Table config generation
# ---------------------------------------------------------------------------

def _find_time_column(config: DatasetConfig) -> str | None:
    """Pick the best timestamp column for timeColumnName.

    Prefers REQUIRED columns over OPTIONAL to avoid null-related issues,
    and prefers MILLIS over MICROS/NANOS to stay within Pinot's accepted
    time range (1971–2071).
    """
    candidates = [col for col in config.columns if _is_timestamp_type(col)]
    if not candidates:
        return None
    required = [c for c in candidates if c.repetition.value == "REQUIRED"]
    if required:
        millis = [c for c in required if c.logicalType == LogicalType.TIMESTAMP_MILLIS]
        return millis[0].name if millis else required[0].name
    millis = [c for c in candidates if c.logicalType == LogicalType.TIMESTAMP_MILLIS]
    return millis[0].name if millis else candidates[0].name


def generate_pinot_table_config(
    config: DatasetConfig,
    mode: MappingMode = MappingMode.PARQUET,
    table_type: str = "OFFLINE",
    replication: int = 1,
) -> dict[str, Any]:
    """
    Generate a Pinot table config JSON dict for a given DatasetConfig.

    Produces a minimal but functional OFFLINE table config suitable for
    local dev testing or segment upload.
    """
    table_name = config.name
    time_col = _find_time_column(config)

    segments_config: dict[str, Any] = {
        "replication": str(replication),
        "schemaName": table_name,
    }
    if time_col:
        segments_config["timeColumnName"] = time_col

    table_index_config: dict[str, Any] = {
        "loadMode": "MMAP",
        "nullHandlingEnabled": True,
    }

    ingestion_config: dict[str, Any] = {
        "batchIngestionConfig": {
            "segmentIngestionType": "APPEND",
            "segmentIngestionFrequency": "DAILY",
        },
    }

    table_config: dict[str, Any] = {
        "tableName": f"{table_name}_{table_type}",
        "tableType": table_type,
        "segmentsConfig": segments_config,
        "tableIndexConfig": table_index_config,
        "tenants": {
            "broker": "DefaultTenant",
            "server": "DefaultTenant",
        },
        "ingestionConfig": ingestion_config,
        "metadata": {
            "customConfigs": {},
        },
    }

    return table_config


# ---------------------------------------------------------------------------
# Writer helpers
# ---------------------------------------------------------------------------

def write_pinot_configs(
    config: DatasetConfig,
    output_dir: str,
    mode: MappingMode = MappingMode.PARQUET,
) -> tuple[str, str]:
    """
    Generate and write both Pinot schema and table config to output_dir.

    Returns (schema_path, table_config_path).
    """
    os.makedirs(output_dir, exist_ok=True)

    schema = generate_pinot_schema(config, mode)
    schema_path = os.path.join(output_dir, f"{config.name}_pinot_schema.json")
    with open(schema_path, "w") as f:
        json.dump(schema, f, indent=2)

    table_config = generate_pinot_table_config(config, mode)
    table_config_path = os.path.join(output_dir, f"{config.name}_pinot_table_config.json")
    with open(table_config_path, "w") as f:
        json.dump(table_config, f, indent=2)

    _print_mapping_summary(config, mode)

    return schema_path, table_config_path


def _print_mapping_summary(config: DatasetConfig, mode: MappingMode) -> None:
    """Print a human-readable summary of the type mapping decisions."""
    print(f"\n{'=' * 60}")
    print(f"  Pinot Config Generation (mode={mode.value})")
    print(f"  Schema: {config.name}")
    print(f"{'=' * 60}")

    for col in config.columns:
        kind = _classify_column(col, mode)
        if kind == _FieldKind.DIMENSION_SV:
            pinot_type = _map_primitive_pinot_type(col, mode)
            print(f"  {col.name}: {col.physicalType.value}"
                  f"{'+' + col.logicalType.value if col.logicalType else ''}"
                  f" -> {pinot_type} (dimension, SV)")
        elif kind == _FieldKind.DIMENSION_MV:
            elem_type = _map_primitive_pinot_type(col.element, mode) if col.element else "STRING"
            print(f"  {col.name}: LIST<{col.element.physicalType.value if col.element else '?'}>"
                  f" -> {elem_type} (dimension, MV)")
        elif kind == _FieldKind.DATETIME:
            pinot_type = _map_primitive_pinot_type(col, mode)
            fmt, _ = _timestamp_format_spec(col)
            print(f"  {col.name}: {col.physicalType.value}"
                  f"{'+' + col.logicalType.value if col.logicalType else ''}"
                  f" -> {pinot_type} (dateTime, {fmt})")
        elif kind == _FieldKind.JSON:
            label = col.physicalType.value
            if col.physicalType == PhysicalType.LIST and col.element:
                label = f"LIST<{col.element.physicalType.value}>"
            elif col.physicalType == PhysicalType.MAP and col.key and col.value:
                label = f"MAP<{col.key.physicalType.value},{col.value.physicalType.value}>"
            print(f"  {col.name}: {label} -> JSON (dimension, SV)")
        elif kind == _FieldKind.MAP:
            key_t = _map_primitive_pinot_type(col.key, mode) if col.key else "STRING"
            val_t = _map_primitive_pinot_type(col.value, mode) if col.value else "STRING"
            print(f"  {col.name}: MAP<{col.key.physicalType.value if col.key else '?'},"
                  f"{col.value.physicalType.value if col.value else '?'}>"
                  f" -> MAP<{key_t},{val_t}> (complexField)")

    print()
