"""
Converts a DatasetConfig (JSON config) into a PyArrow schema.

Maps every physicalType + logicalType combination to the correct pa.DataType,
handling temporal units, decimal precision, and complex (STRUCT/LIST/MAP) types.
"""
from __future__ import annotations

import pyarrow as pa

from .config_model import ColumnConfig, DatasetConfig, LogicalType, PhysicalType


def build_pyarrow_type(col: ColumnConfig) -> pa.DataType:
    """Convert a single ColumnConfig into a PyArrow DataType."""

    pt = col.physicalType
    lt = col.logicalType
    params = col.logicalTypeParams

    # ── Complex types ────────────────────────────────────────────────
    if pt == PhysicalType.LIST:
        elem_type = build_pyarrow_type(col.element)
        return pa.list_(elem_type)

    if pt == PhysicalType.MAP:
        key_type = build_pyarrow_type(col.key)
        val_type = build_pyarrow_type(col.value)
        return pa.map_(key_type, val_type)

    if pt == PhysicalType.STRUCT:
        fields = []
        for f in col.fields:
            nullable = f.repetition.value == "OPTIONAL"
            fields.append(pa.field(f.name, build_pyarrow_type(f), nullable=nullable))
        return pa.struct(fields)

    # ── Logical types on primitives ──────────────────────────────────
    if lt is not None:
        return _resolve_logical_type(pt, lt, params, col.fixedLength)

    # ── Bare physical types (no logical annotation) ──────────────────
    return _PHYSICAL_TYPE_MAP[pt](col)


def _resolve_logical_type(
    pt: PhysicalType,
    lt: LogicalType,
    params,
    fixed_length: int | None,
) -> pa.DataType:
    """Map a physicalType + logicalType pair to a PyArrow type."""

    # Integer subtypes
    _INT_MAP = {
        LogicalType.INT8:   pa.int8(),
        LogicalType.INT16:  pa.int16(),
        LogicalType.INT32:  pa.int32(),
        LogicalType.INT64:  pa.int64(),
        LogicalType.UINT8:  pa.uint8(),
        LogicalType.UINT16: pa.uint16(),
        LogicalType.UINT32: pa.uint32(),
        LogicalType.UINT64: pa.uint64(),
    }
    if lt in _INT_MAP:
        return _INT_MAP[lt]

    # String / binary
    if lt == LogicalType.STRING:
        return pa.utf8()
    if lt == LogicalType.ENUM:
        return pa.utf8()
    if lt == LogicalType.JSON:
        return pa.utf8()
    if lt == LogicalType.BSON:
        return pa.binary()
    if lt == LogicalType.UUID:
        return pa.binary(16)
    if lt in (LogicalType.GEOMETRY, LogicalType.GEOGRAPHY):
        return pa.binary()

    # Temporal
    if lt == LogicalType.DATE:
        return pa.date32()
    if lt == LogicalType.TIME_MILLIS:
        return pa.time32("ms")
    if lt == LogicalType.TIME_MICROS:
        return pa.time64("us")
    if lt == LogicalType.TIME_NANOS:
        return pa.time64("ns")

    utc = params.is_adjusted_to_utc if params else None
    tz = "UTC" if utc else None

    if lt == LogicalType.TIMESTAMP_MILLIS:
        return pa.timestamp("ms", tz=tz)
    if lt == LogicalType.TIMESTAMP_MICROS:
        return pa.timestamp("us", tz=tz)
    if lt == LogicalType.TIMESTAMP_NANOS:
        return pa.timestamp("ns", tz=tz)

    # Decimal
    if lt == LogicalType.DECIMAL:
        precision = params.precision
        scale = params.scale if params.scale is not None else 0
        return pa.decimal128(precision, scale)

    raise ValueError(f"Unsupported logical type: {lt} on physical type {pt}")


def _bare_boolean(_col: ColumnConfig) -> pa.DataType:
    return pa.bool_()

def _bare_int32(_col: ColumnConfig) -> pa.DataType:
    return pa.int32()

def _bare_int64(_col: ColumnConfig) -> pa.DataType:
    return pa.int64()

def _bare_float(_col: ColumnConfig) -> pa.DataType:
    return pa.float32()

def _bare_double(_col: ColumnConfig) -> pa.DataType:
    return pa.float64()

def _bare_binary(_col: ColumnConfig) -> pa.DataType:
    return pa.binary()

def _bare_fixed(col: ColumnConfig) -> pa.DataType:
    return pa.binary(col.fixedLength)

def _bare_int96(_col: ColumnConfig) -> pa.DataType:
    # INT96 is stored as pa.timestamp('ns') and written with
    # use_deprecated_int96_timestamps=True at write time
    return pa.timestamp("ns")


_PHYSICAL_TYPE_MAP = {
    PhysicalType.BOOLEAN:               _bare_boolean,
    PhysicalType.INT32:                  _bare_int32,
    PhysicalType.INT64:                  _bare_int64,
    PhysicalType.FLOAT:                  _bare_float,
    PhysicalType.DOUBLE:                 _bare_double,
    PhysicalType.BINARY:                 _bare_binary,
    PhysicalType.FIXED_LEN_BYTE_ARRAY:  _bare_fixed,
    PhysicalType.INT96:                  _bare_int96,
}


def build_pyarrow_schema(config: DatasetConfig) -> pa.Schema:
    """Build a complete PyArrow schema from a DatasetConfig."""
    fields = []
    for col in config.columns:
        nullable = col.repetition.value == "OPTIONAL"
        pa_type = build_pyarrow_type(col)
        fields.append(pa.field(col.name, pa_type, nullable=nullable))
    return pa.schema(fields)
