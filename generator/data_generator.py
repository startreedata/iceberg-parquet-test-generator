"""
Type-aware random data generation for all Parquet physical+logical type combinations.

Uses seeded random for reproducibility. Null injection is based on nullRatio
and the column's repetition (REQUIRED columns never get nulls).
"""
from __future__ import annotations

import datetime
import math
import random
import struct
import uuid
from decimal import Decimal
from typing import Any

import pyarrow as pa

from .config_model import (
    ColumnConfig,
    DatasetConfig,
    LogicalType,
    PhysicalType,
    Repetition,
)
from .schema_builder import build_pyarrow_type


def generate_dataset(config: DatasetConfig) -> dict[str, pa.Array]:
    """Generate all columns for a dataset, returning a dict of name -> pa.Array."""
    rng = random.Random(config.seed)
    columns = {}
    for col in config.columns:
        null_ratio = _effective_null_ratio(col, config.defaultNullRatio)
        pa_type = build_pyarrow_type(col)
        columns[col.name] = generate_column(col, pa_type, config.numRows, null_ratio, rng)
    return columns


def generate_column(
    col: ColumnConfig,
    pa_type: pa.DataType,
    num_rows: int,
    null_ratio: float,
    rng: random.Random,
) -> pa.Array:
    """Generate a single column as a PyArrow array."""

    if col.fixedValues is not None:
        return _from_fixed_values(col, pa_type, num_rows, null_ratio, rng)

    pt = col.physicalType
    lt = col.logicalType

    if pt == PhysicalType.LIST:
        return _gen_list(col, pa_type, num_rows, null_ratio, rng)
    if pt == PhysicalType.MAP:
        return _gen_map(col, pa_type, num_rows, null_ratio, rng)
    if pt == PhysicalType.STRUCT:
        return _gen_struct(col, pa_type, num_rows, null_ratio, rng)

    gen_fn = _pick_scalar_generator(pt, lt)
    values = [gen_fn(col, rng) for _ in range(num_rows)]
    mask = _null_mask(num_rows, null_ratio, rng)
    return pa.array(
        [v if not is_null else None for v, is_null in zip(values, mask)],
        type=pa_type,
    )


# ── Scalar generators ────────────────────────────────────────────────

def _gen_bool(_col: ColumnConfig, rng: random.Random) -> bool:
    return rng.choice([True, False])


def _gen_int32(_col: ColumnConfig, rng: random.Random) -> int:
    return rng.randint(-2_147_483_647, 2_147_483_647)


def _gen_int64(_col: ColumnConfig, rng: random.Random) -> int:
    return rng.randint(-2**62, 2**62)


def _gen_float(_col: ColumnConfig, rng: random.Random) -> float:
    return rng.uniform(-1e6, 1e6)


def _gen_double(_col: ColumnConfig, rng: random.Random) -> float:
    return rng.uniform(-1e15, 1e15)


def _gen_binary(_col: ColumnConfig, rng: random.Random) -> bytes:
    length = rng.randint(1, 64)
    return bytes(rng.getrandbits(8) for _ in range(length))


def _gen_fixed(col: ColumnConfig, rng: random.Random) -> bytes:
    n = col.fixedLength or 16
    return bytes(rng.getrandbits(8) for _ in range(n))


def _gen_int96(_col: ColumnConfig, rng: random.Random) -> int:
    # Generate a timestamp value that PyArrow will write as INT96
    # Range: 2000-01-01 to 2030-01-01 in nanoseconds since epoch
    start_ns = 946_684_800_000_000_000
    end_ns = 1_893_456_000_000_000_000
    return rng.randint(start_ns, end_ns)


# ── Logical type generators ──────────────────────────────────────────

def _gen_int8(_col: ColumnConfig, rng: random.Random) -> int:
    return rng.randint(-128, 127)


def _gen_int16(_col: ColumnConfig, rng: random.Random) -> int:
    return rng.randint(-32768, 32767)


def _gen_uint8(_col: ColumnConfig, rng: random.Random) -> int:
    return rng.randint(0, 255)


def _gen_uint16(_col: ColumnConfig, rng: random.Random) -> int:
    return rng.randint(0, 65535)


def _gen_uint32(_col: ColumnConfig, rng: random.Random) -> int:
    return rng.randint(0, 2**32 - 1)


def _gen_uint64(_col: ColumnConfig, rng: random.Random) -> int:
    return rng.randint(0, 2**64 - 1)


def _gen_string(_col: ColumnConfig, rng: random.Random) -> str:
    length = rng.randint(1, 50)
    chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 _-"
    return "".join(rng.choice(chars) for _ in range(length))


def _gen_enum(_col: ColumnConfig, rng: random.Random) -> str:
    return rng.choice(["RED", "GREEN", "BLUE", "YELLOW", "PURPLE", "ORANGE"])


def _gen_json(_col: ColumnConfig, rng: random.Random) -> str:
    import json
    obj = {
        "id": rng.randint(1, 10000),
        "value": round(rng.uniform(0, 100), 2),
        "label": rng.choice(["alpha", "beta", "gamma", "delta"]),
        "active": rng.choice([True, False]),
    }
    return json.dumps(obj)


def _gen_bson(_col: ColumnConfig, rng: random.Random) -> bytes:
    # Minimal valid BSON: 5-byte empty document
    # For testing, generate random binary that represents BSON-like data
    length = rng.randint(5, 64)
    return bytes(rng.getrandbits(8) for _ in range(length))


def _gen_uuid(_col: ColumnConfig, rng: random.Random) -> bytes:
    return uuid.UUID(int=rng.getrandbits(128)).bytes


def _gen_date(_col: ColumnConfig, rng: random.Random) -> datetime.date:
    # Range: 1970-01-01 to 2030-12-31
    ordinal = rng.randint(
        datetime.date(1970, 1, 1).toordinal(),
        datetime.date(2030, 12, 31).toordinal(),
    )
    return datetime.date.fromordinal(ordinal)


def _gen_time_millis(_col: ColumnConfig, rng: random.Random) -> int:
    # Milliseconds since midnight: 0 to 86_399_999
    return rng.randint(0, 86_399_999)


def _gen_time_micros(_col: ColumnConfig, rng: random.Random) -> int:
    # Microseconds since midnight: 0 to 86_399_999_999
    return rng.randint(0, 86_399_999_999)


def _gen_time_nanos(_col: ColumnConfig, rng: random.Random) -> int:
    # Nanoseconds since midnight: 0 to 86_399_999_999_999
    return rng.randint(0, 86_399_999_999_999)


def _gen_timestamp_millis(_col: ColumnConfig, rng: random.Random) -> int:
    # 2000-01-01 to 2030-01-01 in milliseconds since epoch
    return rng.randint(946_684_800_000, 1_893_456_000_000)


def _gen_timestamp_micros(_col: ColumnConfig, rng: random.Random) -> int:
    # 2000-01-01 to 2030-01-01 in microseconds since epoch
    return rng.randint(946_684_800_000_000, 1_893_456_000_000_000)


def _gen_timestamp_nanos(_col: ColumnConfig, rng: random.Random) -> int:
    # 2000-01-01 to 2030-01-01 in nanoseconds since epoch
    return rng.randint(946_684_800_000_000_000, 1_893_456_000_000_000_000)


def _gen_decimal(col: ColumnConfig, rng: random.Random) -> Decimal:
    precision = col.logicalTypeParams.precision
    scale = col.logicalTypeParams.scale or 0
    max_unscaled = 10**precision - 1
    unscaled = rng.randint(-max_unscaled, max_unscaled)
    return Decimal(unscaled).scaleb(-scale)


# ── Generator dispatch ───────────────────────────────────────────────

_LOGICAL_GENERATORS = {
    LogicalType.INT8:             _gen_int8,
    LogicalType.INT16:            _gen_int16,
    LogicalType.INT32:            _gen_int32,
    LogicalType.INT64:            _gen_int64,
    LogicalType.UINT8:            _gen_uint8,
    LogicalType.UINT16:           _gen_uint16,
    LogicalType.UINT32:           _gen_uint32,
    LogicalType.UINT64:           _gen_uint64,
    LogicalType.STRING:           _gen_string,
    LogicalType.ENUM:             _gen_enum,
    LogicalType.JSON:             _gen_json,
    LogicalType.BSON:             _gen_bson,
    LogicalType.UUID:             _gen_uuid,
    LogicalType.DATE:             _gen_date,
    LogicalType.TIME_MILLIS:      _gen_time_millis,
    LogicalType.TIME_MICROS:      _gen_time_micros,
    LogicalType.TIME_NANOS:       _gen_time_nanos,
    LogicalType.TIMESTAMP_MILLIS: _gen_timestamp_millis,
    LogicalType.TIMESTAMP_MICROS: _gen_timestamp_micros,
    LogicalType.TIMESTAMP_NANOS:  _gen_timestamp_nanos,
    LogicalType.DECIMAL:          _gen_decimal,
}

_PHYSICAL_GENERATORS = {
    PhysicalType.BOOLEAN:              _gen_bool,
    PhysicalType.INT32:                _gen_int32,
    PhysicalType.INT64:                _gen_int64,
    PhysicalType.FLOAT:                _gen_float,
    PhysicalType.DOUBLE:               _gen_double,
    PhysicalType.BINARY:               _gen_binary,
    PhysicalType.FIXED_LEN_BYTE_ARRAY: _gen_fixed,
    PhysicalType.INT96:                _gen_int96,
}


def _pick_scalar_generator(pt: PhysicalType, lt: LogicalType | None):
    """Select the appropriate generator function for a scalar column."""
    if lt is not None:
        gen = _LOGICAL_GENERATORS.get(lt)
        if gen is None:
            raise ValueError(f"No generator for logical type {lt}")
        return gen
    gen = _PHYSICAL_GENERATORS.get(pt)
    if gen is None:
        raise ValueError(f"No generator for physical type {pt}")
    return gen


# ── Complex type generators ──────────────────────────────────────────

def _gen_list(
    col: ColumnConfig,
    pa_type: pa.DataType,
    num_rows: int,
    null_ratio: float,
    rng: random.Random,
) -> pa.Array:
    """Generate a LIST column."""
    elem_col = col.element
    elem_null_ratio = _effective_null_ratio(elem_col, 0.0)
    elem_pa_type = pa_type.value_type
    is_complex_elem = elem_col.physicalType in (PhysicalType.LIST, PhysicalType.MAP, PhysicalType.STRUCT)
    elem_gen = None if is_complex_elem else _pick_scalar_generator(elem_col.physicalType, elem_col.logicalType)

    mask = _null_mask(num_rows, null_ratio, rng)
    lists = []
    for is_null in mask:
        if is_null:
            lists.append(None)
        else:
            length = rng.randint(0, 5)
            if is_complex_elem:
                sub_arr = generate_column(elem_col, elem_pa_type, length, elem_null_ratio, rng)
                lists.append(sub_arr.to_pylist())
            else:
                elem_mask = _null_mask(length, elem_null_ratio, rng)
                elems = []
                for _j, e_null in enumerate(elem_mask):
                    if e_null:
                        elems.append(None)
                    else:
                        elems.append(elem_gen(elem_col, rng))
                lists.append(elems)
    return pa.array(lists, type=pa_type)


def _gen_map(
    col: ColumnConfig,
    pa_type: pa.DataType,
    num_rows: int,
    null_ratio: float,
    rng: random.Random,
) -> pa.Array:
    """Generate a MAP column."""
    key_col = col.key
    val_col = col.value
    key_gen = _pick_scalar_generator(key_col.physicalType, key_col.logicalType)
    is_complex_val = val_col.physicalType in (PhysicalType.LIST, PhysicalType.MAP, PhysicalType.STRUCT)
    val_gen = None if is_complex_val else _pick_scalar_generator(val_col.physicalType, val_col.logicalType)
    val_null_ratio = _effective_null_ratio(val_col, 0.0)
    val_pa_type = pa_type.item_type if hasattr(pa_type, "item_type") else None

    mask = _null_mask(num_rows, null_ratio, rng)
    maps = []
    for is_null in mask:
        if is_null:
            maps.append(None)
        else:
            length = rng.randint(0, 5)
            keys = [key_gen(key_col, rng) for _ in range(length)]
            # Ensure unique keys for string-keyed maps
            if key_col.logicalType == LogicalType.STRING or key_col.physicalType == PhysicalType.BINARY:
                seen = set()
                unique_keys = []
                for k in keys:
                    k_hashable = k if isinstance(k, (str, int, float, bool)) else str(k)
                    if k_hashable not in seen:
                        seen.add(k_hashable)
                        unique_keys.append(k)
                keys = unique_keys
                length = len(keys)

            if is_complex_val and length > 0:
                val_arr = generate_column(val_col, val_pa_type, length, val_null_ratio, rng)
                vals = val_arr.to_pylist()
            else:
                val_mask = _null_mask(length, val_null_ratio, rng)
                vals = []
                for _j, v_null in enumerate(val_mask):
                    if v_null:
                        vals.append(None)
                    else:
                        vals.append(val_gen(val_col, rng))
            maps.append(list(zip(keys, vals)))
    return pa.array(maps, type=pa_type)


def _gen_struct(
    col: ColumnConfig,
    pa_type: pa.DataType,
    num_rows: int,
    null_ratio: float,
    rng: random.Random,
) -> pa.Array:
    """Generate a STRUCT column."""
    mask = _null_mask(num_rows, null_ratio, rng)

    # Generate each child field independently
    child_arrays = {}
    for field_conf in col.fields:
        field_null_ratio = _effective_null_ratio(field_conf, 0.0)
        field_pa_type = build_pyarrow_type(field_conf)
        child_arrays[field_conf.name] = generate_column(
            field_conf, field_pa_type, num_rows, field_null_ratio, rng,
        )

    # Build struct rows
    structs = []
    for i in range(num_rows):
        if mask[i]:
            structs.append(None)
        else:
            row = {}
            for field_conf in col.fields:
                val = child_arrays[field_conf.name][i].as_py()
                row[field_conf.name] = val
            structs.append(row)
    return pa.array(structs, type=pa_type)


# ── Utilities ────────────────────────────────────────────────────────

def _null_mask(num_rows: int, null_ratio: float, rng: random.Random) -> list[bool]:
    """Generate a boolean mask where True means null."""
    if null_ratio <= 0:
        return [False] * num_rows
    return [rng.random() < null_ratio for _ in range(num_rows)]


def _effective_null_ratio(col: ColumnConfig, default: float) -> float:
    """Get the effective null ratio for a column."""
    if col.repetition == Repetition.REQUIRED:
        return 0.0
    if col.nullRatio is not None:
        return col.nullRatio
    return default


def _from_fixed_values(
    col: ColumnConfig,
    pa_type: pa.DataType,
    num_rows: int,
    null_ratio: float,
    rng: random.Random,
) -> pa.Array:
    """Generate a column using user-supplied fixed values (for sentinel-collision testing)."""
    values = col.fixedValues
    mask = _null_mask(num_rows, null_ratio, rng)
    result = []
    for i in range(num_rows):
        if mask[i]:
            result.append(None)
        else:
            result.append(values[i % len(values)])
    return pa.array(result, type=pa_type)
