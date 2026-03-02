# Parquet Data Types -- Complete Reference

This document covers every Parquet data type, how it maps through Iceberg, and how it lands in Pinot.

---

## Type System Overview

Parquet uses a layered type system:

1. **Physical types** -- the actual storage encoding in column chunks (8 types)
2. **Logical types** -- semantic annotations on physical types that define how bytes should be interpreted
3. **Complex types** -- group-based structures (STRUCT, LIST, MAP)

Logical types have two representations:
- **`LogicalTypeAnnotation`** (modern, since Parquet 2.4) -- typed classes with parameters
- **`OriginalType`** (legacy) -- flat enum, no parameters beyond what the physical type carries

---

## 1. Physical (Primitive) Types

These are the 8 storage types. Every Parquet column ultimately stores data as one of these.

| Physical Type | Size | Description | PyArrow Type | Default Pinot Mapping |
|---|---|---|---|---|
| **BOOLEAN** | 1 bit | true / false | `pa.bool_()` | BOOLEAN |
| **INT32** | 32 bits | Signed 32-bit integer | `pa.int32()` | INT |
| **INT64** | 64 bits | Signed 64-bit integer | `pa.int64()` | LONG |
| **FLOAT** | 32 bits | IEEE 754 single-precision | `pa.float32()` | FLOAT |
| **DOUBLE** | 64 bits | IEEE 754 double-precision | `pa.float64()` | DOUBLE |
| **BINARY** | variable | Arbitrary byte sequence | `pa.binary()` | STRING |
| **FIXED_LEN_BYTE_ARRAY** | fixed N bytes | Fixed-length byte sequence | `pa.binary(N)` | STRING |
| **INT96** | 96 bits | **Deprecated.** 12-byte nano timestamp (Hive/Impala legacy) | `pa.timestamp('ns')` with `use_deprecated_int96_timestamps=True` | TIMESTAMP |

### Notes on INT96

INT96 is deprecated in the Parquet spec but still widely encountered in legacy Hive/Impala data. It stores a Julian day number (4 bytes) + nanoseconds within the day (8 bytes). Modern writers should use INT64 TIMESTAMP instead.

---

## 2. Logical Types (Modern -- LogicalTypeAnnotation)

Logical types annotate physical types to give semantic meaning.

### 2.1 Integer Subtypes

All stored on INT32 or INT64. Control bit-width and signedness.

| Logical Type | Physical Type | Bit Width | Signed | PyArrow Type | Iceberg Type | Pinot Mapping |
|---|---|---|---|---|---|---|
| INT(8, true) | INT32 | 8 | yes | `pa.int8()` | N/A | INT |
| INT(16, true) | INT32 | 16 | yes | `pa.int16()` | N/A | INT |
| INT(32, true) | INT32 | 32 | yes | `pa.int32()` | `int` | INT |
| INT(64, true) | INT64 | 64 | yes | `pa.int64()` | `long` | LONG |
| INT(8, false) | INT32 | 8 | no | `pa.uint8()` | N/A | INT |
| INT(16, false) | INT32 | 16 | no | `pa.uint16()` | N/A | INT |
| INT(32, false) | INT32 | 32 | no | `pa.uint32()` | N/A | LONG |
| INT(64, false) | INT64 | 64 | no | `pa.uint64()` | N/A | LONG |

**Edge case:** UINT32 maps to Pinot LONG (not INT) to avoid overflow since Pinot INT is signed 32-bit.

### 2.2 String and Binary Types

| Logical Type | Physical Type | PyArrow Type | Iceberg Type | Pinot Mapping |
|---|---|---|---|---|
| STRING | BINARY | `pa.string()` / `pa.utf8()` | `string` | STRING |
| ENUM | BINARY | `pa.string()` | N/A | STRING |
| JSON | BINARY | `pa.string()` | N/A | STRING |
| BSON | BINARY | `pa.binary()` | N/A | BYTES |
| UUID | FIXED_LEN_BYTE_ARRAY(16) | `pa.binary(16)` | `uuid` | STRING |

**Note:** Parquet JSON logical type is a semantic annotation on BINARY. It tells readers the bytes are valid JSON. In Pinot, this maps to STRING (Pinot has its own JSON indexing on STRING columns). Iceberg does not have a native JSON type; JSON data is stored as `string`.

### 2.3 Temporal Types

| Logical Type | Physical Type | Unit | UTC-adjusted? | PyArrow Type | Iceberg Type | Pinot Mapping |
|---|---|---|---|---|---|---|
| DATE | INT32 | days since epoch | N/A | `pa.date32()` | `date` | LONG |
| TIME(MILLIS) | INT32 | ms since midnight | isAdjustedToUTC | `pa.time32('ms')` | `time` | INT |
| TIME(MICROS) | INT64 | us since midnight | isAdjustedToUTC | `pa.time64('us')` | `time` | LONG |
| TIME(NANOS) | INT64 | ns since midnight | isAdjustedToUTC | `pa.time64('ns')` | N/A | LONG |
| TIMESTAMP(MILLIS) | INT64 | ms since epoch | isAdjustedToUTC | `pa.timestamp('ms')` or `pa.timestamp('ms', tz='UTC')` | `timestamp` / `timestamptz` | TIMESTAMP |
| TIMESTAMP(MICROS) | INT64 | us since epoch | isAdjustedToUTC | `pa.timestamp('us')` or `pa.timestamp('us', tz='UTC')` | `timestamp` / `timestamptz` | TIMESTAMP |
| TIMESTAMP(NANOS) | INT64 | ns since epoch | isAdjustedToUTC | `pa.timestamp('ns')` or `pa.timestamp('ns', tz='UTC')` | N/A | TIMESTAMP |

**Iceberg defaults:**
- `timestamp` (without timezone) -> `TIMESTAMP(MICROS, isAdjustedToUTC=false)`
- `timestamptz` (with timezone) -> `TIMESTAMP(MICROS, isAdjustedToUTC=true)`
- Iceberg always uses **microsecond** precision, not millisecond

**Edge case:** DATE maps to Pinot LONG (epoch days as a long value), not INT, even though the underlying physical type is INT32. This is a deliberate widening in `ParquetToPinotTypeMapper`.

### 2.4 Decimal Type

DECIMAL can be stored on multiple physical types depending on precision:

| Physical Type | Precision Range | PyArrow Type | Iceberg Type | Pinot Mapping |
|---|---|---|---|---|
| INT32 | 1-9 | `pa.decimal128(P, S)` | `decimal(P, S)` | BIG_DECIMAL |
| INT64 | 1-18 | `pa.decimal128(P, S)` | `decimal(P, S)` | BIG_DECIMAL |
| FIXED_LEN_BYTE_ARRAY(N) | 1-38 | `pa.decimal128(P, S)` | `decimal(P, S)` | BIG_DECIMAL |
| BINARY | 1-38 | `pa.decimal128(P, S)` | `decimal(P, S)` | BIG_DECIMAL |

**Iceberg** stores decimal as `decimal(precision, scale)` and uses FIXED_LEN_BYTE_ARRAY in Parquet.

**Pinot** maps DECIMAL to BIG_DECIMAL in both `ParquetToPinotTypeMapper` and `IcebergSchemaConverter`.

**Edge case:** The required byte length for FIXED_LEN_BYTE_ARRAY is `ceil(precision * log2(10) / 8) + 1` but PyArrow handles this automatically.

### 2.5 Geospatial Types (Extension)

| Logical Type | Physical Type | PyArrow Type | Pinot Mapping |
|---|---|---|---|
| GEOMETRY | BINARY | `pa.binary()` | BYTES |
| GEOGRAPHY | BINARY | `pa.binary()` | BYTES |

These are Parquet extension types, not part of the core spec. Stored as WKB (Well-Known Binary).

### 2.6 Other Logical Types

| Logical Type | Physical Type | PyArrow Type | Pinot Mapping |
|---|---|---|---|
| INTERVAL | FIXED_LEN_BYTE_ARRAY(12) | `pa.month_day_nano_interval()` | STRING |

---

## 3. Legacy Logical Types (OriginalType)

The `OriginalType` enum was the original way to annotate types. Modern Parquet uses `LogicalTypeAnnotation` instead, but legacy files still contain these.

| OriginalType | Modern Equivalent | Physical Type | Pinot Mapping |
|---|---|---|---|
| INT_8 | INT(8, true) | INT32 | INT |
| INT_16 | INT(16, true) | INT32 | INT |
| INT_32 | INT(32, true) | INT32 | INT |
| INT_64 | INT(64, true) | INT64 | LONG |
| UINT_8 | INT(8, false) | INT32 | INT |
| UINT_16 | INT(16, false) | INT32 | INT |
| UINT_32 | INT(32, false) | INT32 | LONG |
| UINT_64 | INT(64, false) | INT64 | LONG |
| UTF8 | STRING | BINARY | STRING |
| ENUM | ENUM | BINARY | STRING |
| DATE | DATE | INT32 | LONG |
| TIME_MILLIS | TIME(MILLIS) | INT32 | INT |
| TIME_MICROS | TIME(MICROS) | INT64 | LONG |
| TIMESTAMP_MILLIS | TIMESTAMP(MILLIS, true) | INT64 | TIMESTAMP |
| TIMESTAMP_MICROS | TIMESTAMP(MICROS, true) | INT64 | TIMESTAMP |
| DECIMAL | DECIMAL(P, S) | INT32/INT64/BINARY/FIXED | BIG_DECIMAL |
| JSON | JSON | BINARY | STRING |
| BSON | BSON | BINARY | BYTES |
| INTERVAL | N/A (no modern equivalent) | FIXED_LEN_BYTE_ARRAY(12) | STRING |

---

## 4. Complex Types (Group Types)

Complex types use Parquet's group (nested) structure.

### 4.1 STRUCT

A group without a logical type annotation. Each child field is a named column.

```
message schema {
  optional group profile {
    optional binary name (STRING);
    optional int32 age;
    optional double score;
  }
}
```

**Iceberg:** `struct<name: string, age: int, score: double>`
**Pinot:** STRING (serialized as JSON) or JSON type

### 4.2 LIST (3-Level Structure)

The standard Parquet list encoding uses three levels:

```
message schema {
  optional group tags (LIST) {
    repeated group list {
      optional binary element (STRING);
    }
  }
}
```

Definition levels for this structure:
- 0 = the entire list is null
- 1 = the list exists but element group is missing (shouldn't happen in practice)
- 2 = element is null within the list
- 3 = element has a value

**Iceberg:** `list<string>`
**Pinot:** STRING (multi-value column for primitive elements) or JSON

### 4.3 MAP

```
message schema {
  optional group properties (MAP) {
    repeated group key_value {
      required binary key (STRING);
      optional int32 value;
    }
  }
}
```

**Iceberg:** `map<string, int>`
**Pinot:** STRING (serialized as JSON) or MAP (ComplexFieldSpec for simple primitive key-value pairs)

### 4.4 Nested Complex Types

Complex types can be nested arbitrarily:

- LIST of STRUCTs: `list<struct<name: string, age: int>>`
- MAP with STRUCT values: `map<string, struct<x: double, y: double>>`
- STRUCT containing LIST: `struct<name: string, tags: list<string>>`
- STRUCT containing MAP: `struct<name: string, attrs: map<string, string>>`

---

## 5. Complete Parquet -> Iceberg -> Pinot Mapping

| Parquet Type | Iceberg Type | Pinot Type | Notes |
|---|---|---|---|
| BOOLEAN | `boolean` | BOOLEAN | |
| INT32 | `int` | INT | |
| INT32 + DATE | `date` | LONG | Days since epoch |
| INT32 + TIME(MILLIS) | `time` | INT | Millis since midnight |
| INT32 + DECIMAL(P,S) | `decimal(P,S)` | BIG_DECIMAL | P <= 9 |
| INT32 + INT(8/16,signed) | N/A | INT | Narrower-than-int fields |
| INT32 + INT(8/16/32,unsigned) | N/A | INT / LONG | UINT32 -> LONG |
| INT64 | `long` | LONG | |
| INT64 + TIMESTAMP(MILLIS,UTC) | `timestamptz` | TIMESTAMP | |
| INT64 + TIMESTAMP(MICROS,UTC) | `timestamptz` | TIMESTAMP | Iceberg default |
| INT64 + TIMESTAMP(MILLIS,local) | `timestamp` | TIMESTAMP | |
| INT64 + TIMESTAMP(MICROS,local) | `timestamp` | TIMESTAMP | Iceberg default |
| INT64 + TIMESTAMP(NANOS,*) | N/A | TIMESTAMP | Not in Iceberg |
| INT64 + TIME(MICROS) | `time` | LONG | |
| INT64 + TIME(NANOS) | N/A | LONG | Not in Iceberg |
| INT64 + DECIMAL(P,S) | `decimal(P,S)` | BIG_DECIMAL | P <= 18 |
| INT64 + INT(64,signed/unsigned) | N/A | LONG | |
| FLOAT | `float` | FLOAT | |
| DOUBLE | `double` | DOUBLE | |
| BINARY + STRING | `string` | STRING | |
| BINARY + ENUM | N/A | STRING | |
| BINARY + JSON | N/A | STRING | |
| BINARY + BSON | N/A | BYTES | |
| BINARY (no annotation) | `binary` | STRING | |
| FIXED_LEN_BYTE_ARRAY + UUID | `uuid` | STRING | 16 bytes |
| FIXED_LEN_BYTE_ARRAY + DECIMAL | `decimal(P,S)` | BIG_DECIMAL | |
| FIXED_LEN_BYTE_ARRAY (no annotation) | `fixed[N]` | STRING | |
| INT96 | N/A (legacy) | TIMESTAMP | Deprecated |
| GROUP (no annotation) = STRUCT | `struct<...>` | STRING (JSON) | |
| GROUP + LIST | `list<...>` | STRING (multi-value or JSON) | |
| GROUP + MAP | `map<K,V>` | STRING (JSON) or MAP | |

---

## 6. Types Iceberg Actually Produces

When you create an Iceberg table and write data, here's what ends up in Parquet:

| Iceberg Type | Parquet Physical | Parquet Logical | Key Detail |
|---|---|---|---|
| `boolean` | BOOLEAN | none | |
| `int` | INT32 | INT(32, true) | |
| `long` | INT64 | INT(64, true) | |
| `float` | FLOAT | none | |
| `double` | DOUBLE | none | |
| `date` | INT32 | DATE | Days since 1970-01-01 |
| `time` | INT64 | TIME(MICROS, adjusted=true) | Microseconds since midnight |
| `timestamp` | INT64 | TIMESTAMP(MICROS, adjusted=false) | Local timestamp, microsecond precision |
| `timestamptz` | INT64 | TIMESTAMP(MICROS, adjusted=true) | UTC timestamp, microsecond precision |
| `string` | BINARY | STRING | UTF-8 |
| `uuid` | FIXED_LEN_BYTE_ARRAY(16) | UUID | |
| `fixed[N]` | FIXED_LEN_BYTE_ARRAY(N) | none | |
| `binary` | BINARY | none | |
| `decimal(P,S)` | FIXED_LEN_BYTE_ARRAY | DECIMAL(P, S) | Byte length depends on precision |
| `struct<...>` | GROUP | none | Nested fields |
| `list<E>` | GROUP | LIST (3-level) | |
| `map<K,V>` | GROUP | MAP | |

---

## 7. Edge Cases and Known Limitations

### DECIMAL Precision
- INT32 backend: precision 1-9
- INT64 backend: precision 1-18
- FIXED_LEN_BYTE_ARRAY backend: precision 1-38
- Both `ParquetToPinotTypeMapper` and `IcebergSchemaConverter` now map DECIMAL to BIG_DECIMAL

### Timestamp Precision Mismatch
- Iceberg uses **microsecond** precision by default
- Pinot's TIMESTAMP type stores **millisecond** precision internally
- Nanosecond timestamps lose precision when ingested into Pinot

### INT96 Deprecation
- No Iceberg type maps to INT96
- Found only in legacy Hive/Impala/Spark 2.x files
- Should be converted to INT64 TIMESTAMP on read

### UTC vs Local Timestamps
- `isAdjustedToUTC=true`: timestamp represents an instant in time (UTC)
- `isAdjustedToUTC=false`: timestamp represents a local date-time (wall clock)
- Pinot treats both the same (TIMESTAMP), losing the timezone semantics

### Binary vs String Default
- BINARY without logical type defaults to STRING in Pinot (not BYTES)
- FIXED_LEN_BYTE_ARRAY without logical type also defaults to STRING
- Only BSON logical type maps to BYTES

---

## 8. Pinot Complex Type Support

Pinot does not natively store nested/hierarchical data the way Parquet does. Complex types go through a lossy conversion depending on the ingestion path.

### 8.1 How Complex Types Land in Pinot

| Parquet Complex Type | Pinot Representation | Notes |
|---|---|---|
| **LIST of primitives** | Multi-value `DimensionFieldSpec` | Elements become a Pinot MV column (e.g. `list<string>` -> MV STRING) |
| **LIST of complex types** | `DimensionFieldSpec` with `DataType.JSON` | Serialized to JSON string |
| **MAP\<primitive, primitive\>** | `ComplexFieldSpec` with `DataType.MAP` | Key/value child fields via `IcebergSchemaConverter` |
| **MAP with complex values** | `DimensionFieldSpec` with `DataType.JSON` | Serialized to JSON string |
| **STRUCT** | `DimensionFieldSpec` with `DataType.JSON` | Always serialized to JSON |
| **Nested complex** (list of structs, etc.) | `DimensionFieldSpec` with `DataType.JSON` | Serialized to JSON string |

### 8.2 Two Ingestion Paths, Two Mappers

The type mapping differs depending on which ingestion path is used:

| Type | `ParquetToPinotTypeMapper` (direct Parquet) | `IcebergSchemaConverter` (Iceberg pipeline) |
|---|---|---|
| DECIMAL | BIG_DECIMAL | BIG_DECIMAL |
| DATE | LONG | INT |
| JSON logical type | STRING | JSON |
| LIST of primitives | STRING (JSON) | Multi-value column |
| Simple MAP | STRING (JSON) | ComplexFieldSpec with MAP |
| STRUCT | STRING (JSON) | DimensionFieldSpec with JSON |

The `ParquetToPinotTypeMapper.mapComplexType()` treats **all** complex types (LIST, MAP, STRUCT) as STRING, relying on JSON serialization. The `IcebergSchemaConverter` is more granular -- it distinguishes LIST-of-primitives (MV columns) and simple MAPs (`ComplexFieldSpec`) from nested structures (JSON).

### 8.3 Known Gaps and Caveats

1. **`ParquetSequentialColumnReaderFactory`** explicitly notes: *"Doesn't handle multi value columns or struct columns yet."* -- the Arrow-based reader path has incomplete complex type support.

2. **STRUCT has no native representation.** It always becomes a JSON string. Queryable via Pinot's JSON indexing, but loses the strongly-typed schema.

3. **Deeply nested types collapse to JSON.** There is no recursive field spec generation -- anything beyond one level of nesting becomes an opaque JSON blob.

4. **Null handling for complex types uses sentinel values** in `GenericRowRecordMaterializer`, but only for primitive converters. The `MulivalueFieldConverter` does not check for sentinel nulls.

5. **Timestamp precision mismatch.** Iceberg defaults to microsecond precision; Pinot TIMESTAMP stores milliseconds internally. Nanosecond timestamps lose precision.

6. **UTC vs local timestamps.** Pinot treats `isAdjustedToUTC=true` and `false` identically (both become TIMESTAMP), losing timezone semantics.
