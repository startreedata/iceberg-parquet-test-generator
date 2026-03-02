# Parquet Data Type Testing — Test Plan

Phase 1 functional testing of all Parquet data types supported by Pinot via raw S3 catalog table type.

---

## Workflow per Test

1. Upload `.parquet` file to S3
2. Create Pinot table using the generated `_pinot_schema.json` and `_pinot_table_config.json`
3. Trigger segment ingestion from S3
4. Run validation queries
5. Record PASS / FAIL per column

---

## Test 1: Primitive Types

**Config:** `all-primitive-types.json`
**Files:** `all_primitive_types.parquet`, `all_primitive_types_pinot_schema.json`, `all_primitive_types_pinot_table_config.json`
**Rows:** 100,000

| # | Column | Parquet Type | Pinot Type | Validation Query | Pass Criteria |
|---|--------|-------------|------------|-----------------|---------------|
| 1.1 | col_boolean | BOOLEAN | BOOLEAN | `SELECT col_boolean, COUNT(*) FROM all_primitive_types GROUP BY col_boolean` | true/false values present, null count ~10% |
| 1.2 | col_int32 | INT32 | INT | `SELECT MIN(col_int32), MAX(col_int32), COUNT(*) FROM all_primitive_types WHERE col_int32 IS NOT NULL` | Values in INT32 range, ~90k non-null |
| 1.3 | col_int64 | INT64 | LONG | `SELECT MIN(col_int64), MAX(col_int64), COUNT(*) FROM all_primitive_types WHERE col_int64 IS NOT NULL` | Values in INT64 range |
| 1.4 | col_float | FLOAT | FLOAT | `SELECT MIN(col_float), MAX(col_float), COUNT(*) FROM all_primitive_types WHERE col_float IS NOT NULL` | Floating point values present |
| 1.5 | col_double | DOUBLE | DOUBLE | `SELECT MIN(col_double), MAX(col_double), COUNT(*) FROM all_primitive_types WHERE col_double IS NOT NULL` | Double precision values |
| 1.6 | col_binary | BINARY | STRING | `SELECT col_binary, COUNT(*) FROM all_primitive_types WHERE col_binary IS NOT NULL LIMIT 5` | Byte data readable as string |
| 1.7 | col_fixed_len | FIXED_LEN_BYTE_ARRAY(20) | STRING | `SELECT col_fixed_len, COUNT(*) FROM all_primitive_types WHERE col_fixed_len IS NOT NULL LIMIT 5` | 20-byte fixed data as string |
| 1.8 | col_int96 | INT96 (deprecated) | TIMESTAMP | `SELECT col_int96, COUNT(*) FROM all_primitive_types WHERE col_int96 IS NOT NULL LIMIT 5` | Timestamp values readable |
| 1.9 | col_boolean_required | BOOLEAN (REQUIRED) | BOOLEAN | `SELECT COUNT(*) FROM all_primitive_types WHERE col_boolean_required IS NULL` | Count = 0 (no nulls) |
| 1.10 | col_int32_required | INT32 (REQUIRED) | INT | `SELECT COUNT(*) FROM all_primitive_types WHERE col_int32_required IS NULL` | Count = 0 |
| 1.11 | col_int64_required | INT64 (REQUIRED) | LONG | `SELECT COUNT(*) FROM all_primitive_types WHERE col_int64_required IS NULL` | Count = 0 |
| 1.12 | col_float_required | FLOAT (REQUIRED) | FLOAT | `SELECT COUNT(*) FROM all_primitive_types WHERE col_float_required IS NULL` | Count = 0 |
| 1.13 | col_double_required | DOUBLE (REQUIRED) | DOUBLE | `SELECT COUNT(*) FROM all_primitive_types WHERE col_double_required IS NULL` | Count = 0 |
| 1.14 | col_binary_required | BINARY (REQUIRED) | STRING | `SELECT COUNT(*) FROM all_primitive_types WHERE col_binary_required IS NULL` | Count = 0 |

**Global check:** `SELECT COUNT(*) FROM all_primitive_types` → 100,000

---

## Test 2: Logical Types

**Config:** `all-logical-types.json`
**Files:** `all_logical_types.parquet`, `all_logical_types_pinot_schema.json`, `all_logical_types_pinot_table_config.json`
**Rows:** 100,000

### 2A: Integer Subtypes

| # | Column | Parquet Logical | Pinot Type | Validation Query | Pass Criteria |
|---|--------|----------------|------------|-----------------|---------------|
| 2.1 | col_int8 | INT(8, signed) | INT | `SELECT MIN(col_int8), MAX(col_int8) FROM all_logical_types WHERE col_int8 IS NOT NULL` | Range: -128 to 127 |
| 2.2 | col_int16 | INT(16, signed) | INT | `SELECT MIN(col_int16), MAX(col_int16) FROM all_logical_types WHERE col_int16 IS NOT NULL` | Range: -32768 to 32767 |
| 2.3 | col_int32_logical | INT(32, signed) | INT | `SELECT MIN(col_int32_logical), MAX(col_int32_logical) FROM all_logical_types WHERE col_int32_logical IS NOT NULL` | INT32 range |
| 2.4 | col_int64_logical | INT(64, signed) | LONG | `SELECT MIN(col_int64_logical), MAX(col_int64_logical) FROM all_logical_types WHERE col_int64_logical IS NOT NULL` | INT64 range |
| 2.5 | col_uint8 | INT(8, unsigned) | INT | `SELECT MIN(col_uint8), MAX(col_uint8) FROM all_logical_types WHERE col_uint8 IS NOT NULL` | Range: 0 to 255 |
| 2.6 | col_uint16 | INT(16, unsigned) | INT | `SELECT MIN(col_uint16), MAX(col_uint16) FROM all_logical_types WHERE col_uint16 IS NOT NULL` | Range: 0 to 65535 |
| 2.7 | col_uint32 | INT(32, unsigned) | LONG | `SELECT MIN(col_uint32), MAX(col_uint32) FROM all_logical_types WHERE col_uint32 IS NOT NULL` | Range: 0 to 4294967295 |
| 2.8 | col_uint64 | INT(64, unsigned) | LONG | `SELECT MIN(col_uint64), MAX(col_uint64) FROM all_logical_types WHERE col_uint64 IS NOT NULL` | Non-negative values |

### 2B: String and Binary Types

| # | Column | Parquet Logical | Pinot Type | Validation Query | Pass Criteria |
|---|--------|----------------|------------|-----------------|---------------|
| 2.9 | col_string | STRING | STRING | `SELECT col_string FROM all_logical_types WHERE col_string IS NOT NULL LIMIT 5` | UTF-8 strings readable |
| 2.10 | col_enum | ENUM | STRING | `SELECT col_enum FROM all_logical_types WHERE col_enum IS NOT NULL LIMIT 5` | Enum values as strings |
| 2.11 | col_json | JSON | STRING | `SELECT col_json FROM all_logical_types WHERE col_json IS NOT NULL LIMIT 5` | Valid JSON strings |
| 2.12 | col_bson | BSON | BYTES | `SELECT col_bson FROM all_logical_types WHERE col_bson IS NOT NULL LIMIT 5` | Binary data present |
| 2.13 | col_uuid | UUID | STRING | `SELECT col_uuid FROM all_logical_types WHERE col_uuid IS NOT NULL LIMIT 5` | UUID-formatted strings (if hex-encoded) |

### 2C: Temporal Types

| # | Column | Parquet Logical | Pinot Type | Validation Query | Pass Criteria |
|---|--------|----------------|------------|-----------------|---------------|
| 2.14 | col_date | DATE | LONG | `SELECT MIN(col_date), MAX(col_date) FROM all_logical_types WHERE col_date IS NOT NULL` | Epoch day values (reasonable range) |
| 2.15 | col_time_millis | TIME(MILLIS) | INT | `SELECT MIN(col_time_millis), MAX(col_time_millis) FROM all_logical_types WHERE col_time_millis IS NOT NULL` | 0 to 86399999 (ms in a day) |
| 2.16 | col_time_micros | TIME(MICROS) | LONG | `SELECT MIN(col_time_micros), MAX(col_time_micros) FROM all_logical_types WHERE col_time_micros IS NOT NULL` | 0 to 86399999999 |
| 2.17 | col_time_nanos | TIME(NANOS) | LONG | `SELECT MIN(col_time_nanos), MAX(col_time_nanos) FROM all_logical_types WHERE col_time_nanos IS NOT NULL` | 0 to 86399999999999 |
| 2.18 | col_timestamp_millis_utc | TIMESTAMP(MILLIS, UTC) | TIMESTAMP | `SELECT col_timestamp_millis_utc FROM all_logical_types WHERE col_timestamp_millis_utc IS NOT NULL LIMIT 5` | Readable timestamps |
| 2.19 | col_timestamp_millis_local | TIMESTAMP(MILLIS, local) | TIMESTAMP | `SELECT col_timestamp_millis_local FROM all_logical_types WHERE col_timestamp_millis_local IS NOT NULL LIMIT 5` | Readable timestamps |
| 2.20 | col_timestamp_micros_utc | TIMESTAMP(MICROS, UTC) | TIMESTAMP | `SELECT col_timestamp_micros_utc FROM all_logical_types WHERE col_timestamp_micros_utc IS NOT NULL LIMIT 5` | Readable timestamps (micros → millis precision loss expected) |
| 2.21 | col_timestamp_micros_local | TIMESTAMP(MICROS, local) | TIMESTAMP | Same as above | Same |
| 2.22 | col_timestamp_nanos_utc | TIMESTAMP(NANOS, UTC) | TIMESTAMP | `SELECT col_timestamp_nanos_utc FROM all_logical_types WHERE col_timestamp_nanos_utc IS NOT NULL LIMIT 5` | Readable timestamps (nanos → millis precision loss expected) |
| 2.23 | col_timestamp_nanos_local | TIMESTAMP(NANOS, local) | TIMESTAMP | Same as above | Same |

### 2D: Decimal Types

| # | Column | Parquet Physical+Logical | Pinot Type | Validation Query | Pass Criteria |
|---|--------|------------------------|------------|-----------------|---------------|
| 2.24 | col_decimal_int32 | INT32 + DECIMAL(9,2) | BIG_DECIMAL | `SELECT col_decimal_int32 FROM all_logical_types WHERE col_decimal_int32 IS NOT NULL LIMIT 5` | Decimal values with scale=2 |
| 2.25 | col_decimal_int64 | INT64 + DECIMAL(18,4) | BIG_DECIMAL | `SELECT col_decimal_int64 FROM all_logical_types WHERE col_decimal_int64 IS NOT NULL LIMIT 5` | Decimal values with scale=4 |
| 2.26 | col_decimal_fixed | FIXED + DECIMAL(28,6) | BIG_DECIMAL | `SELECT col_decimal_fixed FROM all_logical_types WHERE col_decimal_fixed IS NOT NULL LIMIT 5` | Decimal values with scale=6 |
| 2.27 | col_decimal_binary | BINARY + DECIMAL(38,10) | BIG_DECIMAL | `SELECT col_decimal_binary FROM all_logical_types WHERE col_decimal_binary IS NOT NULL LIMIT 5` | Decimal values with scale=10 |

---

## Test 3: Complex Types

**Config:** `all-complex-types.json`
**Files:** `all_complex_types.parquet`, `all_complex_types_pinot_schema.json`, `all_complex_types_pinot_table_config.json`
**Rows:** 100,000

All complex types map to JSON (STRING) via ParquetToPinotTypeMapper.

| # | Column | Parquet Type | Pinot Type | Validation Query | Pass Criteria |
|---|--------|-------------|------------|-----------------|---------------|
| 3.1 | flat_struct | STRUCT{name, age, score} | JSON | `SELECT flat_struct FROM all_complex_types WHERE flat_struct IS NOT NULL LIMIT 3` | Valid JSON with "name", "age", "score" keys |
| 3.2 | nested_struct | STRUCT{id, address{street, city, zip}} | JSON | `SELECT nested_struct FROM all_complex_types WHERE nested_struct IS NOT NULL LIMIT 3` | Valid JSON with nested "address" object |
| 3.3 | string_list | LIST\<STRING\> | JSON | `SELECT string_list FROM all_complex_types WHERE string_list IS NOT NULL LIMIT 3` | JSON array of strings |
| 3.4 | int_list | LIST\<INT32\> | JSON | `SELECT int_list FROM all_complex_types WHERE int_list IS NOT NULL LIMIT 3` | JSON array of integers |
| 3.5 | list_of_structs | LIST\<STRUCT\> | JSON | `SELECT list_of_structs FROM all_complex_types WHERE list_of_structs IS NOT NULL LIMIT 3` | JSON array of objects |
| 3.6 | map_string_int | MAP\<STRING, INT32\> | JSON | `SELECT map_string_int FROM all_complex_types WHERE map_string_int IS NOT NULL LIMIT 3` | JSON object with string keys, int values |
| 3.7 | map_string_string | MAP\<STRING, STRING\> | JSON | `SELECT map_string_string FROM all_complex_types WHERE map_string_string IS NOT NULL LIMIT 3` | JSON object with string keys/values |
| 3.8 | map_string_struct | MAP\<STRING, STRUCT\> | JSON | `SELECT map_string_struct FROM all_complex_types WHERE map_string_struct IS NOT NULL LIMIT 3` | JSON object with nested struct values |
| 3.9 | struct_with_list | STRUCT{label, tags: LIST} | JSON | `SELECT struct_with_list FROM all_complex_types WHERE struct_with_list IS NOT NULL LIMIT 3` | JSON with "tags" as nested array |
| 3.10 | struct_with_map | STRUCT{name, attrs: MAP} | JSON | `SELECT struct_with_map FROM all_complex_types WHERE struct_with_map IS NOT NULL LIMIT 3` | JSON with "attrs" as nested object |

**JSON index check:** Verify JSON indexing works on complex type columns:
```sql
SELECT JSON_EXTRACT_SCALAR(flat_struct, '$.name', 'STRING') AS name,
       JSON_EXTRACT_SCALAR(flat_struct, '$.age', 'INT') AS age
FROM all_complex_types
WHERE JSON_MATCH(flat_struct, '"$.name" IS NOT NULL')
LIMIT 5
```

---

## Test 4: Null Handling

**Config:** `null-scenarios.json`
**Files:** `null_scenarios.parquet`, `null_scenarios_pinot_schema.json`, `null_scenarios_pinot_table_config.json`
**Rows:** 100,000

### 4A: Basic Optional Primitives (~15% nulls)

| # | Column | Validation Query | Pass Criteria |
|---|--------|-----------------|---------------|
| 4.1 | scenario1_optional_int | `SELECT COUNT(*) FROM null_scenarios WHERE scenario1_optional_int IS NULL` | ~15,000 nulls |
| 4.2 | scenario1_optional_long | Same pattern | ~15,000 nulls |
| 4.3 | scenario1_optional_float | Same pattern | ~15,000 nulls |
| 4.4 | scenario1_optional_double | Same pattern | ~15,000 nulls |
| 4.5 | scenario1_optional_bool | Same pattern | ~15,000 nulls |
| 4.6 | scenario1_optional_string | Same pattern | ~15,000 nulls |

### 4B: Required Columns (Zero Nulls)

| # | Column | Validation Query | Pass Criteria |
|---|--------|-----------------|---------------|
| 4.7 | scenario2_required_int | `SELECT COUNT(*) FROM null_scenarios WHERE scenario2_required_int IS NULL` | 0 |
| 4.8 | scenario2_required_long | Same pattern | 0 |
| 4.9 | scenario2_required_string | Same pattern | 0 |

### 4C: Sentinel Collision (~20% nulls, fixedValues include sentinel values)

| # | Column | Validation Query | Pass Criteria |
|---|--------|-----------------|---------------|
| 4.10 | scenario3_sentinel_int | `SELECT COUNT(*) FROM null_scenarios WHERE scenario3_sentinel_int = -2147483648` | Should have rows with Integer.MIN_VALUE as real data (if column-based null handling works) |
| 4.11 | scenario3_sentinel_long | `SELECT COUNT(*) FROM null_scenarios WHERE scenario3_sentinel_long = -9223372036854775808` | Same — Long.MIN_VALUE as real data |
| 4.12 | scenario3_sentinel_string | `SELECT COUNT(*) FROM null_scenarios WHERE scenario3_sentinel_string = 'null'` | Literal string "null" preserved as data, not treated as null |

### 4D: Nullable Complex Types (~20% nulls)

| # | Column | Validation Query | Pass Criteria |
|---|--------|-----------------|---------------|
| 4.13 | scenario4_nullable_list | `SELECT COUNT(*) FROM null_scenarios WHERE scenario4_nullable_list IS NULL` | ~20,000 nulls |
| 4.14 | scenario4_nullable_list (non-null) | `SELECT scenario4_nullable_list FROM null_scenarios WHERE scenario4_nullable_list IS NOT NULL LIMIT 5` | Valid JSON arrays |
| 4.15 | scenario5_nullable_map | `SELECT COUNT(*) FROM null_scenarios WHERE scenario5_nullable_map IS NULL` | ~20,000 nulls |
| 4.16 | scenario6_nullable_struct | `SELECT COUNT(*) FROM null_scenarios WHERE scenario6_nullable_struct IS NULL` | ~20,000 nulls |

### 4E: Nullable Temporal

| # | Column | Validation Query | Pass Criteria |
|---|--------|-----------------|---------------|
| 4.17 | scenario7_optional_date | `SELECT COUNT(*) FROM null_scenarios WHERE scenario7_optional_date IS NULL` | ~25,000 nulls |
| 4.18 | scenario7_optional_timestamp | `SELECT COUNT(*) FROM null_scenarios WHERE scenario7_optional_timestamp IS NULL` | ~25,000 nulls |

### 4F: Required vs Optional Strings

| # | Column | Validation Query | Pass Criteria |
|---|--------|-----------------|---------------|
| 4.19 | scenario8_required_iceberg_string | `SELECT COUNT(*) FROM null_scenarios WHERE scenario8_required_iceberg_string IS NULL` | 0 |
| 4.20 | scenario8_optional_iceberg_string | `SELECT COUNT(*) FROM null_scenarios WHERE scenario8_optional_iceberg_string IS NULL` | ~50,000 nulls |

---

## Test 5: Iceberg Full Coverage

**Config:** `iceberg-full-coverage.json`
**Files:** `iceberg_full_coverage.parquet`, `iceberg_full_coverage_pinot_schema.json`, `iceberg_full_coverage_pinot_table_config.json`
**Rows:** 100,000

This config mirrors the exact types Iceberg produces. Key validations:

| # | Column | Type Chain | Validation |
|---|--------|-----------|------------|
| 5.1 | iceberg_boolean | BOOLEAN | Standard boolean check |
| 5.2 | iceberg_int | INT32+INT(32,true) | INT range |
| 5.3 | iceberg_long | INT64+INT(64,true) | LONG range |
| 5.4 | iceberg_float | FLOAT | Float values |
| 5.5 | iceberg_double | DOUBLE | Double values |
| 5.6 | iceberg_date | INT32+DATE → LONG | Epoch days, positive values |
| 5.7 | iceberg_time | INT64+TIME(MICROS) → LONG | 0 to 86399999999 |
| 5.8 | iceberg_timestamp | INT64+TIMESTAMP(MICROS, local) → TIMESTAMP | Readable timestamps |
| 5.9 | iceberg_timestamptz | INT64+TIMESTAMP(MICROS, UTC) → TIMESTAMP | Readable timestamps |
| 5.10 | iceberg_string | BINARY+STRING | UTF-8 strings |
| 5.11 | iceberg_uuid | FIXED(16)+UUID → STRING | UUID-format strings |
| 5.12 | iceberg_binary | BINARY → STRING | Binary as base64/hex string |
| 5.13 | iceberg_fixed_8 | FIXED(8) → STRING | 8-byte fixed data |
| 5.14 | iceberg_decimal_9_2 | FIXED+DECIMAL(9,2) → BIG_DECIMAL | Decimal with 2 decimal places |
| 5.15 | iceberg_decimal_18_4 | FIXED+DECIMAL(18,4) → BIG_DECIMAL | 4 decimal places |
| 5.16 | iceberg_decimal_38_10 | FIXED+DECIMAL(38,10) → BIG_DECIMAL | 10 decimal places, high precision |
| 5.17 | iceberg_struct | STRUCT → JSON | Valid JSON object |
| 5.18 | iceberg_list_string | LIST\<STRING\> → JSON | JSON array |
| 5.19 | iceberg_list_int | LIST\<INT32\> → JSON | JSON array of ints |
| 5.20 | iceberg_map_string_int | MAP → JSON | JSON object |
| 5.21 | iceberg_map_string_string | MAP → JSON | JSON object |
| 5.22 | iceberg_nested_struct | STRUCT{id, inner{x,y}} → JSON | Nested JSON |
| 5.23 | iceberg_required_string | STRING (REQUIRED) | 0 nulls |
| 5.24 | iceberg_required_int | INT (REQUIRED) | 0 nulls |

---

## Test 6: Golden Dataset (All-in-One)

**Config:** `golden-dataset-extended.json`
**Files:** `golden_dataset_extended.parquet`, `golden_dataset_extended_pinot_schema.json`, `golden_dataset_extended_pinot_table_config.json`
**Rows:** 1,000

This is a superset smoke test with every type category in a single table. Smaller row count (1,000) for quick iteration.

**Quick validation:**
```sql
SELECT COUNT(*) FROM golden_dataset_extended
-- Expected: 1000

SELECT * FROM golden_dataset_extended LIMIT 5
-- Expected: all 36 columns populated with correct types
```

---

## Execution Checklist

| Test | Config | Table Created | Ingested | Queries Pass | Notes |
|------|--------|:------------:|:--------:|:------------:|-------|
| 1. Primitive Types | all-primitive-types.json | [ ] | [ ] | [ ] | |
| 2. Logical Types | all-logical-types.json | [ ] | [ ] | [ ] | |
| 3. Complex Types | all-complex-types.json | [ ] | [ ] | [ ] | |
| 4. Null Handling | null-scenarios.json | [ ] | [ ] | [ ] | |
| 5. Iceberg Coverage | iceberg-full-coverage.json | [ ] | [ ] | [ ] | |
| 6. Golden Dataset | golden-dataset-extended.json | [ ] | [ ] | [ ] | |

---

## Known Risks to Watch For

1. **INT96 ingestion** — deprecated, no Parquet statistics. May fail in some ingestion paths. Not used as timeColumnName.
2. **BSON as BYTES** — Pinot BYTES columns may need special handling in queries (base64 encoding).
3. **Timestamp precision loss** — MICROS and NANOS timestamps will lose sub-millisecond precision in Pinot.
4. **Sentinel collision (Test 4C)** — With `enableColumnBasedNullHandling: true` (our default), sentinel values should be preserved as real data. If they're treated as null, column-based null handling is not working.
5. **Complex types as JSON** — All STRUCT/LIST/MAP become JSON strings. JSON_EXTRACT_SCALAR queries should work if JSON indexing is enabled (our table configs enable it).
6. **DECIMAL as BIG_DECIMAL** — This depends on the upcoming ParquetToPinotTypeMapper change. If the change isn't deployed yet, DECIMAL columns may fail or map to STRING instead.
