# NULL Handling -- Parquet vs Pinot

This document explains how NULLs work in Parquet, how Pinot handles them, and the test scenarios we need to cover for Iceberg data type validation.

---

## 1. Parquet NULL Representation

**Parquet DOES store nulls** -- efficiently, using definition levels.

### Definition Levels

Every column in Parquet has a **repetition** attribute:
- `REQUIRED` -- value must always be present (definition level not stored, saves space)
- `OPTIONAL` -- value can be null (definition level column tracks presence)
- `REPEATED` -- value can appear zero or more times (used inside LIST/MAP)

For an OPTIONAL field, the definition level is a small integer:
- **max definition level** = value is present
- **less than max** = value is null at some nesting depth

### Simple OPTIONAL Column

```
optional int32 age;
```

- Definition level 0 = null
- Definition level 1 = value present

Storage cost: ~1 bit per value in the definition level column (run-length + bit-packing encoded).

### 3-Level LIST Definition Levels

```
optional group tags (LIST) {        -- def level 0 = entire list is null
  repeated group list {             -- def level 1 = list exists but empty
    optional binary element (STRING);  -- def level 2 = element is null
  }                                    -- def level 3 = element has a value
}
```

| Definition Level | Meaning |
|---|---|
| 0 | The entire list column is null |
| 1 | The list exists but is empty (zero elements) |
| 2 | An element slot exists but the element value is null |
| 3 | An element has a non-null value |

This distinction between "null list" (level 0) and "empty list" (level 1) is important -- they are semantically different.

### MAP Definition Levels

```
optional group attrs (MAP) {
  repeated group key_value {
    required binary key (STRING);
    optional int32 value;
  }
}
```

- Level 0 = entire map is null
- Level 1 = map exists but empty
- Level 2 (on value column) = key exists but value is null
- Level 3 (on value column) = key-value pair fully present

### STRUCT Definition Levels

```
optional group profile {
  optional binary name (STRING);
  optional int32 age;
}
```

- Level 0 on profile = entire struct is null (all child fields are implicitly null)
- Level 1 on name = struct exists, name field is null
- Level 2 on name = struct exists, name has a value

---

## 2. Pinot NULL Handling

Pinot has two mechanisms, controlled by the schema flag `enableColumnBasedNullHandling`.

### 2.1 Sentinel-Based (Legacy, Default)

When `enableColumnBasedNullHandling` is **false** (default), Pinot uses magic sentinel values to represent nulls:

| Pinot Type | Sentinel Value | Risk |
|---|---|---|
| INT | `Integer.MIN_VALUE` (-2147483648) | Actual data with this value is treated as null |
| LONG | `Long.MIN_VALUE` (-9223372036854775808) | Same collision risk |
| FLOAT | `Float.NEGATIVE_INFINITY` | Rare in real data but possible |
| DOUBLE | `Double.NEGATIVE_INFINITY` | Same |
| BOOLEAN | `-1` (as int) | |
| STRING | `"null"` literal string | The string "null" becomes indistinguishable from null |
| BYTES | specific placeholder bytes | |

**Problem:** If actual data contains a sentinel value, it is incorrectly treated as null. This is the primary motivation for column-based null handling.

The `GenericRowRecordMaterializer` in the Parquet ingestion path uses these sentinels:
- `addInt(Integer.MIN_VALUE)` -> `putDefaultNullValue()`
- `addLong(Long.MIN_VALUE)` -> `putDefaultNullValue()`
- `addFloat(Float.NEGATIVE_INFINITY)` -> `putDefaultNullValue()`
- `addDouble(Double.NEGATIVE_INFINITY)` -> `putDefaultNullValue()`
- `addBinary("NULL_PLACEHOLDER")` -> `putDefaultNullValue()`

### 2.2 Column-Based Null Handling (Modern)

When `enableColumnBasedNullHandling` is **true**, Pinot uses a **null bitmap** (`NullValueVectorReader`) per column. Each bit indicates whether a row is null.

Advantages:
- No sentinel value collision
- Proper null semantics in queries (`IS NULL`, `IS NOT NULL`)
- Nulls are stored separately from values

This is the recommended approach for new tables.

### 2.3 Default Null Values

When a column is null and `enableColumnBasedNullHandling` is false, Pinot stores a default null value:

| Pinot Type | Default Null Value |
|---|---|
| INT | `Integer.MIN_VALUE` |
| LONG | `Long.MIN_VALUE` |
| FLOAT | `Float.NEGATIVE_INFINITY` |
| DOUBLE | `Double.NEGATIVE_INFINITY` |
| STRING | `"null"` |
| BOOLEAN | `0` (false) |
| BYTES | empty byte array |
| TIMESTAMP | `0` |
| JSON | `"null"` |
| BIG_DECIMAL | `0` |

These can be overridden per-column in the Pinot schema with `defaultNullValue`.

---

## 3. NULL Test Scenarios

### Scenario 1: Basic OPTIONAL Primitives

**Goal:** Verify that Parquet OPTIONAL columns with null values are correctly read by Pinot.

Columns: OPTIONAL INT32, INT64, FLOAT, DOUBLE, BOOLEAN, BINARY+STRING
- 10% nulls randomly distributed
- Verify nulls round-trip through ingestion

### Scenario 2: REQUIRED Columns (Never Null)

**Goal:** Verify REQUIRED columns never produce false nulls.

Columns: REQUIRED INT32, INT64, FLOAT, DOUBLE, BINARY+STRING
- All values present, no definition level column in Parquet
- Pinot should read all values without any nulls

### Scenario 3: Sentinel Value Collision

**Goal:** Test what happens when actual data values equal Pinot's null sentinels.

Columns with specific values:
- INT32 column containing `Integer.MIN_VALUE` (-2147483648) as actual data
- INT64 column containing `Long.MIN_VALUE` as actual data
- FLOAT column containing `Float.NEGATIVE_INFINITY` as actual data
- DOUBLE column containing `Double.NEGATIVE_INFINITY` as actual data
- STRING column containing the literal string `"null"` as actual data

Expected behavior:
- With `enableColumnBasedNullHandling=false`: these values are **incorrectly** treated as null (known limitation)
- With `enableColumnBasedNullHandling=true`: these values are correctly preserved as non-null

### Scenario 4: Nullable LIST

**Goal:** Distinguish null list vs empty list vs list with null elements.

Rows:
1. Null list (definition level 0) -- the entire list is null
2. Empty list `[]` (definition level 1) -- list exists but has no elements
3. List with null element `[null, "a"]` (definition level 2 + 3)
4. List with all non-null elements `["a", "b", "c"]` (definition level 3 for all)

### Scenario 5: Nullable MAP

**Goal:** Test null map vs map with null values.

Rows:
1. Null map -- entire map is null
2. Empty map `{}` -- map exists but has no entries
3. Map with null value `{"key1": null}` -- key exists, value is null
4. Map with all non-null entries `{"key1": 1, "key2": 2}`

### Scenario 6: Nullable STRUCT

**Goal:** Test null struct vs struct with null fields.

Rows:
1. Null struct -- entire struct is null
2. Struct with null fields `{"name": null, "age": 25}` -- struct exists, some fields null
3. Struct with all null fields `{"name": null, "age": null}` -- struct exists, all fields null
4. Struct with all values `{"name": "Alice", "age": 30}`

### Scenario 7: Schema Mismatch -- OPTIONAL in Parquet, notNull in Pinot

**Goal:** Test what happens when the Parquet column is OPTIONAL (has nulls) but the Pinot schema declares `notNull: true`.

Expected behavior: Pinot should apply the default null value for rows where the Parquet value is null, or reject the data depending on configuration.

### Scenario 8: Iceberg Column Optionality

**Goal:** Verify Iceberg `required: true` vs `required: false` propagates correctly.

Iceberg schema fields have a `required` flag:
- `required: true` -> Parquet REQUIRED -> no nulls possible
- `required: false` (default) -> Parquet OPTIONAL -> nulls allowed

Verify that the `IcebergSchemaConverter` correctly respects this when building the Pinot schema.

---

## 4. Practical Testing Approach

For each scenario, the generator creates Parquet files with carefully crafted data. Validation is manual:

1. Generate Parquet file with the config
2. Inspect with `pyarrow.parquet.read_table()` to confirm the file has correct types and null patterns
3. Ingest into Pinot and query to verify null handling
4. Compare results with and without `enableColumnBasedNullHandling`

```python
import pyarrow.parquet as pq

table = pq.read_table("output/null-scenarios.parquet")
for col_name in table.column_names:
    col = table.column(col_name)
    print(f"{col_name}: type={col.type}, nulls={col.null_count}/{len(col)}")
```
