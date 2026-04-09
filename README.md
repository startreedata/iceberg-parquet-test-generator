# Iceberg Parquet Test Generator

JSON-config-driven Parquet dataset generator for validating all Parquet data types through the Iceberg-to-Pinot pipeline.

## Quick Start

```bash
pip install -r requirements.txt

# Generate a dataset from a config
python -m generator.cli --config configs/all-primitive-types.json --output output/

# Override row count
python -m generator.cli --config configs/all-logical-types.json --output output/ --num-rows 5000

# Use a specific seed for reproducibility
python -m generator.cli --config configs/iceberg-full-coverage.json --output output/ --seed 123
```

## Project Structure

```
docs/                          Documentation
  parquet-data-types.md        Complete Parquet type taxonomy + Iceberg/Pinot mapping
  null-handling.md             NULL strategy: Parquet definition levels vs Pinot sentinels/bitmaps

generator/                     Python package
  config_model.py              Pydantic models for JSON config validation
  schema_builder.py            JSON config -> PyArrow schema
  data_generator.py            Type-aware random data generation
  parquet_writer.py            Write Parquet files with configurable options
  pinot_config_generator.py    Generate Pinot schema + table config from dataset config
  cli.py                       CLI entry point

configs/                       Pre-built JSON configs
  all-primitive-types.json     All 8 physical types without logical annotations
  all-logical-types.json       All logical type combinations
  all-complex-types.json       STRUCT, MAP, LIST, nested combinations
  multi-value-types.json       Multi-value (MV) primitive columns (LIST<primitive>)
  iceberg-full-coverage.json   Types as Iceberg would actually produce them
  null-scenarios.json          NULL edge-case scenarios
  golden-dataset-extended.json Superset of the original golden_schema
```

## CLI Reference

| Flag | Description |
|---|---|
| `--config`, `-c` | **(required)** Path to a JSON config file |
| `--output`, `-o` | Output directory (default: `output/`) |
| `--num-rows`, `-n` | Override `numRows` from the config |
| `--seed`, `-s` | Override `seed` from the config (see [Seed & Reproducibility](#seed--reproducibility)) |
| `--pinot-config` | Generate Pinot schema and table config JSON files |
| `--parquet` | Generate Parquet data file (default when `--pinot-config` is not set) |
| `--mapping-mode` | Type mapping mode: `parquet` (default) or `iceberg` (see [Mapping Modes](#mapping-modes)) |

## Seed & Reproducibility

All random data generation is driven by a **seeded PRNG** (`random.Random(seed)`). The seed (default: `42`) controls every random decision -- column values, null placement, list lengths, map key generation, etc.

**Same config + same seed = byte-identical Parquet output every time.** This matters because the workflow is: generate a dataset, create a Pinot table using the generated schema/table config, ingest the Parquet file, and verify every data type round-trips correctly. A stable seed means you can reproduce the exact dataset that caused an ingestion failure and debug against known data.

Override with `--seed` on the CLI or `"seed"` in the config JSON. Different seeds produce statistically similar but distinct datasets.

## JSON Config Format

Each config specifies Parquet-native types (not Pinot types):

```json
{
  "name": "my_dataset",
  "numRows": 1000,
  "seed": 42,
  "defaultNullRatio": 0.1,
  "columns": [
    {
      "name": "id",
      "physicalType": "INT32",
      "repetition": "REQUIRED"
    },
    {
      "name": "created_date",
      "physicalType": "INT32",
      "logicalType": "DATE",
      "repetition": "OPTIONAL"
    },
    {
      "name": "amount",
      "physicalType": "FIXED_LEN_BYTE_ARRAY",
      "logicalType": "DECIMAL",
      "logicalTypeParams": { "precision": 10, "scale": 2 },
      "fixedLength": 16,
      "repetition": "OPTIONAL"
    },
    {
      "name": "tags",
      "physicalType": "LIST",
      "element": { "physicalType": "BINARY", "logicalType": "STRING" },
      "repetition": "OPTIONAL"
    }
  ]
}
```

## Supported Parquet Types

See [docs/parquet-data-types.md](docs/parquet-data-types.md) for the complete type taxonomy and mapping to Iceberg/Pinot types.

### Physical Types
BOOLEAN, INT32, INT64, FLOAT, DOUBLE, BINARY, FIXED_LEN_BYTE_ARRAY, INT96

### Logical Types
INT(8/16/32/64), UINT(8/16/32/64), STRING, ENUM, JSON, BSON, UUID,
DATE, TIME(ms/us/ns), TIMESTAMP(ms/us/ns, UTC/local), DECIMAL

### Complex Types
STRUCT, LIST, MAP (including nested combinations)

### Multi-Value Types
LIST\<INT32\>, LIST\<INT64\>, LIST\<FLOAT\>, LIST\<DOUBLE\>, LIST\<STRING\>
(Parquet LIST with primitive elements → Pinot multi-value dimensions)

## Generating Pinot Schema & Table Config

Automatically generate Pinot schema and table config JSON files from the same config used for Parquet generation:

```bash
# Generate only Pinot configs (no Parquet data)
python -m generator.cli --config configs/iceberg-full-coverage.json --output output/ --pinot-config

# Generate both Parquet data AND Pinot configs
python -m generator.cli --config configs/iceberg-full-coverage.json --output output/ --pinot-config --parquet

# Use Iceberg mapping mode instead of the default direct-Parquet mapping
python -m generator.cli --config configs/all-primitive-types.json --output output/ --pinot-config --mapping-mode iceberg
```

This produces two JSON files in the output directory:

| File | Description |
|---|---|
| `{name}_pinot_schema.json` | Pinot schema with dimensionFieldSpecs, dateTimeFieldSpecs, complexFieldSpecs |
| `{name}_pinot_table_config.json` | OFFLINE table config with null handling, JSON index columns, time column |

### Mapping Modes

StarTree Pinot has **two separate code paths** that map Parquet types to Pinot types, and they don't always agree. The `--mapping-mode` flag controls which one the generator follows when producing Pinot configs:

**`parquet`** (default) -- Mirrors `ParquetToPinotTypeMapper`, the path used for **raw S3 catalog / direct Parquet file ingestion** without catalog metadata:

| Parquet Type | Pinot Type |
|---|---|
| DECIMAL | BIG_DECIMAL |
| JSON logical type | STRING |
| LIST\<primitive\> | Multi-value dimension (same primitive type) |
| LIST (nested/complex) | STRING (JSON serialized) |
| MAP (any) | STRING (JSON serialized) |
| STRUCT | STRING (JSON serialized) |

**`iceberg`** -- Mirrors `IcebergSchemaConverter`, the path used when data flows through **Iceberg tables with catalog metadata into Pinot**:

| Parquet Type | Pinot Type |
|---|---|
| DECIMAL | BIG_DECIMAL |
| JSON logical type | JSON |
| LIST of primitives | Multi-value column |
| MAP\<primitive, primitive\> | MAP (ComplexFieldSpec) |
| STRUCT / nested complex | JSON |

Use `parquet` mode (the default) when testing with raw S3 catalog table type where Parquet files sit in S3 without Iceberg catalog metadata. Use `iceberg` mode when testing the full Iceberg-to-Pinot pipeline with catalog metadata.

See [docs/parquet-data-types.md](docs/parquet-data-types.md) Section 8 for the full comparison and known gaps.

## Validating Output

```python
import pyarrow.parquet as pq

# Read back and inspect
table = pq.read_table("output/my_dataset.parquet")
print(table.schema)
print(table.to_pandas().head())

# Check metadata
metadata = pq.read_metadata("output/my_dataset.parquet")
print(metadata.schema)
print(f"Rows: {metadata.num_rows}, Row groups: {metadata.num_row_groups}")
```
