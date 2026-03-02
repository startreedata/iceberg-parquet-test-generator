"""
Write Parquet files from a DatasetConfig with configurable compression,
row group size, dictionary encoding, and INT96 timestamp mode.
"""
from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .config_model import DatasetConfig
from .data_generator import generate_dataset
from .schema_builder import build_pyarrow_schema


def write_dataset(config: DatasetConfig, output_dir: str) -> str:
    """
    Generate data from config and write it to a Parquet file.

    Returns the path to the written file.
    """
    schema = build_pyarrow_schema(config)
    columns = generate_dataset(config)

    table = pa.table(columns, schema=schema)

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{config.name}.parquet")

    opts = config.writerOptions
    pq.write_table(
        table,
        output_path,
        compression=opts.compression,
        row_group_size=opts.rowGroupSize,
        use_dictionary=opts.useDictionary,
        write_statistics=opts.writeStatistics,
        use_deprecated_int96_timestamps=opts.useDeprecatedInt96Timestamps,
    )

    _print_summary(output_path, table, config)
    return output_path


def _print_summary(path: str, table: pa.Table, config: DatasetConfig) -> None:
    """Print a summary of the written Parquet file."""
    metadata = pq.read_metadata(path)
    file_size = os.path.getsize(path)

    print(f"\n{'=' * 60}")
    print(f"  Written: {path}")
    print(f"  Rows: {metadata.num_rows}")
    print(f"  Row groups: {metadata.num_row_groups}")
    print(f"  Columns: {metadata.num_columns}")
    print(f"  File size: {file_size:,} bytes")
    print(f"  Compression: {config.writerOptions.compression}")
    print(f"{'=' * 60}")

    print(f"\n  Schema:")
    for i, field in enumerate(table.schema):
        col = table.column(i)
        print(f"    {field.name}: {field.type} (nullable={field.nullable}, nulls={col.null_count}/{len(col)})")
    print()
