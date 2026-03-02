"""
CLI entry point for the Parquet test generator.

Usage:
    python -m generator.cli --config configs/all-primitive-types.json --output output/
    python -m generator.cli --config configs/all-logical-types.json --output output/ --num-rows 5000
    python -m generator.cli --config configs/iceberg-full-coverage.json --output output/ --seed 123

    # Generate Pinot schema + table config (without Parquet data)
    python -m generator.cli --config configs/iceberg-full-coverage.json --output output/ --pinot-config

    # Generate both Parquet data AND Pinot configs
    python -m generator.cli --config configs/iceberg-full-coverage.json --output output/ --pinot-config --parquet

    # Use the Iceberg mapping mode instead of direct-Parquet
    python -m generator.cli --config configs/all-primitive-types.json --output output/ --pinot-config --mapping-mode iceberg
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .config_model import DatasetConfig
from .parquet_writer import write_dataset
from .pinot_config_generator import MappingMode, write_pinot_configs


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate Parquet test datasets and Pinot configs from JSON config files",
        prog="parquet-gen",
    )
    parser.add_argument(
        "--config", "-c",
        required=True,
        help="Path to a JSON config file",
    )
    parser.add_argument(
        "--output", "-o",
        default="output/",
        help="Output directory for generated files (default: output/)",
    )
    parser.add_argument(
        "--num-rows", "-n",
        type=int,
        default=None,
        help="Override numRows from the config",
    )
    parser.add_argument(
        "--seed", "-s",
        type=int,
        default=None,
        help="Override seed from the config",
    )
    parser.add_argument(
        "--pinot-config",
        action="store_true",
        default=False,
        help="Generate Pinot schema and table config JSON files",
    )
    parser.add_argument(
        "--parquet",
        action="store_true",
        default=False,
        help="Generate Parquet data file (default when --pinot-config is not set)",
    )
    parser.add_argument(
        "--mapping-mode",
        choices=["parquet", "iceberg"],
        default="parquet",
        help="Type mapping mode: 'parquet' (ParquetToPinotTypeMapper) or 'iceberg' (IcebergSchemaConverter). Default: parquet",
    )

    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Error: config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    with open(config_path) as f:
        raw = json.load(f)

    if args.num_rows is not None:
        raw["numRows"] = args.num_rows
    if args.seed is not None:
        raw["seed"] = args.seed

    config = DatasetConfig(**raw)
    mode = MappingMode(args.mapping_mode)

    # Default behavior: generate Parquet when --pinot-config is not specified
    generate_parquet = args.parquet or not args.pinot_config
    generate_pinot = args.pinot_config

    if generate_parquet:
        print(f"Generating dataset '{config.name}' with {config.numRows} rows, seed={config.seed} ...")
        output_path = write_dataset(config, args.output)
        print(f"Parquet file written to: {output_path}")

    if generate_pinot:
        schema_path, table_path = write_pinot_configs(config, args.output, mode)
        print(f"Pinot schema written to:       {schema_path}")
        print(f"Pinot table config written to: {table_path}")


if __name__ == "__main__":
    main()
