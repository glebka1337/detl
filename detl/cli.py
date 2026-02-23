import argparse
import sys
import yaml
from pathlib import Path
from typing import Optional

import polars as pl
from pydantic import ValidationError
from rich.console import Console
from rich.panel import Panel
from rich.theme import Theme

from detl.schema import Manifesto
from detl.config import Config
from detl.core import Processor
from detl.exceptions import DetlException, ConnectionConfigurationError

from detl.connectors import (
    Source, Sink,
    CsvSource, CsvSink,
    ParquetSource, ParquetSink,
    ExcelSource, ExcelSink,
    PostgresSource, PostgresSink,
    MySQLSource, MySQLSink,
    SQLiteSource, SQLiteSink,
    S3Source, S3Sink
)

custom_theme = Theme({
    "info": "cyan",
    "warning": "yellow",
    "error": "bold red",
    "success": "bold green"
})

console = Console(theme=custom_theme)

def build_source(args: argparse.Namespace) -> Source:
    """Instantiate the appropriate Source connector based on CLI arguments.

    Args:
        args (argparse.Namespace): Parsed command-line arguments.

    Returns:
        Source: A configured Source connector instance.

    Raises:
        ValueError: If an unsupported format or unknown source type is provided.
    """
    if args.input:
        path = Path(args.input)
        ext = path.suffix.lower()
        if ext == ".csv": return CsvSource(path)
        if ext == ".parquet": return ParquetSource(path)
        if ext in (".xls", ".xlsx"): return ExcelSource(path)
        raise ValueError(f"Unsupported input format: {ext}")
        
    stype = args.source_type
    if not stype or not args.source_uri:
        raise ValueError("Must provide either --input OR (--source-type AND --source-uri)")
        
    try:
        if args.source_batch_size:
            args.source_batch_size = int(args.source_batch_size)
    except ValueError:
        raise ValueError("source-batch-size must be an integer")
        
    if stype == "postgres": return PostgresSource(args.source_uri, args.source_query, batch_size=args.source_batch_size)
    if stype == "mysql": return MySQLSource(args.source_uri, args.source_query, batch_size=args.source_batch_size)
    if stype == "sqlite": return SQLiteSource(args.source_uri, args.source_query, batch_size=args.source_batch_size)
    if stype == "csv": return CsvSource(args.source_uri)
    if stype == "parquet": return ParquetSource(args.source_uri)
    if stype == "excel": return ExcelSource(args.source_uri)
    if stype == "s3": return S3Source(args.source_uri, endpoint_url=args.s3_endpoint_url)
    raise ValueError(f"Unknown source type: {stype}")

def build_sink(args: argparse.Namespace) -> Sink:
    """Instantiate the appropriate Sink connector based on CLI arguments.

    Args:
        args (argparse.Namespace): Parsed command-line arguments.

    Returns:
        Sink: A configured Sink connector instance.

    Raises:
        ValueError: If an unsupported format or unknown sink type is provided.
    """
    if args.output:
        path = Path(args.output)
        ext = path.suffix.lower()
        if ext == ".csv": return CsvSink(path)
        if ext == ".parquet": return ParquetSink(path)
        if ext in (".xls", ".xlsx"): return ExcelSink(path)
        raise ValueError(f"Unsupported output format: {ext}")
        
    stype = args.sink_type
    if not stype or not args.sink_uri:
        raise ValueError("Must provide either --output OR (--sink-type AND --sink-uri)")
        
    try:
        if args.sink_batch_size:
            args.sink_batch_size = int(args.sink_batch_size)
    except ValueError:
        raise ValueError("sink-batch-size must be an integer")
        
    if stype == "postgres": return PostgresSink(args.sink_uri, args.sink_table, batch_size=args.sink_batch_size)
    if stype == "mysql": return MySQLSink(args.sink_uri, args.sink_table, batch_size=args.sink_batch_size)
    if stype == "sqlite": return SQLiteSink(args.sink_uri, args.sink_table, batch_size=args.sink_batch_size)
    if stype == "csv": return CsvSink(args.sink_uri)
    if stype == "parquet": return ParquetSink(args.sink_uri)
    if stype == "excel": return ExcelSink(args.sink_uri)
    if stype == "s3": return S3Sink(args.sink_uri, endpoint_url=args.s3_endpoint_url)
    raise ValueError(f"Unknown sink type: {stype}")

def main() -> None:
    """Main entrypoint for the detl CLI.

    Parses arguments, evaluates the declarative configuration, initializes connectors,
    and executes the pipeline.
    """
    parser = argparse.ArgumentParser(
        description="detl (Declarative ETL) - Strict, modular CLI utility for declarative data cleaning and transformation."
    )
    parser.add_argument("-f", "--config", type=Path, required=True, help="Path to the YAML manifest (Data Contract).")
    
    # Legacy file shortcuts
    parser.add_argument("-i", "--input", type=str, required=False, help="Input data file (.csv, .parquet, .xlsx).")
    parser.add_argument("-o", "--output", type=str, required=False, help="Output data file (.csv, .parquet, .xlsx).")
    
    # Advanced Connector args
    parser.add_argument("--source-type", type=str, required=False, help="Source connector type (postgres, mysql, sqlite, csv, parquet, excel, s3)")
    parser.add_argument("--source-uri", type=str, required=False, help="Connection string or filepath for Source")
    parser.add_argument("--source-query", type=str, required=False, help="SQL Query to execute for Database sources")
    parser.add_argument("--source-batch-size", type=str, required=False, help="Optional batch size for chunked database reading")
    
    parser.add_argument("--sink-type", type=str, required=False, help="Sink connector type (postgres, mysql, sqlite, csv, parquet, excel, s3)")
    parser.add_argument("--sink-uri", type=str, required=False, help="Connection string or filepath for Sink")
    parser.add_argument("--sink-table", type=str, required=False, help="Target table name for Database sinks")
    parser.add_argument("--sink-batch-size", type=str, required=False, help="Optional batch size for chunked database writing")
    
    parser.add_argument("--s3-endpoint-url", type=str, required=False, help="Optional Endpoint URL for S3/MinIO connections.")

    args = parser.parse_args()

    console.print("[success]Manifest successfully validated.[/success]")

    try:
        source_connector = build_source(args)
        sink_connector = build_sink(args)
    except Exception as e:
        console.print(f"[error]Connector Initialization Error:[/error] {e}")
        sys.exit(1)

    try:
        processor = Processor(config)
        processor.execute(source=source_connector, sink=sink_connector)
    except pl.exceptions.PolarsError as e:
        console.print(f"[error]Polars computation error:[/error]\n{e}")
        sys.exit(1)
    except ConnectionConfigurationError as e:
        console.print(f"[error]Source/Sink Connection Error:[/error]\n{e}")
        sys.exit(1)
    except DetlException as e:
        console.print(f"[error]Data contract violation/config error:[/error]\n{e}")
        sys.exit(1)
    except Exception as e:
        console.print(f"[error]Critical pipeline error:[/error]\n{e}")
        sys.exit(1)

    console.print(f"[success]Done! Results successfully saved to output connector.[/success]")

if __name__ == "__main__":
    main()
