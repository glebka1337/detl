import argparse
import sys
import yaml
from pathlib import Path

import polars as pl
from pydantic import ValidationError
from rich.console import Console
from rich.panel import Panel
from rich.theme import Theme

from detl.schema import Manifesto
from detl.engine.core import DetlEngine

custom_theme = Theme({
    "info": "cyan",
    "warning": "yellow",
    "error": "bold red",
    "success": "bold green"
})

console = Console(theme=custom_theme)

def main():
    parser = argparse.ArgumentParser(
        description="detl (Declarative ETL) - Strict, modular CLI utility for declarative data cleaning and transformation."
    )
    parser.add_argument("-f", "--config", type=Path, required=True, help="Path to the YAML manifest (Data Contract).")
    parser.add_argument("-i", "--input", type=Path, required=True, help="Input data file (.csv or .parquet).")
    parser.add_argument("-o", "--output", type=Path, required=True, help="Output data file (.csv or .parquet).")

    args = parser.parse_args()

    # 1. Load configuration
    console.print(f"[*] Reading manifest [bold]{args.config.name}[/bold]...", style="info")
    try:
        if not args.config.exists():
            raise FileNotFoundError(f"Manifest file '{args.config}' not found.")
            
        with open(args.config, "r", encoding="utf-8") as f:
            yaml_data = yaml.safe_load(f)
            if yaml_data is None:
                yaml_data = {}
                
        manifest = Manifesto(**yaml_data)
        
    except FileNotFoundError as e:
        console.print(f"[error]Error:[/error] {e}")
        sys.exit(1)
    except yaml.YAMLError as e:
        console.print(f"[error]YAML parsing error:[/error]\n{e}")
        sys.exit(1)
    except ValidationError as e:
        console.print(Panel.fit(f"[error]Manifest (Data Contract) validation errors:[/error]"))
        for err in e.errors():
            loc = " -> ".join([str(x) for x in err["loc"]])
            msg = err["msg"]
            console.print(f"  [warning]• Location:[/warning] {loc}")
            console.print(f"    [error]Error:[/error] {msg}")
        sys.exit(1)
    except Exception as e:
        console.print(f"[error]Unknown error while loading manifest:[/error] {e}")
        sys.exit(1)

    console.print("[success]Manifest successfully validated.[/success]")

    # 2. Read data
    console.print(f"[*] Scanning raw data from [bold]{args.input.name}[/bold]...", style="info")
    try:
        if not args.input.exists():
            raise FileNotFoundError(f"Input data file '{args.input}' not found.")
            
        ext = args.input.suffix.lower()
        if ext == ".csv":
            df = pl.scan_csv(args.input)
        elif ext == ".parquet":
            df = pl.scan_parquet(args.input)
        else:
            raise ValueError(f"Unsupported input format: {ext}. Supported formats are .csv and .parquet")
            
    except FileNotFoundError as e:
        console.print(f"[error]Error:[/error] {e}")
        sys.exit(1)
    except Exception as e:
        console.print(f"[error]Error scanning data:[/error] {e}")
        sys.exit(1)

    console.print(f"[success]Data scanned into LazyFrame.[/success]")

    # 3. Execute pipeline
    console.print("[*] Running ETL pipeline (Polars Engine)...", style="info")
    try:
        engine = DetlEngine(manifest)
        processed_df = engine.execute(df)
    except pl.exceptions.PolarsError as e:
        console.print(f"[error]Polars computation error:[/error]\n{e}")
        sys.exit(1)
    except ValueError as e:
        console.print(f"[error]Data logic/constraint error:[/error]\n{e}")
        sys.exit(1)
    except Exception as e:
        console.print(f"[error]Critical pipeline error:[/error]\n{e}")
        sys.exit(1)

    console.print(f"[success]Pipeline execution graph constructed successfully.[/success]")

    # 4. Save data (Evaluation)
    console.print(f"[*] Evaluating graph and writing results to [bold]{args.output.name}[/bold]...", style="info")
    try:
        out_ext = args.output.suffix.lower()
        args.output.parent.mkdir(parents=True, exist_ok=True)
        
        if out_ext == ".csv":
            processed_df.sink_csv(args.output)
        elif out_ext == ".parquet":
            processed_df.sink_parquet(args.output)
        else:
            raise ValueError(f"Unsupported output format: {out_ext}. Supported formats are .csv and .parquet")
            
    except Exception as e:
        console.print(f"[error]Error writing data:[/error] {e}")
        sys.exit(1)

    console.print(f"[success]Done! Results successfully saved to: {args.output.resolve()}[/success]")

if __name__ == "__main__":
    main()
