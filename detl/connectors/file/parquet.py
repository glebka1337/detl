from pathlib import Path
from typing import Union
import polars as pl
from detl.connectors.base import Source, Sink
from detl.exceptions import ConnectionConfigurationError

class ParquetSource(Source):
    def __init__(self, path: Union[str, Path]):
        self.path = Path(path)
        
    def read(self) -> pl.LazyFrame:
        if not self.path.exists():
            raise ConnectionConfigurationError(f"Parquet source file '{self.path}' not found.")
        try:
            return pl.scan_parquet(self.path)
        except Exception as e:
            raise ConnectionConfigurationError(f"Failed to scan Parquet at '{self.path}': {e}")

class ParquetSink(Sink):
    def __init__(self, path: Union[str, Path], streaming: bool = True):
        self.path = Path(path)
        self.streaming = streaming
        
    def write(self, df: pl.DataFrame | pl.LazyFrame) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            if isinstance(df, pl.LazyFrame):
                df.sink_parquet(self.path)
            else:
                df.write_parquet(self.path)
        except Exception as e:
            raise ConnectionConfigurationError(f"Failed to write Parquet to '{self.path}': {e}")
