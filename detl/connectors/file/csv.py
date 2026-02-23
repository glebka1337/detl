from pathlib import Path
from typing import Union
import polars as pl
from detl.connectors.base import Source, Sink
from detl.exceptions import ConnectionConfigurationError

class CsvSource(Source):
    def __init__(self, path: Union[str, Path], separator: str = ","):
        self.path = Path(path)
        self.separator = separator
        
    def read(self) -> pl.LazyFrame:
        if not self.path.exists():
            raise ConnectionConfigurationError(f"CSV source file '{self.path}' not found.")
        try:
            return pl.scan_csv(self.path, separator=self.separator)
        except Exception as e:
            raise ConnectionConfigurationError(f"Failed to scan CSV at '{self.path}': {e}")

class CsvSink(Sink):
    def __init__(self, path: Union[str, Path], separator: str = ",", streaming: bool = True):
        self.path = Path(path)
        self.separator = separator
        self.streaming = streaming
        
    def write(self, df: pl.DataFrame | pl.LazyFrame) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            if isinstance(df, pl.LazyFrame):
                try:
                    df.collect(streaming=self.streaming).write_csv(self.path, separator=self.separator)
                except Exception:
                    df.sink_csv(self.path)
            else:
                df.write_csv(self.path, separator=self.separator)
        except Exception as e:
            raise ConnectionConfigurationError(f"Failed to write CSV to '{self.path}': {e}")
