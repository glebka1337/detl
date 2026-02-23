from pathlib import Path
from typing import Union
import polars as pl
from detl.connectors.base import Source, Sink
from detl.exceptions import ConnectionConfigurationError

class ExcelSource(Source):
    def __init__(self, path: Union[str, Path], sheet_name: str | None = None):
        self.path = Path(path)
        self.sheet_name = sheet_name
        
    def read(self) -> pl.DataFrame:
        if not self.path.exists():
            raise ConnectionConfigurationError(f"Excel source file '{self.path}' not found.")
        try:
            return pl.read_excel(self.path, sheet_name=self.sheet_name)
        except Exception as e:
            raise ConnectionConfigurationError(f"Failed to read Excel at '{self.path}': {e}")

class ExcelSink(Sink):
    def __init__(self, path: Union[str, Path], sheet_name: str = "Sheet1"):
        self.path = Path(path)
        self.sheet_name = sheet_name
        
    def write(self, df: pl.DataFrame | pl.LazyFrame) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            if isinstance(df, pl.LazyFrame):
                df = df.collect()
            df.write_excel(self.path, worksheet=self.sheet_name)
        except Exception as e:
            raise ConnectionConfigurationError(f"Failed to write Excel to '{self.path}': {e}")
