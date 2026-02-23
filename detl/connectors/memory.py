import polars as pl
from detl.connectors.base import Source, Sink

class MemorySource(Source):
    """A simple connector that yields a predefined Polars Frame from memory. Useful for testing."""
    def __init__(self, df: pl.DataFrame | pl.LazyFrame):
        self._df = df
        
    def read(self) -> pl.DataFrame | pl.LazyFrame:
        return self._df

class MemorySink(Sink):
    """A simple connector that traps the output Frame in memory. Useful for testing."""
    def __init__(self):
        self.result: pl.DataFrame | pl.LazyFrame | None = None
        
    def write(self, df: pl.DataFrame | pl.LazyFrame) -> None:
        self.result = df
