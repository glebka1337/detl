import abc
import polars as pl
from typing import Any

class Source(abc.ABC):
    """
    Abstract interface for all Extraction layer components.
    A Source represents an input dataset that can be lazily or eagerly scanned into Polars.
    """
    @abc.abstractmethod
    def read(self) -> pl.LazyFrame | pl.DataFrame:
        """
        Reads data from the external system and returns it as a Polars Frame.
        Must raise `ConnectionConfigurationError` on validation failures.
        """
        pass

class Sink(abc.ABC):
    """
    Abstract interface for all Load layer components.
    A Sink represents an external storage destination where processed outputs are deposited.
    """
    @abc.abstractmethod
    def write(self, df: pl.DataFrame | pl.LazyFrame) -> None:
        """
        Takes the finalized Polars Frame and executes the write mapping to the target system.
        Must raise `ConnectionConfigurationError` on destination issues.
        """
        pass
