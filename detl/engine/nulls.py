import polars as pl
from typing import Callable, Dict

from detl.schema import ColumnDef
from detl.constants import NullTactic, DType
from detl.exceptions import NullViolationError

NullHandler = Callable[[pl.DataFrame, str, ColumnDef], pl.DataFrame]

NULL_REGISTRY: Dict[str, NullHandler] = {}

def register_null_handler(tactic_name: str) -> Callable[[NullHandler], NullHandler]:
    """Decorator to register a null imputation tactic."""
    def decorator(func: NullHandler) -> NullHandler:
        NULL_REGISTRY[tactic_name] = func
        return func
    return decorator

@register_null_handler(NullTactic.DROP_ROW)
def _handle_drop_row(df: pl.DataFrame, col_name: str, col_def: ColumnDef) -> pl.DataFrame:
    return df.filter(~pl.col(col_name).is_null())

@register_null_handler(NullTactic.FAIL)
def _handle_fail(df: pl.DataFrame | pl.LazyFrame, col_name: str, col_def: ColumnDef) -> pl.DataFrame | pl.LazyFrame:
    checker = df.select(pl.col(col_name).is_null())
    if isinstance(checker, pl.LazyFrame):
        has_failed = checker.collect().to_series().any()
    else:
        has_failed = checker.to_series().any()
        
    if has_failed:
        raise NullViolationError(f"Column '{col_name}' contains null values which is forbidden by 'fail' tactic.")
    return df

@register_null_handler(NullTactic.FILL_VALUE)
def _handle_fill_value(df: pl.DataFrame, col_name: str, col_def: ColumnDef) -> pl.DataFrame:
    if col_def.dtype in [DType.DATE, DType.DATETIME]:
        tgt_type = pl.Date if col_def.dtype == DType.DATE else pl.Datetime
        
        if col_def.date_format:
            fill_expr = pl.lit(col_def.on_null.value).str.strptime(tgt_type, format=col_def.date_format.in_format, strict=True)
        else:
            fill_expr = pl.lit(col_def.on_null.value).cast(tgt_type, strict=True)
             
        return df.with_columns(pl.col(col_name).fill_null(fill_expr))
        
    return df.with_columns(pl.col(col_name).fill_null(col_def.on_null.value))

@register_null_handler(NullTactic.FILL_MEAN)
def _handle_fill_mean(df: pl.DataFrame, col_name: str, col_def: ColumnDef) -> pl.DataFrame:
    return df.with_columns(pl.col(col_name).fill_null(pl.col(col_name).mean()))

@register_null_handler(NullTactic.FILL_MEDIAN)
def _handle_fill_median(df: pl.DataFrame, col_name: str, col_def: ColumnDef) -> pl.DataFrame:
    return df.with_columns(pl.col(col_name).fill_null(pl.col(col_name).median()))

@register_null_handler(NullTactic.FILL_MAX)
def _handle_fill_max(df: pl.DataFrame, col_name: str, col_def: ColumnDef) -> pl.DataFrame:
    return df.with_columns(pl.col(col_name).fill_null(pl.col(col_name).max()))

@register_null_handler(NullTactic.FILL_MIN)
def _handle_fill_min(df: pl.DataFrame, col_name: str, col_def: ColumnDef) -> pl.DataFrame:
    return df.with_columns(pl.col(col_name).fill_null(pl.col(col_name).min()))

@register_null_handler(NullTactic.FILL_MOST_FREQUENT)
def _handle_fill_most_frequent(df: pl.DataFrame, col_name: str, col_def: ColumnDef) -> pl.DataFrame:
    return df.with_columns(pl.col(col_name).fill_null(pl.col(col_name).mode().first()))

@register_null_handler(NullTactic.FFILL)
def _handle_ffill(df: pl.DataFrame, col_name: str, col_def: ColumnDef) -> pl.DataFrame:
    return df.with_columns(pl.col(col_name).fill_null(strategy="forward"))

@register_null_handler(NullTactic.BFILL)
def _handle_bfill(df: pl.DataFrame, col_name: str, col_def: ColumnDef) -> pl.DataFrame:
    return df.with_columns(pl.col(col_name).fill_null(strategy="backward"))

def handle_nulls(df: pl.DataFrame, col_name: str, col_def: ColumnDef) -> pl.DataFrame:
    """
    Applies the specified null imputation tactic via dispatch decorator registry.
    """
    handler = NULL_REGISTRY.get(col_def.on_null.tactic)
    if not handler:
        raise NullViolationError(f"Null tactic '{col_def.on_null.tactic}' mapping is missing.")
    return handler(df, col_name, col_def)
