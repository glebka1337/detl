import polars as pl
from typing import Callable, Dict
from detl.schema import ColumnDef
from detl.constants import DType
from detl.exceptions import TypeCastingError

TypeCaster = Callable[[pl.DataFrame, str, ColumnDef], pl.DataFrame]

TYPE_REGISTRY: Dict[str, TypeCaster] = {}

def register_type(type_name: str) -> Callable[[TypeCaster], TypeCaster]:
    """Decorator to register a specific dtype column caster."""
    def decorator(func: TypeCaster) -> TypeCaster:
        TYPE_REGISTRY[type_name] = func
        return func
    return decorator

@register_type(DType.STRING)
def _cast_string(df: pl.DataFrame, col_name: str, col_def: ColumnDef) -> pl.DataFrame:
    return df.with_columns(pl.col(col_name).cast(pl.Utf8))

@register_type(DType.INT)
def _cast_int(df: pl.DataFrame, col_name: str, col_def: ColumnDef) -> pl.DataFrame:
    return df.with_columns(pl.col(col_name).cast(pl.Int64, strict=False))

@register_type(DType.FLOAT)
def _cast_float(df: pl.DataFrame, col_name: str, col_def: ColumnDef) -> pl.DataFrame:
    return df.with_columns(pl.col(col_name).cast(pl.Float64, strict=False))

@register_type(DType.BOOLEAN)
def _cast_boolean(df: pl.DataFrame, col_name: str, col_def: ColumnDef) -> pl.DataFrame:
    return df.with_columns(pl.col(col_name).cast(pl.Boolean, strict=False))

@register_type(DType.DATE)
def _cast_date(df: pl.DataFrame, col_name: str, col_def: ColumnDef) -> pl.DataFrame:
    return _parse_temporal(df, col_name, col_def, pl.Date)

@register_type(DType.DATETIME)
def _cast_datetime(df: pl.DataFrame, col_name: str, col_def: ColumnDef) -> pl.DataFrame:
    return _parse_temporal(df, col_name, col_def, pl.Datetime)

def _parse_temporal(df: pl.DataFrame, col_name: str, col_def: ColumnDef, target_type) -> pl.DataFrame:
    """Helper method properly utilizing string format configuration."""
    if col_def.date_format:
        fmt = col_def.date_format.in_format
        error_tactic = col_def.date_format.on_parse_error.get("tactic", "drop_row")
        strict = (error_tactic == "fail")
        
        # If the dataframe column is entirely null, Polars types it as pl.Null, which crashes str.strptime
        if df.schema[col_name] == pl.Null:
            parsed = pl.col(col_name).cast(target_type)
        else:
            # Cast to Utf8 first to guarantee strptime behaves correctly even with mixed types
            parsed = pl.col(col_name).cast(pl.Utf8).str.strptime(target_type, format=fmt, strict=strict)
            
        return df.with_columns(parsed.alias(col_name))
    
    return df.with_columns(pl.col(col_name).cast(target_type, strict=False))

def apply_types(df: pl.DataFrame, col_name: str, col_def: ColumnDef) -> pl.DataFrame:
    """
    Casts a single column to its specified dtype and applies date parsing if needed.
    """
    handler = TYPE_REGISTRY.get(col_def.dtype)
    if not handler:
        raise TypeCastingError(f"Type mapping for '{col_def.dtype}' is currently unsupported.")
    return handler(df, col_name, col_def)
