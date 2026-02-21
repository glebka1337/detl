import polars as pl
from typing import Callable, Dict, Any, List

PipelineHandler = Callable[[pl.DataFrame | pl.LazyFrame, Any], pl.DataFrame | pl.LazyFrame]

PIPELINE_REGISTRY: Dict[str, PipelineHandler] = {}

def register_pipeline_stage(stage_name: str) -> Callable[[PipelineHandler], PipelineHandler]:
    """Decorator to register a pipeline transformation stage."""
    def decorator(func: PipelineHandler) -> PipelineHandler:
        PIPELINE_REGISTRY[stage_name] = func
        return func
    return decorator

@register_pipeline_stage("mutate")
def _handle_mutate(df: pl.DataFrame | pl.LazyFrame, config: Dict[str, str]) -> pl.DataFrame | pl.LazyFrame:
    """Evaluates SQL expressions to create or modify columns natively."""
    for new_col, sql_expr in config.items():
        ctx = pl.SQLContext(frame=df)
        df = ctx.execute(f"SELECT *, ({sql_expr}) as {new_col} FROM frame")
    return df

@register_pipeline_stage("filter")
def _handle_filter(df: pl.DataFrame | pl.LazyFrame, sql_expr: str) -> pl.DataFrame | pl.LazyFrame:
    """Filters data based on a SQL WHERE-clause expression."""
    ctx = pl.SQLContext(frame=df)
    return ctx.execute(f"SELECT * FROM frame WHERE {sql_expr}")

@register_pipeline_stage("rename")
def _handle_rename(df: pl.DataFrame | pl.LazyFrame, config: Dict[str, str]) -> pl.DataFrame | pl.LazyFrame:
    """Renames columns based on a dictionary mapping {old_name: new_name}."""
    return df.rename(config)

@register_pipeline_stage("sort")
def _handle_sort(df: pl.DataFrame | pl.LazyFrame, config: Dict[str, Any]) -> pl.DataFrame | pl.LazyFrame:
    """Sorts data by specified column(s)."""
    by = config.get("by")
    order = config.get("order", "asc")
    descending = (order.lower() == "desc")
    
    if not by:
        raise ValueError("Sort stage requires a 'by' parameter specifying the column to sort on.")
        
    return df.sort(by, descending=descending)

def apply_pipeline(df: pl.DataFrame | pl.LazyFrame, pipeline: List[Dict[str, Any]]) -> pl.DataFrame | pl.LazyFrame:
    """
    Applies the ordered sequence of pipeline stages to the DataFrame.
    Leverages a dispatcher decorators mapping to native handlers.
    """
    if not pipeline:
        return df
        
    for stage in pipeline:
        for stage_name, config in stage.items():
            handler = PIPELINE_REGISTRY.get(stage_name)
            if not handler:
                raise NotImplementedError(f"Pipeline stage '{stage_name}' is not supported.")
            df = handler(df, config)
            
    return df
