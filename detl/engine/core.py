import polars as pl
from typing import Dict, Any, Callable

from detl.schema import Manifesto
from detl.engine.types import apply_types
from detl.engine.nulls import handle_nulls
from detl.engine.constraints import apply_constraints
from detl.engine.pipeline import apply_pipeline

class DetlEngine:
    """
    Main execution engine that applies a Declarative ETL Data Contract (Manifesto)
    onto a Polars DataFrame.
    """
    def __init__(self, manifest: Manifesto):
        self.manifest = manifest

    def execute(self, df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
        """
        Executes the entire manifest sequence on the DataFrame or LazyFrame.
        """
        df = self._drop_undefined_columns(df)
        df = self._apply_types_and_date_formats(df)
        df = self._handle_nulls(df)
        df = self._apply_constraints(df)
        df = self._handle_duplicates(df)
        df = self._run_pipeline(df)
        return df

    def _drop_undefined_columns(self, df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
        if self.manifest.conf.undefined_columns == "drop":
            defined = list(self.manifest.columns.keys())
            df_cols = df.collect_schema().names() if isinstance(df, pl.LazyFrame) else df.columns
            keep_cols = [c for c in df_cols if c in defined]
            df = df.select(keep_cols)
        return df

    def _apply_types_and_date_formats(self, df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
        df_cols = df.collect_schema().names() if isinstance(df, pl.LazyFrame) else df.columns
        for col_name, col_def in self.manifest.columns.items():
            if col_name in df_cols:
                df = apply_types(df, col_name, col_def)
        return df

    def _handle_nulls(self, df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
        df_cols = df.collect_schema().names() if isinstance(df, pl.LazyFrame) else df.columns
        for col_name, col_def in self.manifest.columns.items():
            if col_name in df_cols and col_def.on_null:
                df = handle_nulls(df, col_name, col_def.on_null)
        return df

    def _apply_constraints(self, df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
        df_cols = df.collect_schema().names() if isinstance(df, pl.LazyFrame) else df.columns
        for col_name, col_def in self.manifest.columns.items():
            if col_name in df_cols and col_def.constraints:
                df = apply_constraints(df, col_name, col_def.constraints)
        return df

    def _handle_duplicates(self, df: pl.DataFrame) -> pl.DataFrame:
        dup_conf = self.manifest.conf.on_duplicate_rows
        tactic = dup_conf.tactic
        subset = dup_conf.subset

        if tactic == "keep":
            return df
            
        if tactic == "drop_extras":
            return df.unique(subset=subset, maintain_order=False)

        if tactic == "fail":
            # Evaluates the duplicate check eagerly
            dup_check_df = df.group_by(subset).agg(pl.len().alias("count")).filter(pl.col("count") > 1)
            
            if isinstance(dup_check_df, pl.LazyFrame):
                has_duplicates = dup_check_df.select(pl.len() > 0).collect().item()
            else:
                has_duplicates = dup_check_df.height > 0
                
            if has_duplicates:
                raise ValueError("Duplicate rows detected and 'fail' tactic is active.")

        return df

    def _run_pipeline(self, df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
        if self.manifest.pipeline:
            df = apply_pipeline(df, self.manifest.pipeline)
        return df
