import polars as pl
from detl.constants import DType
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
        self._infer_schema(df)
        self._validate_schema_vs_data(df)

        df = self._drop_undefined_columns(df)
        df = self._apply_types_and_date_formats(df)
        df = self._handle_nulls(df)
        df = self._apply_constraints(df)
        df = self._handle_duplicates(df)
        df = self._run_pipeline(df)
        return df

    def _infer_schema(self, df: pl.DataFrame | pl.LazyFrame) -> None:
        """Dynamically infer schemas for 'keep' behavior without explicit YAML column mappings."""
        if self.manifest.conf.undefined_columns == "keep" and self.manifest.conf.defaults:
            schema = df.collect_schema() if isinstance(df, pl.LazyFrame) else df.schema
            for col_name, ptype in schema.items():
                if col_name not in self.manifest.columns:
                    # Resolve Polars Type to DType
                    dtype = None
                    if ptype in [pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64]:
                        dtype = DType.INT
                    elif ptype in [pl.Float32, pl.Float64]:
                        dtype = DType.FLOAT
                    elif ptype == pl.Utf8 or ptype == pl.String:
                        dtype = DType.STRING
                    elif ptype == pl.Boolean:
                        dtype = DType.BOOLEAN
                    elif ptype == pl.Date:
                        dtype = DType.DATE
                    elif ptype == pl.Datetime:
                        dtype = DType.DATETIME

                    if dtype:
                        # Construct a generic column and inject the global defaults
                        from detl.schema import ColumnDef
                        col_def = ColumnDef(dtype=dtype)
                        default_policy = self.manifest.conf.defaults.get(dtype)
                        if default_policy:
                            if default_policy.on_null:
                                col_def.on_null = default_policy.on_null
                            if default_policy.constraints:
                                col_def.constraints = default_policy.constraints
                        self.manifest.columns[col_name] = col_def

    def _validate_schema_vs_data(self, df: pl.DataFrame | pl.LazyFrame) -> None:
        """Fail-fast checking to prevent late evaluations from crashing inexplicably."""
        real_cols = df.collect_schema().names() if isinstance(df, pl.LazyFrame) else df.columns

        # Check duplicate configurations
        dup_subset = self.manifest.conf.on_duplicate_rows.subset
        if dup_subset:
            for col in dup_subset:
                if col not in real_cols:
                    raise ValueError(f"Subset column '{col}' for duplicate rows check does not exist in the dataset.")

        # Check explicit column mappings and renames
        for col_name, col_def in self.manifest.columns.items():
            if col_name not in real_cols:
                raise ValueError(f"Manifest column '{col_name}' does not exist in the dataset.")

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
