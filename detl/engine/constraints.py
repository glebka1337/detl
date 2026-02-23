import polars as pl
from pathlib import Path
from typing import Callable, Any, Dict

from detl.schema.constraints import (
    ConstraintsDef, MinPolicy, MaxPolicy, RegexPolicy, 
    StringLengthPolicy, AllowedValuesPolicy, CustomExprPolicy, UniqueConstraint
)
from detl.exceptions import ConstraintViolationError, DuplicateRowError
from detl.engine.actions import apply_violate_action

ConstraintHandler = Callable[[pl.DataFrame, str, Any], pl.DataFrame]

CONSTRAINT_REGISTRY: Dict[str, ConstraintHandler] = {}

def register_constraint(constraint_name: str) -> Callable[[ConstraintHandler], ConstraintHandler]:
    """Decorator to register a data quality constraint evaluator."""
    def decorator(func: ConstraintHandler) -> ConstraintHandler:
        CONSTRAINT_REGISTRY[constraint_name] = func
        return func
    return decorator

@register_constraint("min_policy")
def _apply_min_policy(df: pl.DataFrame, col_name: str, policy: MinPolicy) -> pl.DataFrame:
    return apply_violate_action(df, col_name, pl.col(col_name) < policy.threshold, policy.violate_action)

@register_constraint("max_policy")
def _apply_max_policy(df: pl.DataFrame, col_name: str, policy: MaxPolicy) -> pl.DataFrame:
    return apply_violate_action(df, col_name, pl.col(col_name) > policy.threshold, policy.violate_action)

@register_constraint("regex")
def _apply_regex(df: pl.DataFrame, col_name: str, policy: RegexPolicy) -> pl.DataFrame:
    return apply_violate_action(df, col_name, ~pl.col(col_name).str.contains(policy.pattern), policy.violate_action)

@register_constraint("min_length")
def _apply_min_length(df: pl.DataFrame, col_name: str, policy: StringLengthPolicy) -> pl.DataFrame:
    return apply_violate_action(df, col_name, pl.col(col_name).str.len_chars() < policy.length, policy.violate_action)

@register_constraint("max_length")
def _apply_max_length(df: pl.DataFrame, col_name: str, policy: StringLengthPolicy) -> pl.DataFrame:
    return apply_violate_action(df, col_name, pl.col(col_name).str.len_chars() > policy.length, policy.violate_action)

@register_constraint("allowed_values")
def _apply_allowed_values(df: pl.DataFrame, col_name: str, policy: AllowedValuesPolicy) -> pl.DataFrame:
    valid_set = []
    if policy.values is not None:
        valid_set = policy.values
    elif policy.source is not None:
        path = Path(policy.source)
        if not path.exists():
            raise ConstraintViolationError(f"Allowed values source '{path}' not found for column '{col_name}'.")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if path.suffix == ".csv" or policy.separator in content:
                valid_set = [x.strip() for x in content.split(policy.separator) if x.strip()]
            else:
                valid_set = [line.strip() for line in content.splitlines() if line.strip()]

    return apply_violate_action(df, col_name, ~pl.col(col_name).is_in(valid_set), policy.violate_action)

@register_constraint("custom_expr")
def _apply_custom_expr(df: pl.DataFrame | pl.LazyFrame, col_name: str, policy: CustomExprPolicy) -> pl.DataFrame | pl.LazyFrame:
    ctx = pl.SQLContext(frame=df)
    res = ctx.execute(f"SELECT *, ({policy.expr}) as __is_valid FROM frame")
    mask = ~pl.col("__is_valid")
    
    df = apply_violate_action(res, col_name, mask, policy.violate_action)
    df = df.drop("__is_valid")
    return df

@register_constraint("unique")
def _apply_unique(df: pl.DataFrame | pl.LazyFrame, col_name: str, policy: UniqueConstraint) -> pl.DataFrame | pl.LazyFrame:
    if policy.tactic == "drop_extras":
        return df.unique(subset=[col_name], maintain_order=False)
    if policy.tactic == "fail":
        dup_check_df = df.group_by([col_name]).agg(pl.len().alias("count")).filter(pl.col("count") > 1)
        if isinstance(dup_check_df, pl.LazyFrame):
            has_duplicates = dup_check_df.select(pl.len() > 0).collect().item()
        else:
            has_duplicates = dup_check_df.height > 0
            
        if has_duplicates:
            raise DuplicateRowError(f"Unique constraint failed on column '{col_name}'.")
    return df

def apply_constraints(df: pl.DataFrame, col_name: str, constraints: ConstraintsDef) -> pl.DataFrame:
    """
    Evaluates and enforces data quality constraints defined in the schema for a single column.
    Leverages a dispatcher decorators mapping to handlers natively parsing policies.
    """
    for field_name in ConstraintsDef.model_fields.keys():
        policy = getattr(constraints, field_name)
        if policy is not None:
            handler = CONSTRAINT_REGISTRY.get(field_name)
            if handler:
                df = handler(df, col_name, policy)
            else:
                raise ConstraintViolationError(f"Constraint handler for '{field_name}' not implemented.")
    return df
