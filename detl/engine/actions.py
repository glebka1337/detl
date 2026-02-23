import polars as pl
from typing import Callable, Union, Dict
from detl.schema.common import StringViolateAction, NumericViolateAction
from detl.constants import StringActionTactic, NumericActionTactic
from detl.exceptions import ConstraintViolationError

ActionHandler = Callable[
    [pl.DataFrame, str, pl.Expr, Union[StringViolateAction, NumericViolateAction]],
    pl.DataFrame,
]

ACTION_REGISTRY: Dict[str, ActionHandler] = {}

def register_action(tactic_name: str) -> Callable[[ActionHandler], ActionHandler]:
    """Decorator to register a violation action handler."""
    def decorator(func: ActionHandler) -> ActionHandler:
        ACTION_REGISTRY[tactic_name] = func
        return func
    return decorator

@register_action(StringActionTactic.DROP_ROW)
@register_action(NumericActionTactic.DROP_ROW)
def _handle_drop_row(df: pl.DataFrame, col_name: str, mask: pl.Expr, action) -> pl.DataFrame:
    return df.filter(~mask.fill_null(False))

@register_action(StringActionTactic.FAIL)
@register_action(NumericActionTactic.FAIL)
def _handle_fail(df: pl.DataFrame | pl.LazyFrame, col_name: str, mask: pl.Expr, action) -> pl.DataFrame | pl.LazyFrame:
    checker = df.select(mask.fill_null(False))
    if isinstance(checker, pl.LazyFrame):
        has_failed = checker.collect().to_series().any()
    else:
        has_failed = checker.to_series().any()
        
    if has_failed:
        raise ConstraintViolationError(f"Constraint failed on column '{col_name}'.")
    return df

@register_action(StringActionTactic.FILL_VALUE)
@register_action(NumericActionTactic.FILL_VALUE)
def _handle_fill_value(df: pl.DataFrame, col_name: str, mask: pl.Expr, action) -> pl.DataFrame:
    if getattr(action, "value", None) is None:
        raise ConstraintViolationError(f"'fill_value' requires a 'value' parameter for '{col_name}'.")
    return df.with_columns(
        pl.when(mask).then(pl.lit(action.value)).otherwise(pl.col(col_name)).alias(col_name)
    )

@register_action(NumericActionTactic.FILL_MAX)
def _handle_fill_max(df: pl.DataFrame, col_name: str, mask: pl.Expr, action) -> pl.DataFrame:
    return df.with_columns(
        pl.when(mask).then(pl.col(col_name).max()).otherwise(pl.col(col_name)).alias(col_name)
    )

@register_action(NumericActionTactic.FILL_MIN)
def _handle_fill_min(df: pl.DataFrame, col_name: str, mask: pl.Expr, action) -> pl.DataFrame:
    return df.with_columns(
        pl.when(mask).then(pl.col(col_name).min()).otherwise(pl.col(col_name)).alias(col_name)
    )

@register_action(NumericActionTactic.FILL_MEAN)
def _handle_fill_mean(df: pl.DataFrame, col_name: str, mask: pl.Expr, action) -> pl.DataFrame:
    return df.with_columns(
        pl.when(mask).then(pl.col(col_name).mean()).otherwise(pl.col(col_name)).alias(col_name)
    )

@register_action(NumericActionTactic.FILL_MEDIAN)
def _handle_fill_median(df: pl.DataFrame, col_name: str, mask: pl.Expr, action) -> pl.DataFrame:
    return df.with_columns(
        pl.when(mask).then(pl.col(col_name).median()).otherwise(pl.col(col_name)).alias(col_name)
    )

def apply_violate_action(
    df: pl.DataFrame,
    col_name: str,
    mask: pl.Expr,
    action: Union[StringViolateAction, NumericViolateAction],
) -> pl.DataFrame:
    """
    Applies the specified tactic when a constraint is violated.
    Dispatches logic through the Action Registry decorators.
    """
    handler = ACTION_REGISTRY.get(action.tactic)
    if not handler:
        raise ConstraintViolationError(f"Tactic '{action.tactic}' is not supported for action mapping.")
    
    return handler(df, col_name, mask, action)
