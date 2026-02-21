from typing import Optional, Union, Dict, List, Any, Literal
from pydantic import BaseModel, Field, model_validator
from pathlib import Path
import datetime

from detl.constants import (
    DType,
    NullTactic,
    DupTactic,
    StringActionTactic,
    NumericActionTactic
)

class StringViolateAction(BaseModel):
    tactic: StringActionTactic
    value: Optional[str] = None

    @model_validator(mode='after')
    def check_logic(self) -> 'StringViolateAction':
        if self.tactic == "fill_value" and self.value is None:
            raise ValueError("Tactic 'fill_value' requires a 'value' parameter.")
        if self.tactic != "fill_value" and self.value is not None:
            raise ValueError(f"Parameter 'value' is forbidden for tactic '{self.tactic}'.")
        return self

class NumericViolateAction(BaseModel):
    tactic: NumericActionTactic
    value: Optional[Union[int, float]] = None

    @model_validator(mode='after')
    def check_logic(self) -> 'NumericViolateAction':
        needs_value = ["fill_value"]
        if self.tactic in needs_value and self.value is None:
            raise ValueError(f"Tactic '{self.tactic}' requires a 'value' parameter.")
        if self.tactic not in needs_value and self.value is not None:
            raise ValueError(f"Parameter 'value' is forbidden for dynamically computed tactic '{self.tactic}'.")
        return self


class NullPolicy(BaseModel):
    tactic: NullTactic
    value: Optional[Union[int, float, str, bool]] = None

    @model_validator(mode='after')
    def check_null_logic(self) -> 'NullPolicy':
        if self.tactic == "fill_value" and self.value is None:
            raise ValueError("Tactic 'fill_value' requires a 'value' parameter.")
        if self.tactic != "fill_value" and self.value is not None:
            raise ValueError(f"Parameter 'value' allowed only for 'fill_value'.")
        return self

class DateFormatConfig(BaseModel):
    in_format: str = Field(alias="input")
    out_format: str = Field(alias="output")
    on_parse_error: Dict[str, Literal["drop_row", "fail"]] = Field(default_factory=lambda: {"tactic": "drop_row"})

class UniqueConstraint(BaseModel):
    tactic: DupTactic

class MinPolicy(BaseModel):
    threshold: Union[int, float, str] # Also string representation of dates
    violate_action: NumericViolateAction

    @model_validator(mode='after')
    def check_no_max(self) -> 'MinPolicy':
        if self.violate_action.tactic == "fill_max":
            raise ValueError("'fill_max' makes no sense inside 'min_policy'. Use 'fill_min'.")
        return self

class MaxPolicy(BaseModel):
    threshold: Union[int, float, str] # Also string representation of dates
    violate_action: NumericViolateAction

    @model_validator(mode='after')
    def check_no_min(self) -> 'MaxPolicy':
        if self.violate_action.tactic == "fill_min":
            raise ValueError("'fill_min' makes no sense inside 'max_policy'. Use 'fill_max'.")
        return self

class RegexPolicy(BaseModel):
    pattern: str
    violate_action: StringViolateAction

class StringLengthPolicy(BaseModel):
    length: int
    violate_action: StringViolateAction

    @model_validator(mode='after')
    def check_positive(self) -> 'StringLengthPolicy':
        if self.length < 0:
            raise ValueError("Length must be a non-negative integer.")
        return self

class AllowedValuesPolicy(BaseModel):
    source: Optional[Path] = None
    values: Optional[List[str]] = None
    separator: str = "," 
    violate_action: StringViolateAction

    @model_validator(mode='after')
    def check_source_or_values(self) -> 'AllowedValuesPolicy':
        if self.source is None and self.values is None:
            raise ValueError("Either 'source' or 'values' must be provided for allowed_values.")
        if self.source is not None and self.values is not None:
            raise ValueError("Cannot provide both 'source' and 'values' simultaneously.")
        
        if self.source is not None:
            if self.source.suffix not in ['.txt', '.csv', '.npy']:
                raise ValueError(f"Source must be .txt, .csv, or .npy. Got: {self.source}")
        
        return self

class CustomExprPolicy(BaseModel):
    expr: str
    violate_action: StringViolateAction

class ConstraintsDef(BaseModel):
    unique: Optional[UniqueConstraint] = None
    min_policy: Optional[MinPolicy] = None
    max_policy: Optional[MaxPolicy] = None
    min_length: Optional[StringLengthPolicy] = None
    max_length: Optional[StringLengthPolicy] = None
    allowed_values: Optional[AllowedValuesPolicy] = None
    regex: Optional[RegexPolicy] = None
    custom_expr: Optional[CustomExprPolicy] = None

class ColumnDef(BaseModel):
    rename: Optional[str] = None
    dtype: DType
    date_format: Optional[DateFormatConfig] = Field(None, alias="format")
    on_null: Optional[NullPolicy] = None
    constraints: Optional[ConstraintsDef] = None

    @model_validator(mode='after')
    def check_column_logic(self) -> 'ColumnDef':
        if self.constraints:
            if self.dtype == "string":
                if self.constraints.min_policy or self.constraints.max_policy:
                    raise ValueError("min/max policy cannot be applied to 'string' dtype. Use min_length/max_length.")
            elif self.dtype in ["int", "float"]:
                if self.constraints.regex:
                    raise ValueError("Regex constraints can only be applied to 'string' dtype.")
                if self.constraints.min_length or self.constraints.max_length:
                    raise ValueError("Length constraints can only be applied to 'string' dtype.")
            elif self.dtype in ["date", "datetime"]:
                if self.constraints.regex or self.constraints.min_length or self.constraints.max_length:
                    raise ValueError(f"Regex/length constraints cannot be applied to '{self.dtype}' dtype.")

        return self

class DuplicateRowsConfig(BaseModel):
    tactic: DupTactic = "keep"
    subset: Optional[List[str]] = None

class ConfDef(BaseModel):
    undefined_columns: Literal["drop", "keep"] = "drop"
    on_duplicate_rows: DuplicateRowsConfig = Field(default_factory=DuplicateRowsConfig)

class Manifesto(BaseModel):
    conf: ConfDef = Field(default_factory=ConfDef)
    columns: Dict[str, ColumnDef]
    pipeline: Optional[List[Dict[str, Any]]] = None