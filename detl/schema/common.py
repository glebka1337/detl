from typing import Optional, Union, Dict, Literal, Any
from pydantic import BaseModel, Field, model_validator

from detl.constants import (
    DType,
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

class DateFormatConfig(BaseModel):
    in_format: str = Field(alias="input")
    out_format: str = Field(alias="output")
    on_parse_error: Dict[str, Literal["drop_row", "fail"]] = Field(default_factory=lambda: {"tactic": "drop_row"})

def validate_type_logic(dtype: DType, constraints: Optional[Any], on_null: Optional[Any], context: str) -> None:
    if constraints:
        if dtype in ["string", "boolean"]:
            if constraints.min_policy or constraints.max_policy:
                raise ValueError(f"min/max policy {context}cannot be applied to '{dtype}' dtype. Use min_length/max_length for strings.")
        if dtype in ["int", "float", "boolean", "date", "datetime"]:
            if getattr(constraints, 'regex', None):
                raise ValueError(f"Regex constraints {context}can only be applied to 'string' dtype.")
            if getattr(constraints, 'min_length', None) or getattr(constraints, 'max_length', None):
                raise ValueError(f"Length constraints {context}can only be applied to 'string' dtype.")

    if on_null:
        compute_tactics = ["fill_mean", "fill_median", "fill_min", "fill_max"]
        tactic = getattr(on_null, 'tactic', None)
        if tactic in compute_tactics:
            if dtype not in ["int", "float"]:
                if dtype in ["date", "datetime"] and tactic in ["fill_max", "fill_min"]:
                    pass
                else:
                    raise ValueError(f"Tactic '{tactic}' {context}cannot be used on dtype '{dtype}'. Must be numeric.")
        elif tactic == "fill_value":
            val = getattr(on_null, 'value', None)
            if val is not None:
                if dtype in ["int", "float"] and not isinstance(val, (int, float)):
                    raise ValueError(f"fill_value for numeric dtype '{dtype}' {context}must be a number. Got: {type(val).__name__}")
                if dtype == "string" and not isinstance(val, str):
                    raise ValueError(f"fill_value for dtype 'string' {context}must be a string. Got: {type(val).__name__}")
                if dtype == "boolean" and not isinstance(val, bool):
                    raise ValueError(f"fill_value for dtype 'boolean' {context}must be a boolean. Got: {type(val).__name__}")
                if dtype in ["date", "datetime"] and not isinstance(val, str):
                    raise ValueError(f"fill_value for temporal dtype '{dtype}' {context}must be a string matching the defined input or output format. Got: {type(val).__name__}")
