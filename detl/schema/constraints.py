from typing import Optional, Union, List
from pydantic import BaseModel, model_validator
from pathlib import Path

from detl.constants import DupTactic
from detl.schema.common import StringViolateAction, NumericViolateAction

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
        
        if not self.separator:
            raise ValueError("Separator cannot be empty.")
            
        if self.source is not None:
            if self.source.suffix not in ['.txt', '.csv', '.npy']:
                raise ValueError(f"Source must be .txt, .csv, or .npy. Got: {self.source}")
            if self.source.suffix == '.npy' and 'separator' in self.model_fields_set:
                raise ValueError("Cannot specify a 'separator' when using a .npy source file.")
        
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
