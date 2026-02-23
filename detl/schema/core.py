from typing import Optional, Dict, List, Any, Literal
from pydantic import BaseModel, Field, model_validator, BeforeValidator

from detl.constants import DType, DupTactic
from detl.schema.common import DateFormatConfig, validate_type_logic
from detl.schema.nulls import NullPolicy
from detl.schema.constraints import ConstraintsDef

class ColumnDef(BaseModel):
    rename: Optional[str] = None
    dtype: DType
    date_format: Optional[DateFormatConfig] = Field(None, alias="format")
    on_null: Optional[NullPolicy] = None
    constraints: Optional[ConstraintsDef] = None

    @model_validator(mode='after')
    def check_column_logic(self) -> 'ColumnDef':
        validate_type_logic(self.dtype, self.constraints, self.on_null, context="")
        return self

class DefaultPolicies(BaseModel):
    on_null: Optional[NullPolicy] = None
    constraints: Optional[ConstraintsDef] = None

class DuplicateRowsConfig(BaseModel):
    tactic: DupTactic = "keep"
    subset: Optional[List[str]] = None

def coerce_dup_config(v: Any) -> Any:
    if isinstance(v, str):
        return {"tactic": v}
    return v

from typing import Annotated

class ConfDef(BaseModel):
    undefined_columns: Literal["drop", "keep"] = "drop"
    on_duplicate_rows: Annotated[DuplicateRowsConfig, BeforeValidator(coerce_dup_config)] = Field(default_factory=DuplicateRowsConfig)
    defaults: Optional[Dict[DType, DefaultPolicies]] = None

    @model_validator(mode='after')
    def check_defaults_logic(self) -> 'ConfDef':
        if not self.defaults:
            return self
        
        for dtype, policy in self.defaults.items():
            validate_type_logic(dtype, policy.constraints, policy.on_null, context="in defaults ")
            
        return self

class Manifesto(BaseModel):
    conf: ConfDef = Field(default_factory=ConfDef)
    columns: Dict[str, ColumnDef] = Field(default_factory=dict)
    pipeline: Optional[List[Dict[str, Any]]] = None

    @model_validator(mode='after')
    def apply_defaults(self) -> 'Manifesto':
        if not self.conf.defaults:
            return self

        for col_name, col_def in self.columns.items():
            default_policy = self.conf.defaults.get(col_def.dtype)
            if default_policy:
                if default_policy.on_null and not col_def.on_null:
                    col_def.on_null = default_policy.on_null
                if default_policy.constraints and not col_def.constraints:
                    col_def.constraints = default_policy.constraints
        return self
