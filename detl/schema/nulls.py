from typing import Optional, Union
from pydantic import BaseModel, model_validator

from detl.constants import NullTactic

class NullPolicy(BaseModel):
    tactic: NullTactic
    value: Optional[Union[int, float, str, bool]] = None

    @model_validator(mode='after')
    def check_null_logic(self) -> 'NullPolicy':
        if self.tactic == "fill_value" and self.value is None:
            raise ValueError("Tactic 'fill_value' requires a 'value' parameter.")
        if self.tactic != "fill_value" and self.value is not None:
            raise ValueError("Parameter 'value' allowed only for 'fill_value'.")
        return self
