import yaml
import pytest
from pathlib import Path
from pydantic import ValidationError
from detl.schema import (
    Manifesto,
    ColumnDef,
    NullPolicy,
    MinPolicy,
    AllowedValuesPolicy,
    NumericViolateAction,
    StringViolateAction
)

def test_valid_manifest_example():
    yaml_content = """
    conf:
      undefined_columns: "drop"
      on_duplicate_rows:
        tactic: "drop_extras"
        subset: ["email"]

    columns:
      student_id:
        rename: "stid"
        dtype: int
        on_null:
          tactic: "drop_row"
        constraints:
          unique:
            tactic: "drop_extras"
      
      age:
        dtype: int
        on_null:
          tactic: "fill_median"
        constraints:
          min_policy:
            threshold: 16
            violate_action:
              tactic: "fill_value" 
              value: 16
          max_policy:
            threshold: 100
            violate_action:
              tactic: "drop_row"
      
      group:
        dtype: string
        on_null:
          tactic: "fill_most_frequent" 
        constraints:
          allowed_values: 
            values: ["A", "B", "C"]
            violate_action:
              tactic: "drop_row"
      
      email:
        dtype: string
        on_null:
          tactic: "fill_value"
          value: "no-reply@university.edu"
        constraints:
          regex:
            pattern: '^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$'
            violate_action:
              tactic: "fill_value"
              value: "invalid_format@university.edu"

      date_registered:
        dtype: date
        format:
          input: "%d/%m/%Y"
          output: "%Y-%m-%d"
          on_parse_error:
            tactic: "drop_row"
        on_null:
          tactic: "fill_value" 
          value: "2026-03-27"
        constraints:
          min_policy:
            threshold: "2020-01-01"
            violate_action:
              tactic: "drop_row"

    pipeline:
      - mutate:
          is_adult: "age >= 18"
    """
    data = yaml.safe_load(yaml_content)
    manifest = Manifesto(**data)
    
    # Assertions on parsed contents
    assert manifest.conf.undefined_columns == "drop"
    assert manifest.conf.on_duplicate_rows.tactic == "drop_extras"
    assert manifest.conf.on_duplicate_rows.subset == ["email"]
    assert manifest.columns["age"].dtype == "int"
    assert manifest.columns["group"].constraints.allowed_values.values == ["A", "B", "C"]
    assert manifest.columns["date_registered"].date_format.in_format == "%d/%m/%Y"


def test_invalid_string_constraint():
    with pytest.raises(ValidationError, match="min/max policy cannot be applied to 'string' dtype."):
        ColumnDef(
            dtype="string",
            constraints={
                "min_policy": MinPolicy(
                    threshold=10, 
                    violate_action=NumericViolateAction(tactic="drop_row")
                )
            }
        )

def test_invalid_numeric_constraint():
    with pytest.raises(ValidationError, match="Regex constraints can only be applied to 'string' dtype."):
        ColumnDef(
            dtype="int",
            constraints={
                "regex": {
                    "pattern": ".*",
                    "violate_action": {"tactic": "drop_row"}
                }
            }
        )

def test_invalid_allowed_values():
    with pytest.raises(ValidationError, match="Either 'source' or 'values' must be provided"):
        AllowedValuesPolicy(violate_action=StringViolateAction(tactic="drop_row"))
        
    with pytest.raises(ValidationError, match="Cannot provide both"):
        AllowedValuesPolicy(
            source=Path("dummy.txt"), 
            values=["A"], 
            violate_action=StringViolateAction(tactic="drop_row")
        )

def test_invalid_fill_value_missing():
    with pytest.raises(ValidationError, match="requires a 'value' parameter"):
        NumericViolateAction(tactic="fill_value")

def test_invalid_fill_max_with_value():
    with pytest.raises(ValidationError, match="forbidden for dynamically computed tactic"):
        NumericViolateAction(tactic="fill_max", value=10)

def test_invalid_min_policy_fill_max():
    with pytest.raises(ValidationError, match="'fill_max' makes no sense inside 'min_policy'"):
        MinPolicy(
            threshold=10,
            violate_action=NumericViolateAction(tactic="fill_max")
        )

def test_invalid_date_format_type():
    with pytest.raises(ValidationError):
        ColumnDef(
            dtype="date",
            format="not a dictionary"
        )

def test_invalid_null_tactic():
    with pytest.raises(ValidationError, match="Tactic 'fill_value' requires a 'value'"):
        NullPolicy(tactic="fill_value")

def test_string_length_policy():
    col = ColumnDef(
        dtype="string",
        constraints={
            "max_length": {
                "length": 100,
                "violate_action": {"tactic": "drop_row"}
            }
        }
    )
    assert col.constraints.max_length.length == 100

    with pytest.raises(ValidationError, match="Length must be a non-negative integer"):
        ColumnDef(
            dtype="string",
            constraints={
                "min_length": {
                    "length": -5,
                    "violate_action": {"tactic": "drop_row"}
                }
            }
        )
