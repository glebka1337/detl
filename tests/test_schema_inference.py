import pytest
import polars as pl
from detl.schema import Manifesto
from detl.engine.core import DetlEngine
from pydantic import ValidationError

def test_zero_config_inference():
    # Only supply global defaults, no columns!
    yaml_dict = {
        "conf": {
            "undefined_columns": "keep",
            "defaults": {
                "int": {"on_null": {"tactic": "fill_value", "value": 0}},
                "string": {"on_null": {"tactic": "fill_value", "value": "Unknown"}}
            }
        }
    }
    manifest = Manifesto(**yaml_dict)
    engine = DetlEngine(manifest)
    
    # Create raw dataframe with missing values
    df = pl.LazyFrame({
        "age": [10, None, 30],
        "name": ["Alice", None, "Charlie"]
    })
    
    # Execute the engine (this should inherently build the schema using _infer_schema)
    res = engine.execute(df).collect()
    
    assert res.get_column("age")[1] == 0
    assert res.get_column("name")[1] == "Unknown"

def test_fail_fast_validation():
    yaml_dict = {
        "columns": {
            "missing_col": {"dtype": "int"}
        }
    }
    manifest = Manifesto(**yaml_dict)
    engine = DetlEngine(manifest)
    
    df = pl.LazyFrame({"age": [10]})
    
    with pytest.raises(ValueError, match="Manifest column 'missing_col' does not exist in the dataset."):
        engine.execute(df)
