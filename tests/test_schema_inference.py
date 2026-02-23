import pytest
import polars as pl
from detl.config import Config
from detl.core import Processor
from detl.exceptions import ConfigError
from detl.connectors.memory import MemorySource
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
    config = Config(yaml_dict)
    engine = Processor(config)
    
    # Create raw dataframe with missing values
    df = pl.LazyFrame({
        "age": [10, None, 30],
        "name": ["Alice", None, "Charlie"]
    })
    source = MemorySource(df)
    
    # Execute the engine (this should inherently build the schema using _infer_schema)
    res = engine.execute(source).collect()
    
    assert res.get_column("age")[1] == 0
    assert res.get_column("name")[1] == "Unknown"

def test_fail_fast_validation():
    yaml_dict = {
        "columns": {
            "missing_col": {"dtype": "int"}
        }
    }
    config = Config(yaml_dict)
    engine = Processor(config)
    
    df = pl.LazyFrame({"age": [10]})
    source = MemorySource(df)
    
    with pytest.raises(ConfigError, match="Manifest column 'missing_col' does not exist in the dataset."):
        engine.execute(source)
