import polars as pl
from detl.core import Processor
from detl.config import Config

def test_string_trimming():
    yaml_config = {
        "columns": {
            "messy_name": {
                "dtype": "string",
                "trim": True,
                "constraints": {
                    "max_length": {
                        "length": 5,
                        "violate_action": {"tactic": "fail"}
                    }
                }
            },
            "strict_name": {
                "dtype": "string"
            }
        }
    }
    
    config = Config(yaml_config)
    proc = Processor(config)
    
    df = pl.DataFrame({
        "messy_name": ["  Gleb  ", "\tMax\t", "Alex"],
        "strict_name": ["  Gleb  ", "\tMax\t", "Alex"]
    })
    
    proc._apply_global_defaults()
    proc._infer_schema(df)
    proc._validate_schema_vs_data(df)
    
    df = proc._drop_undefined_columns(df)
    df = proc._apply_types_and_date_formats(df)
    
    clean_list = df.select("messy_name").to_series().to_list()
    assert clean_list == ["Gleb", "Max", "Alex"], "Trim failed to strip whitespaces"
    
    strict_list = df.select("strict_name").to_series().to_list()
    assert strict_list == ["  Gleb  ", "\tMax\t", "Alex"], "Trim modified a column without explicit permission"
