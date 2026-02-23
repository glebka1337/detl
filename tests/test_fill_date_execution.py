import polars as pl
from detl.config import Config
from detl.core import Processor
from detl.connectors.base import Source

class MockSource(Source):
    def __init__(self, data):
        self._data = data
    def read(self):
        return pl.DataFrame(self._data)

def test_robust_date_imputation():
    raw_data = {
        "id": [1, 2, 3],
        "created_at": ["2023-01-01", None, "invalid_date"]
    }
    
    yaml_config = {
        "conf": {
            "undefined_columns": "keep"
        },
        "columns": {
            "created_at": {
                "dtype": "date",
                "format": {
                    "input": "%Y-%m-%d",
                    "output": "%Y-%m-%d",
                    "on_parse_error": { "tactic": "drop_row" } # invalid date will become null
                },
                "on_null": {
                    "tactic": "fill_value",
                    "value": "2024-12-31" # The fallback!
                }
            }
        }
    }
    
    config = Config(yaml_config)
    proc = Processor(config)
    source = MockSource(raw_data)
    
    df = proc.execute(source)
    
    # 2nd row was None originally
    assert str(df["created_at"][1]) == "2024-12-31"
    
    # 3rd row was "invalid_date", parser drops it to Null, then on_null catches it
    assert str(df["created_at"][2]) == "2024-12-31"

def test_robust_datetime_imputation():
    raw_data = {
        "id": [1, 2],
        "created_at": [None, None]
    }
    
    yaml_config = {
        "conf": {
            "undefined_columns": "keep"
        },
        "columns": {
            "created_at": {
                "dtype": "datetime",
                "format": {
                    "input": "%Y-%m-%d %H:%M:%S",
                    "output": "%Y-%m-%d %H:%M:%S"
                },
                "on_null": {
                    "tactic": "fill_value",
                    "value": "2030-01-01 12:00:00"
                }
            }
        }
    }
    
    config = Config(yaml_config)
    proc = Processor(config)
    source = MockSource(raw_data)
    
    df = proc.execute(source)
    
    assert str(df["created_at"][0]) in ["2030-01-01 12:00:00", "2030-01-01 12:00:00.000000"]
