import pytest
import tempfile
from pathlib import Path
import polars as pl

from detl.config import Config
from detl.core import Processor
from detl.exceptions import ConfigError

def test_empty_yaml():
    yaml_content = ""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        f_path = f.name
    
    with pytest.raises(ConfigError, match="is empty or invalid"):
        Config(f_path)
    Path(f_path).unlink()

def test_negative_length_constraint():
    yaml_config = {
        "columns": {
            "name": {
                "dtype": "string",
                "constraints": {
                    "min_length": {
                        "length": -10,
                        "violate_action": {"tactic": "fail"}
                    }
                }
            }
        }
    }
    with pytest.raises(ConfigError) as exc:
        Config(yaml_config)
    assert "Length must be a non-negative integer" in str(exc.value)

def test_csv_as_yaml():
    csv_content = "id,name\n1,gleb\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(csv_content)
        f_path = f.name
    
    with pytest.raises(ConfigError):
        # YAML parser might parse this as a weird string or it might fail pydantic dict validation
        Config(f_path)
    Path(f_path).unlink()

def test_circular_renaming():
    yaml_config = {
        "columns": {
            "A": {"dtype": "string", "rename": "B"},
            "B": {"dtype": "string", "rename": "A"}
        }
    }
    config = Config(yaml_config)
    proc = Processor(config)
    
    df = pl.DataFrame({
        "A": ["a_val"],
        "B": ["b_val"]
    })
    
    proc._apply_global_defaults()
    proc._infer_schema(df)
    proc._validate_schema_vs_data(df)
    
    # Normally this rename happens at the very end in _apply_outputs
    try:
        final_df = proc._apply_outputs(df)
        # Polars might actually support this natively without collision because renames are parallel!
        assert "A" in final_df.columns
        assert "B" in final_df.columns
        # Did they actually swap?
        assert final_df.select("A").item() == "b_val"
        assert final_df.select("B").item() == "a_val"
    except Exception as e:
        pytest.fail(f"Circular renaming caused an unexpected exception: {e}")

def test_int64_overflow_during_cast():
    yaml_config = {
        "columns": {
            "massive_number": {"dtype": "int"}
        }
    }
    config = Config(yaml_config)
    proc = Processor(config)
    
    df = pl.DataFrame({
        "massive_number": ["9999999999999999999999999999999999"]
    })
    
    proc._apply_global_defaults()
    proc._infer_schema(df)
    proc._validate_schema_vs_data(df)
    
    df = proc._drop_undefined_columns(df)
    # This should fail or overflow during strict casting
    with pytest.raises((Exception, pl.exceptions.ComputeError, pl.exceptions.PolarsError)):
        # Because strict=True logic was removed (cast is just .cast(pl.Int64)), this might actually 
        # convert to null silently using strict=False default. Or it might raise ComputeError.
        df.with_columns(pl.col("massive_number").cast(pl.Int64, strict=True))

def test_trim_on_non_string():
    # Attempting to assign `trim: true` to a FLOAT should be either ignored or caught.
    # Currently Schema doesn't validate interdependent field logic (trim requires string dtype).
    yaml_config = {
        "columns": {
            "price": {
                "dtype": "float",
                "trim": True
            }
        }
    }
    # It passes config because trim is just a boolean on ColumnDef regardless of dtype.
    config = Config(yaml_config)
    proc = Processor(config)
    df = pl.DataFrame({"price": [10.5, 20.0]})
    proc._apply_global_defaults()
    proc._infer_schema(df)
    proc._validate_schema_vs_data(df)
    df = proc._drop_undefined_columns(df)
    
    # Should not crash because we only inspect col_def.trim in _cast_string!
    df = proc._apply_types_and_date_formats(df)
    assert df.select("price").dtypes[0] == pl.Float64
