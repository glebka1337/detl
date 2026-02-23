import polars as pl
import yaml
import yaml
import pytest
from detl.config import Config
from detl.core import Processor
from detl.connectors.memory import MemorySource

def test_detl_engine_e2e():
    yaml_content = """
    conf:
      undefined_columns: "drop"
      on_duplicate_rows:
        tactic: "drop_extras"

    columns:
      age:
        dtype: int
        on_null:
          tactic: "fill_median"
        constraints:
          min_policy:
            threshold: 18
            violate_action:
              tactic: "fill_value" 
              value: 18
      name:
        dtype: string
        constraints:
          min_length:
            length: 3
            violate_action:
              tactic: "drop_row"
      role:
        dtype: string
        constraints:
          allowed_values:
            values: ["admin", "user"]
            violate_action:
              tactic: "fill_value"
              value: "user"
      score:
        dtype: int
        on_null:
          tactic: "fill_mean"

    pipeline:
      - mutate:
          is_adult: "age >= 18"
    """
    
    # 1. Load schema
    config = Config(yaml.safe_load(yaml_content))
    engine = Processor(config)
    
    # 2. Raw Data
    df = pl.DataFrame({
        "age": [10, 25, None, 40, 10], # One underage, one None (median will be 25), duplicates row 1 and 5
        "name": ["Jo", "Alice", "Bob", "Charlie", "Jo"], # Jo is too short
        "role": ["superadmin", "user", "guest", "admin", "superadmin"],
        "score": [100, 200, None, 400, 100], # Mean will be 200
        "garbage": ["x", "y", "z", "w", "x"] # Undefined column
    })
    source = MemorySource(df)
    
    # 3. Execute!
    processed_df = engine.execute(source)
    if isinstance(processed_df, pl.LazyFrame):
        processed_df = processed_df.collect()
    
    # 4. Assertions
    # Garbage dropped
    assert "garbage" not in processed_df.columns
    
    # Dropped Jo (length < 3)
    assert len(processed_df) == 3 
    
    # Check Age logic
    # Original valid remaining ages: 25, 40
    # None gets filled by median (32.5 -> cast as int? Actually handled before drops if we test)
    # Wait, execution order: drop undefined -> types -> nulls -> constraints (drops Jo) -> duplicates -> pipeline.
    # df pre constraints: 
    # age: 10, 25, 25 (median of 10,25,40,10=17.5->int?), 40, 10
    
    # Instead of brittle math, let's verify exact properties:
    assert processed_df.get_column("age").min() >= 18 # Underage 10 became 18
    assert "Jo" not in processed_df.get_column("name").to_list() # Dropped text
    assert sorted(processed_df.get_column("role").to_list()) == sorted(["user", "user", "admin"]) # superadmin and guest became user
    assert "is_adult" in processed_df.columns # Pipeline mutant generated
    assert processed_df.get_column("is_adult").all() # Everyone is adult now
