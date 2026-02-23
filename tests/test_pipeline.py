import polars as pl
import pytest
import yaml
from detl.config import Config
from detl.core import Processor
from detl.connectors.memory import MemorySource

def test_detl_pipeline_advanced():
    yaml_content = """
    conf:
      undefined_columns: "keep"

    columns:
      name:
        dtype: string
      age:
        dtype: int
      pull_ups:
        dtype: int
      push_ups:
        dtype: int

    pipeline:
      - filter: "age >= 18"
      - mutate:
          total_reps: "pull_ups + push_ups"
          is_adult: "age >= 18"
      - rename:
          total_reps: "volume"
      - sort:
          by: "volume"
          order: "desc"
    """

    # 1. Load schema
    config = Config(yaml.safe_load(yaml_content))
    engine = Processor(config)

    # 2. Raw Data
    df = pl.LazyFrame({
        "name": ["Alice", "Bob", "Charlie", "David"],
        "age": [25, 17, 30, 22],
        "pull_ups": [10, 15, 5, 20],
        "push_ups": [20, 30, 20, 40]
    })
    source = MemorySource(df)

    # 3. Execute!
    processed_df = engine.execute(source)
    result = processed_df.collect()

    # 4. Assertions
    # Bob (17) should be filtered out
    assert len(result) == 3
    assert "Bob" not in result.get_column("name").to_list()

    # is_adult should be True for all remaining
    assert result.get_column("is_adult").all()

    # Rename & Mutate check (Charlie volume = 25, Alice = 30, David = 60)
    assert "volume" in result.columns
    assert "total_reps" not in result.columns

    # Sort check (David=60 > Alice=30 > Charlie=25)
    names = result.get_column("name").to_list()
    assert names == ["David", "Alice", "Charlie"]
    volumes = result.get_column("volume").to_list()
    assert volumes == [60, 30, 25]
