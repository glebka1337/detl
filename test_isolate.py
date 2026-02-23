import polars as pl
from detl.config import Config
from detl.core import Processor
import yaml
from detl.engine.constraints import apply_constraints

with open("tests/manual/dirty_conf.yaml") as f:
    config_dict = yaml.safe_load(f)
config = Config(config_dict)
proc = Processor(config)

df = pl.DataFrame({"username": ["gleb_gigachad"]})
proc._apply_global_defaults()
col_def = proc.manifest.columns["username"]

print("Initial:")
print(df)

for field_name in col_def.constraints.model_fields.keys():
    policy = getattr(col_def.constraints, field_name)
    if policy:
        print(f"Applying {field_name}...")
        from detl.engine.constraints import CONSTRAINT_REGISTRY
        handler = CONSTRAINT_REGISTRY.get(field_name)
        df = handler(df, "username", policy)
        print(df)
