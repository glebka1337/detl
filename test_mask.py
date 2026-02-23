import polars as pl
from detl.config import Config
from detl.core import Processor
from detl.connectors import PostgresSource, CsvSink

config = Config("tests/manual/dirty_conf.yaml")
proc = Processor(config)
df = PostgresSource("postgresql://user:password@localhost:5432/detldb", "SELECT * FROM users").read()

proc._apply_global_defaults()
proc._infer_schema(df)
proc._validate_schema_vs_data(df)

df = proc._drop_undefined_columns(df)
df = proc._apply_types_and_date_formats(df)

print("AFTER TYPES:")
print(df)

df = proc._handle_nulls(df)
df = proc._apply_constraints(df)

print("AFTER CONSTRAINTS:")
print(df)

