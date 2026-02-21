# detl (Declarative ETL)

`detl` is a rigorous, declarative CLI tool designed to perform rapid data validation, transformation, and cleansing using Data Contracts written in YAML.

Powered by **Polars** (for blazing-fast, C++ backed LazyFrame evaluations) and **Pydantic** (for strict, type-safe API schema definitions), `detl` is highly modular, extensible, and professional.

## Features

- **Data Contracts as Code**: Guarantee data quality using human-readable YAML configurations without writing complex Python scripts.
- **Lazy Evaluation**: Utilizes `pl.LazyFrame` beneath the hood. The engine constructs a highly optimized query execution graph and only pulls data into memory strictly when necessary, allowing you to seamlessly process gigabytes of data.
- **Strict Operations**: Clean and professional error handling without Python tracebacks. Aborts elegantly and tells exactly where your data violates the contract.
- **Modular Decorators**: The engine architecture leverages strict dispatch registries (e.g. `@register_pipeline_stage`, `@register_constraint`). Extending capabilities is natively simple as writing a `Callable` payload.
- **Transformative Pipeline Ops**: Capable of dynamically filtering, mutating via SQL, renaming, and sorting.

## Installation

`detl` uses `uv` for modern, blazing-fast package management. Ensure Python 3.10+ is installed.

```bash
# Install detl globally using uv
uv pip install -e .
```

You will then have access to the global CLI entrypoint alias: `detl`.

## Quick Start

Create a Data Contract (Manifesto), like `contract.yml`:

```yaml
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

pipeline:
  - filter: "age >= 18"
  - mutate:
      is_adult: "True"
  - sort:
      by: "age"
      order: "desc"
```

Then run the pipeline passing your raw input and output targets:

```bash
detl --config contract.yml --input raw.csv --output clean_data.parquet
```

The CLI handles smart I/O streaming automatically, figuring out `.csv` vs `.parquet` via file extensions and evaluating the pipeline gracefully.

## Architecture Guidelines for Developers

The inner engine relies on strictly coupled Registry routines isolating data transformations by context. Emojis and unmaintainable `if-else` dictionaries are entirely stripped from the source codebase. 

- `detl/engine/types.py` - Casts schema dtypes (string, booleans, dates, dates matching custom parsing strategies)
- `detl/engine/nulls.py` - Manages `NullTactic` executions (fill median, value, bfill, fail, etc.)
- `detl/engine/constraints.py` - Translates custom logic (Regex, subsets, Unique evaluations) into boolean threshold checks. 
- `detl/engine/actions.py` - Dictates strict behaviors of violation policies (drop rows, rewrite masks).
- `detl/engine/pipeline.py` - Exposes SQL mutating handlers spanning dataset operations (mutate, filter, sort, rename). 

Everything aggregates flawlessly into the `DetlEngine.execute()` which natively intercepts and forwards Polars evaluations safely.
