<div align="center">
  <img src="https://raw.githubusercontent.com/pola-rs/polars-static/master/logos/polars-logo-dark.svg" height="80" alt="Powered By Polars" />
  <h1>detl (Declarative ETL)</h1>
  <p><strong>A rigorous, declarative CLI tool and Python API explicitly designed to perform rapid data validation, transformation, and cleansing using strictly typed YAML Data Contracts.</strong></p>
  
  [![Python](https://img.shields.io/badge/Python-3.10+-blue?style=for-the-badge&logo=python)](https://python.org)
  [![Polars](https://img.shields.io/badge/Powered_By-Polars-orange?style=for-the-badge)](https://pola.rs)
  [![Pydantic](https://img.shields.io/badge/Validation-Pydantic-e92063?style=for-the-badge)](https://docs.pydantic.dev)
  [![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](LICENSE)
</div>

---

Powered natively by **Polars** (for blazing-fast, C++ backed `LazyFrame` memory mapping) and **Pydantic** (for strict, Pythonic data validations), `detl` allows Data Engineers to stop writing 1000-line monolithic scripts that ingest data and instead rely on decoupled configuration streams and `Source`/`Sink` protocols.

##  Key Features

*    **Data Contracts as Code**: Configure pipelines gracefully over YAML. Validate nulls, schemas, strings, regex, ranges, formats, and duplicates safely.
*    **Pluggable Connector Ecosystem**: Read and write straight from `S3 (MinIO/AWS)`, `Postgres`, `MySQL`, `SQLite`, `Parquet`, `Excel`, and `CSV` files natively.
*    **Zero-Copy Memory**: Direct SQL mappings into Polars engines utilizing state-of-the-art native `adbc` and `connectorx` rust accelerators.
*    **Modular Python API**: Fully accessible in Airflow, Prefect, or any standard application logic.
*    **Strict Domain Exceptions**: Predictable exception tracebacks mapping to exactly what constraint violated the dataset (`NullViolationError`, `DuplicateRowError`).

##  Installation

Ensure your environment complies with Python 3.10+ and utilizes modern packaging:

```bash
# Core installation
uv pip install -e .

# Extend with zero-copy database & cloud adapters (Recommended)
uv pip install connectorx adbc-driver-postgresql adbc-driver-sqlite pymysql sqlalchemy pandas boto3
```

## ️ Global CLI Usage

You don't even need to write Python files to process Gigabytes of Data securely. Just deploy a YAML contract and pipe your DB bounds:

### Database Ingestion to Parquet Cloud Archiving
```bash
detl --config contract.yml \
     --source-type postgres \
     --source-uri "postgresql://admin:password@localhost:5432/analytics" \
     --source-query "SELECT * FROM raw_telemetry WHERE active = true" \
     --source-batch-size 100000 \
     --sink-type s3 \
     --sink-uri "s3://telemetry-lake/clean_output.parquet"
```

##  Python Library Orchestration

Integrating `detl` directly into your existing backends is remarkably easy using the decoupled API mappings.

```python
from detl import Processor, Config
from detl.connectors import PostgresSource, S3Sink

# 1. Digest the Pipeline Contract
config = Config("contract.yml")

# 2. Wire the Input and Output boundaries (Files, DBs, and S3 are all supported universally)
source = PostgresSource(
    connection_uri="postgresql://user:pass@localhost:5432/db", 
    query="SELECT * FROM raw_events"
)
sink = S3Sink(
    s3_uri="s3://data-lake/clean_events.parquet",
    format="parquet"
)

# 3. Spin up the Processor and execute the mapping!
proc = Processor(config)
proc.execute(source, sink)
```

##  Comprehensive Documentation

To master Data Contract structures and the Connector API, inspect our documentation ecosystem:

1. [01_configuration.md](docs/01_configuration.md) - Global parsing & deduplication options.
2. [02_columns_and_types.md](docs/02_columns_and_types.md) - Casting logic and date formatting.
3. [03_null_tactics.md](docs/03_null_tactics.md) - Handling anomalies (Drop, Fill Median, Fill Means, Static Value).
4. [04_constraints.md](docs/04_constraints.md) - Validation bounds, regex matches, and mathematical assertions.
5. [05_pipeline.md](docs/05_pipeline.md) - Native SQL Filter / Mutating executions.
6. [06_connectors.md](docs/06_connectors.md) - High-level architectural reasoning around Sources and Sinks.
7. [07_connector_api_reference.md](docs/07_connector_api_reference.md) - **Complete CLI mapping and Python Class parameters for every Connector type.**

Check `examples/03_kitchen_sink.yml` for a highly documented example of everything all at once.
