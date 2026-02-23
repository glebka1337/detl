# Connectors & Extract, Transform, Load (ETL) Routing

The beauty of `detl` lies in its **completely decoupled Extract and Load components**. The core processing logic operates on an abstract Polars LazyFrame, meaning you can plug practically ANY combination of sources and sinks to hydrate and export your data safely.

No monstrous if-else branching or tightly-coupled framework code. Everything implements a simple `Source` or `Sink` interface.

## Natively Supported Connectors

### File Systems
- **CSV:** `CsvSource`, `CsvSink`
- **Parquet:** `ParquetSource`, `ParquetSink` 
- **Excel:** `ExcelSource`, `ExcelSink` 

### Databases (Powered by ADBC & ConnectorX)
- **Postgres:** `PostgresSource`, `PostgresSink`
- **MySQL:** `MySQLSource`, `MySQLSink`
- **SQLite:** `SQLiteSource`, `SQLiteSink`

*(All database interactions are fully zero-copy, streaming data directly into Apache Arrow memory formats without Python iteration overheads).*

### 3. S3 / MinIO API
Powered natively by `boto3` delegating raw bytes directly into Polars via `io.BytesIO`. Requires `boto3`.

```bash
detl --config contract.yml \
     --source-type s3 --source-uri "s3://my-bucket/data.parquet" \
     --sink-type s3 --sink-uri "s3://processed-bucket/output.csv" --s3-endpoint-url "http://localhost:9000"
```

## Performance & Limitations

### 1. Batch Control
For large DB loads, you can implement chunking:
```python
sink = PostgresSink(..., batch_size=50000)
```
Or via CLI: `--source-batch-size 100000 --sink-batch-size 50000`

### 2. MySQL `sqlalchemy` constraints
While PostgeSQL and SQLite can natively stream writes through bleeding-edge `adbc` bindings natively linked by Polars, MySQL fallback write operations (`MySQLSink`) rely on `sqlalchemy`. Therefore, writing to a MySQL sink natively currently demands the installation of `pandas`.

## Python API Usage

When integrating `detl` natively into Airflow or Prefect flows, you can utilize the `Processor` and `Config` classes directly:

```python
from detl import Processor, Config
from detl.connectors import PostgresSource, ParquetSink

# 1. Load your Declarative Specification
config = Config("contract.yml")

# 2. Define Extractor & Loader bounds
source = PostgresSource(
    connection_uri="postgresql://user:pass@localhost:5432/db", 
    query="SELECT * FROM raw_events"
)
sink = ParquetSink("s3://data-lake/clean_events.parquet")

# 3. Fire the pipeline!
proc = Processor(config)
proc.execute(source, sink)
```

## CLI Usage

The `detl` CLI makes it trivial to route inputs and outputs directly from the shell without deploying Airflow configurations.

### Simple File to File (Legacy)
```bash
detl -f auth_manifest.yml -i ./raw_users.csv -o ./clean_users.parquet
```

### Advanced Matrix Deployments 
Extract from SQLite -> Transform via Configuration Contract -> Load to Postgres.
```bash
detl --config contract.yml \
     --source-type sqlite --source-uri "sqlite:///events.db" --source-query "SELECT * FROM web_events" \
     --sink-type postgres --sink-uri "postgresql://admin:password@prod-db.internal/analytics" --sink-table "web_events_clean"
```

## Creating Custom Connectors
If you have an aggressive internal data-lake or proprietary API (such as HubSpot or Salesforce), you can build a native connector rapidly by inheriting from the `detl.connectors.Source` and `detl.connectors.Sink` Abstract Base Classes:

```python
import polars as pl
from detl.connectors import Source

class SalesforceSource(Source):
    ...
```
