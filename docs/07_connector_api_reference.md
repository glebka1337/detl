# 7. Connector API & CLI Reference

`detl` uses a fully decoupled architecture routing data through the `detl.connectors` abstraction boundaries. Here is the explicit mapping of every single available argument, their Python programmatic equivalent, and how to invoke them through the CLI.

---

## Command Line Global Arguments

When using the `detl` CLI, mappings conform to this global structure:
*   `--config [FILE]`: **Required**. The path to your YAML declarative contract.
*   `--source-type [TYPE]` / `--source-uri [URI]`: Identifies the `Source` extractor.
*   `--sink-type [TYPE]` / `--sink-uri [URI]`: Identifies the `Sink` loader.

---

## ☁️ Cloud Connectors (S3 / MinIO)

Transfers data in-memory directly utilizing strictly `boto3`. No local disk space is ever required during pipeline routing.

### Python API
```python
from detl.connectors import S3Source, S3Sink

source = S3Source(
    s3_uri="s3://my-bucket/data.parquet",
    format="parquet", # 'csv' or 'parquet'
    aws_access_key_id="admin", # Or relies on ENV vars natively
    aws_secret_access_key="password",
    endpoint_url="http://localhost:9000" # For MinIO
)

sink = S3Sink(
    s3_uri="s3://my-bucket/cleaned.parquet",
    format="parquet"
)
```

### CLI Arguments
*   `--source-type s3` / `--sink-type s3`
*   `--source-uri "s3://..."` / `--sink-uri "s3://..."`
*   `--s3-endpoint-url "http://localhost:9000"` (Optional modifier for custom domains)

---

## ️ Database Connectors (Postgres / MySQL / SQLite)

Native database protocols mapping zero-copy direct queries via `connectorx` reading protocols and `adbc` / `sqlalchemy` optimized writing blocks.

### Python API
```python
from detl.connectors import PostgresSource, MySQLSink

source = PostgresSource(
    connection_uri="postgresql://user:pass@host:5432/db",
    query="SELECT id, name FROM users",
    batch_size=50000 # Optional chunking size constraint
)

sink = MySQLSink(
    connection_uri="mysql://user:pass@host:3306/db",
    table_name="cleaned_users",
    if_table_exists="replace", # 'append' or 'fail'
    batch_size=100000
)
```

**Dependency Requirements:**
*   Postgres Reads: `connectorx`, `adbc-driver-postgresql`
*   MySQL Reads: `connectorx`
*   MySQL Writes: `sqlalchemy`, `pymysql`, `pandas` (Pandas utilized purely as an abstraction fallback)
*   SQLite: `adbc-driver-sqlite`

### CLI Arguments
*   `--source-type postgres` | `mysql` | `sqlite`
*   `--source-uri "protocol://credentials..."`
*   `--source-query "SELECT * FROM..."`
*   `--source-batch-size 1000`
*   `--sink-type postgres` | `mysql` | `sqlite`
*   `--sink-uri "protocol://credentials..."`
*   `--sink-table "target_table_name"`
*   `--sink-batch-size 5000`

---

##  File System Connectors (CSV / Parquet / Excel)

Polars-native highly tuned binary abstractions pointing directly at physical disk targets. Automatically evaluates formats lazily avoiding out-of-memory evaluation errors.

### Python API
```python
from detl.connectors import CsvSource, ParquetSink

source = CsvSource(
    path="./local_data/input.csv",
    separator="," # Auto-defaults to comma
)

sink = ParquetSink(
    path="./local_data/output_cleaned.parquet",
    streaming=True # Enforces streaming API evaluation dynamically against LazyFrames
)
```

### CLI Arguments 
*If avoiding complex declarations, you can still use `-i input.csv -o output.parquet` for implicitly typed file paths!*

**Explicit Types:**
*   `--source-type csv` | `parquet` | `excel`
*   `--source-uri "/path/to/infile.csv"`
*   `--sink-type csv` | `parquet` | `excel`
*   `--sink-uri "/path/to/outfile.parquet"`
