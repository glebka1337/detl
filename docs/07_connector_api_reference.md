# 7. Connector API & CLI Reference

`detl` uses a fully decoupled architecture routing data through the `detl.connectors` abstraction boundaries. What makes `detl` unique is **Matrix Routing**: you can pipe data from *any* Source to *any* Sink directly without ever saving intermediate files locally!

---

## CLI Crash Course (Matrix Routing)

The true power of `detl` is executed via the CLI. All operations require a path to the Contract (`-f config.yaml`).

### 1. Local Files (The Defaults)
Use `-i` and `-o` as quick shortcuts for File System connectors (CSV, Parquet, Excel).

```bash
# Convert a CSV to Parquet while applying validation schema
uv run detl -f conf.yaml -i raw_data.csv -o clean_data.parquet
```

### 2. Database Extraction
Use `--source-type` and `--source-uri` to execute SQL queries natively directly into the pipeline!

```bash
# Extract messy records from Postgres, clean them, and save to CSV
uv run detl -f conf.yaml \
  --source-type postgres \
  --source-uri "postgresql://user:password@localhost:5432/detldb" \
  --source-query "SELECT * FROM users" \
  -o clean_db.csv
```

### 3. Database to Database (Or DB to S3)
You don't need `-o`! Just specify `--sink-type` to push data across domains.

```bash
# Read from Postgres -> Validate -> Write cleanly into MySQL
uv run detl -f conf.yaml \
  --source-type postgres \
  --source-uri "postgresql://user:pass@localhost:5432/db1" \
  --source-query "SELECT * FROM public.orders" \
  --sink-type mysql \
  --sink-uri "mysql+pymysql://user:pass@localhost:3306/db2" \
  --sink-table "clean_orders"
```

---

## Cloud Connectors (S3 / MinIO)

Transfers data in-memory directly utilizing strictly `boto3`. No local disk space is ever required during pipeline routing.

### CLI Arguments
*   `--source-type s3` / `--sink-type s3`
*   `--source-uri "s3://..."` / `--sink-uri "s3://..."`
*   `--s3-endpoint-url "http://localhost:9000"` (Optional modifier for custom domains like MinIO)

```bash
uv run detl -f conf.yaml -i local.csv --sink-type s3 --sink-uri "s3://my-bucket/datalake.parquet" --s3-endpoint-url "http://localhost:9000"
```

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

---

## Database Connectors (Postgres / MySQL / SQLite)

Native database protocols mapping zero-copy direct queries via `connectorx` reading protocols and `adbc` / `sqlalchemy` optimized writing blocks.

### CLI Arguments
*   `--source-type postgres` | `mysql` | `sqlite`
*   `--source-uri "protocol://credentials..."`
*   `--source-query "SELECT * FROM..."`
*   `--source-batch-size 1000` (Optional)
*   `--sink-type postgres` | `mysql` | `sqlite`
*   `--sink-uri "protocol://credentials..."`
*   `--sink-table "target_table_name"`
*   `--sink-if-exists replace` (Choices: `replace` (default), `append`, `fail`. Dictates idempotency)
*   `--sink-batch-size 5000` (Optional. Caps streaming memory consumption)

### Fault Tolerance, Memory & Idempotency

Database extractors are strictly typed to act predictably when pipelining large data volumes:
1. **Idempotency Defaults** (`--sink-if-exists replace`): Running `detl` twice against the same data pipeline guarantees identical outcomes natively by safely tearing down the target `sink-table` and rewriting schemas to match the manifest. Override passing `append` to map new valid entries sequentially.
2. **Memory Overflows** (`--...-batch-size 50000`): Avoid OOM (Out Of Memory) node crashes when extracting millions of rows via `PostgresSource` by strictly declaring the maximum chunk limits. `detl` streams data in constrained iterations.
3. **Hard Failures**: If the `source` table is missing, the API crashes immediately with `[error]Source/Sink Connection Error:[/error]`, preventing "phantom runs". Null tables return error outputs matching strict DB paradigms.

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

---

## File System Connectors (CSV / Parquet / Excel)

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
