# Launching `detl` 🚀: The Death of Pandas ETL Boilerplate

If you are a Data Engineer, you have written the exact same Python script 50 times in your career.

You connect to a Postgres database. You pull 2 million rows. You drop nulls. You validate string formats. You cast the dates. You write it to a Parquet file in AWS S3 or shuffle it into a MySQL data warehouse.

And you probably wrote it using **Pandas**. 

Here is the problem: Pandas is greedy. If your table has 10 million rows, Pandas attempts to load all 10 million into RAM simultaneously. Your Docker container OOM crashes. So you rewrite your script to use manual `chunking=10000` via SQLAlchemy. Now your script takes 45 minutes to run and your code is 300 lines of unreadable boilerplate.

Enter **`detl`** (Declarative ETL). 

`detl` is an open-source, strictly typed, zero-copy CLI engine powered by **Polars** and **Pydantic**. It was built to solve one specific problem: moving data from Point A to Point B while guaranteeing contract-driven integrity, without writing a single line of Python.

## Matrix Routing: Zero-Copy Transfers 🗄️ ☁️

The fundamental superpower of `detl` is its decoupled architecture. It treats sources (CSV, S3, Postgres, MySQL) and sinks exactly the same. 

Because `detl` maps memory lazily via **Polars LazyFrames**, and handles DB transports natively via **ConnectorX** and **ADBC C++ bindings**, you can extract data from a SQL database and push it directly into another SQL database *without ever touching your local hard drive*.

Execute a cross-engine transfer from your terminal:

```bash
uv run detl -f conf.yaml \
  --source-type postgres \
  --source-uri "postgresql://user:pass@localhost:5432/db1" \
  --source-query "SELECT * FROM public.orders" \
  --sink-type mysql \
  --sink-uri "mysql+pymysql://user:pass@localhost:3306/db2" \
  --sink-table "clean_orders"
```

## Data Contracts as YAML 📜

`detl` enforces absolute data integrity via Pydantic validators. Before data ever reaches your target Sink, it must satisfy your YAML contract.

```yaml
columns:
  email:
    dtype: string
    rename: user_email
    trim: true
    constraints:
      min_length: { length: 5, violate_action: { tactic: "drop_row" } }
      custom_expr: { expr: "pl.col('email').str.contains('^[\w\.-]+@[\w\.-]+\.\w+$')", violate_action: { tactic: "fail" } }
  
  birthday:
    dtype: date
    format: { in_format: "%Y-%m-%d", out_format: "%Y" }
    on_null: { tactic: "fill_value", value: "1990-01-01" }
```

In 10 lines of YAML, we just instructed the Engine to natively drop records with emails shorter than 5 chars, validate a regex pattern (crashing the pipeline if broken), strip trailing spaces, format parsed ISO dates down to pure Years, and impute nulls.

The Polars compiler absorbs this YAML and pushes it down into a highly optimized, multi-threaded Execution Plan.

## Built for Chaos 🛡️

We built `detl` defending against malicious environments. 
- **OOM Shielding:** Sinks and Sources map directly via `--source-batch-size 50000` constraints mapping memory exactly.
- **Idempotency Control:** Supplying `--sink-if-exists replace` (or `append/fail`) guarantees the exact same pipeline yields the exact same state locally or remotely.
- **SQLi Protection:** Native cursor bindings intercept and drop Bobby Tables payloads `SELECT * FROM users; DROP TABLE users;--`.

### The Bottom Line
Stop rewriting boilerplate validation loops. Start defining contracts. Let Rust and C++ handle the physical transfer.

Check out the repo, grab the `pip` install, and route your first matrix pipeline today!

🔗 **[GitHub Repository: glebka1337/detl](https://github.com/glebka1337/detl)**
