# Launching `detl` 🚀: The Death of Pandas ETL Boilerplate

If you are a Data Engineer, you have written the exact same Python script 50 times in your career.

You connect to a Postgres database. You pull 2 million rows. You drop nulls. You validate string formats. You cast the dates. You write it to a Parquet file in AWS S3 or shuffle it into a MySQL data warehouse.

And you probably wrote it using **Pandas**. 

Here is the problem: Pandas is greedy. If your table has 10 million rows, Pandas attempts to load all 10 million into RAM simultaneously. Your Docker container OOM crashes. So you rewrite your script to use manual `chunking=10000` via SQLAlchemy. Now your script takes 45 minutes to run and your code is 300 lines of unreadable boilerplate. What happens when a string length violates your schema? It fails silently, or worse, crashes the pipeline on row 9,999,999 leaving your Data Warehouse in a corrupted, half-written state.

Enter **`detl`** (Declarative ETL). 

`detl` is an open-source, strictly typed, zero-copy CLI engine powered by the blazing-fast C++ memory engine of **Polars** and the rigorous Pythonic validation of **Pydantic**. It was built to solve one specific problem: moving data from Point A to Point B while guaranteeing absolute contract-driven integrity, without writing a single line of Python.

---

## Matrix Routing: Zero-Copy Transfers 🗄️ ☁️

The fundamental superpower of `detl` is its decoupled architecture. It treats sources (CSV, S3, Postgres, MySQL) and sinks exactly the same. 

Because `detl` maps memory lazily via **Polars LazyFrames**, and handles Database transports natively via **ConnectorX** (Rust) and **ADBC C++ bindings**, you can extract data from a SQL database and push it directly into another SQL database *without ever touching your local hard drive*. We call this **Matrix Routing**.

Execute a cross-engine transfer directly from your terminal:

```bash
uv run detl -f conf.yaml \
  --source-type postgres \
  --source-uri "postgresql://user:pass@localhost:5432/db1" \
  --source-query "SELECT * FROM public.orders" \
  --sink-type mysql \
  --sink-uri "mysql+pymysql://user:pass@localhost:3306/db2" \
  --sink-table "clean_orders"
```

No interim CSV downloads. No `df.to_sql()` memory bloat. The Rust connector streams the Postgres query directly into Polars, the pipeline evaluates the schema, and the ADBC driver pushes the binary stream into MySQL. Zero friction.

---

## Data Contracts as YAML 📜

Airflow DAGs are notoriously complex to test because business logic gets entangled with infrastructure logic. `detl` enforces absolute data integrity via decoupled Pydantic validators. Before data ever reaches your target Sink, it must satisfy your standalone YAML contract.

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

  transaction_amount:
    dtype: numeric
    constraints:
      range: { min_val: 0, max_val: 1000000, violate_action: { tactic: "drop_row" } }
```

In 15 lines of YAML, we just instructed the Engine to natively drop records with emails shorter than 5 chars, validate a regex pattern (crashing the pipeline if broken), strip trailing spaces, format parsed ISO dates down to pure Years, drop negative financial transactions, and impute null dates.

The Polars compiler absorbs this YAML and pushes it down into a highly optimized, multi-threaded Execution Plan.

---

## Built for Chaos (Idempotency & Resilience) 🛡️

We built `detl` defending against malicious environments and chaotic infrastructure.
- **OOM Shielding:** Sinks and Sources map directly via `--source-batch-size 50000` constraints, bounding memory footprints absolutely regardless of how massive the data lake is.
- **Idempotency Control:** Supplying `--sink-if-exists replace` guarantees that running the exact same pipeline yields the exact same state locally or remotely, preventing data duplication.
- **SQLi Protection:** Native cursor bindings intercept and drop Bobby Tables payloads targeting the extraction core (`SELECT * FROM users; DROP TABLE users;--`).

### The Bottom Line
Stop rewriting boilerplate validation loops. Start defining contracts. Let Rust and C++ handle the physical transfers.

Check out the repo, grab the `pip` install, and route your first matrix pipeline today!

🔗 **[GitHub Repository: glebka1337/detl](https://github.com/glebka1337/detl)**

***

<br><br>

---

# 🚀 The Publication Guide: How to Deploy This Article

If you want this article to go viral in the developer community, do not simply post it on your personal Medium profile. Follow this exact deployment pipeline to maximize impressions.

### Step 1: Prepare the Visual Assets
1. **The Cover Image:** Find a high-quality widescreen image on [Unsplash.com](https://unsplash.com/). Search for terms like "Data Flow", "Matrix", or "Neon Server". Insert it at the absolute top of the article.
2. **The Terminal GIF:** Use [Asciinema](https://asciinema.org/) to record your terminal while executing the `detl` Postgres-to-MySQL matrix routing command. Convert the Asciinema cast to a GIF and embed it right below the YAML code block to prove the speed and UI of the tool.

### Step 2: Pitching to "Towards Data Science" (TDS)
TDS is the largest Data Engineering publication on Medium. Getting accepted here guarantees thousands of targeted readers.
1. Create a Draft on your Medium account and paste this exact article. **Do not click Publish.**
2. Go to the [Towards Data Science Contributor Page](https://towardsdatascience.com/questions-and-feedback-9fb5e0bcbc0c) (or google "TDS submit article").
3. Fill out their submission form linking to your Draft URL.
4. *Alternative Publications:* If TDS takes too long, submit the draft to **Better Programming** or **Level Up Coding**.

### Step 3: SEO and Tags
Before submission, configure the Medium post settings:
* **SEO Title:** `Declarative ETL: Replacing Pandas with Polars & Pydantic`
* **SEO Description:** `How to stop writing boilerplate data engineering scripts using detl, a zero-copy CLI tool powered by Polars, ConnectorX, and Pydantic.`
* **5 Critical Tags:** `Data Engineering`, `Python`, `Polars`, `Open Source`, `ETL`.

### Step 4: The Developer Cross-Post (Reddit & HackerNews)
Once Medium approves and publishes the article, do the following on Tuesday or Wednesday morning (EST hours):
1. **Reddit (`r/dataengineering` & `r/Python`):** Do not post the raw link! Post a text discussion titled: *"I got tired of Pandas OOM crashing my ETLs, so I wrapped Polars and Pydantic into a declarative CLI."* Paste a summary of the problem, a snippet of the YAML, and drop the Medium link at the very end.
2. **Hacker News:** Submit the link with the title: `Show HN: detl – Zero-copy declarative ETL CLI using Polars and Pydantic`.
3. **Habr (Russian Market):** Translate this article into Russian. Structure it rigidly. Habr loves performance metrics, so add a paragraph comparing the run-time of `df.to_sql()` versus `detl`'s Matrix Routing. Don't forget to set the "Canonical Link" in Habr settings to point back to the original Medium article to protect SEO rankings!
