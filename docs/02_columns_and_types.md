# 2. Column Typings (`columns`)

The `columns` dictionary is the core schema block. Every defined column mandates a `dtype`. The core pipeline relies fundamentally on Polars native schemas.

### Supported `dtype`s
- `string`: standard text mapping.
- `int`/`float`: strict numeric parsing constraints.
- `boolean`: truthy values.
- `date` / `datetime`: Dedicated chronological structs.

**DO:**
```yaml
columns:
  user_id:
    dtype: int
  is_active:
    dtype: boolean
```

**DON'T:**
```yaml
columns:
  price:
    dtype: string # DON'T cast numbers to strings. You cannot run statistical pipelines on string prices!
```

---

### Implicit Type Casting (Coercion)
`detl` leverages Polars' native type casting engine (`strict=False` by default for primitives). This means the engine will attempt to coerce incoming data into your defined `dtype` automatically if a logical path exists, rather than crashing immediately.

**Common Implicit Casts:**
- `integer/float` ➔ `string`: Numbers are cleanly stringified (e.g., `12.5` becomes `"12.5"`).
- `integer/float` ➔ `boolean`: `0` or `0.0` translates to `false`, while any non-zero number translates to `true`.
- `string` ➔ `numeric`: The engine will attempt to parse valid number strings. Unparseable strings become `null` and are subsequently handled by your `on_null` tactic.

*Note: If you are relying on strict zero-to-false mappings (e.g., mapping `Catering_Cost: 100` to `boolean: true`), ensure this behavior is intentional.*

---

### Date Formatting (`format`)
If `dtype` is `date` or `datetime`, a specialized parsing block `format` can be attached to guide formatting and handle unparseable strings.

**DO (Safe Fallback Parsing):**
```yaml
columns:
  created_at:
    dtype: datetime
    format:
      input: "%Y-%m-%d %H:%M:%S"
      output: "%Y-%m-%d" # Automatically truncates the time!
      on_parse_error:
        tactic: "drop_row" # Silently removes bad timestamp strings like "N/A"
```

**DON'T (Hard Failing):**
```yaml
columns:
  created_at:
    dtype: datetime
    format:
      input: "%Y-%m-%d"
      on_parse_error:
        tactic: "fail" # CAUTION! If someone types "2023-13-40", the engine immediately crashes.
```

### Date Parsing Errors (`on_parse_error`)
When dealing with temporal strings, the source data might contain unparseable garbage like `"N/A"`, `"missing"`, or illogical dates like `"2024-13-45"`.

The `format.on_parse_error` configuration allows you to define exactly how `detl` evaluates these dirty values via two explicit tactics:
- `drop_row`: (Default) The engine will safely convert the unparseable string into a `null`. If a fallback policy (`on_null`) is defined for this column, the `null` will subsequently be resolved via the specified fallback. Otherwise, the row maintains a null value or fails based on standard strictness checks.
- `fail`: The engine acts defensively and aggressively. If *any* string violates the strictly defined `in_format`, `detl` crashes execution instantly. This is extremely important if dropping structural data implies downstream catastrophic failure.

---

### Robust Date Handling and Fallbacks
When specifying fallback values (`fill_value`) for `date` or `datetime` types in your configuration (either globally via `defaults` or specifically per column), `detl` strictly enforces type and formatting mapping.

You must provide the fallback value as a **string** that strictly matches the `date_format.input` defined for the column. The execution engine dynamically parses your string fallback into a native temporal struct before imputation, avoiding silent type crashes.

**DO (Correct Typed Fallback):**
```yaml
columns:
  created_at:
    dtype: date
    format:
      input: "%Y-%m-%d"
    on_null:
      tactic: "fill_value"
      value: "2024-01-01" # Valid!
```

**DON'T (Invalid Type Fallback):**
```yaml
columns:
  created_at:
    dtype: date
    format:
      input: "%Y-%m-%d"
    on_null:
      tactic: "fill_value"
      value: 20240101 # FAILS: detl will natively block numeric fallbacks on date columns!
```

---

### Output Formatting and Aliasing (`rename`)
Because `detl` is fundamentally a structural ETL orchestrator, native Polars datatypes are maintained strictly in memory through the entire transformational mapping pipeline.

However, sometimes you need the exported shape of the data to differ from internal evaluation formats. `detl` applies structural output mutations as the **absolute final step** before pushing data to a Connector.

#### Date Output Formatting
If a column is typed as `date` or `datetime`, and a `format.output` is defined alongside the parser format, `detl` will stringify the native Temporal object specifically to the assigned schema immediately before writing to Sinks like CSVs or external APIs.

```yaml
columns:
  birth_date:
    dtype: date
    format: 
      input: "%Y-%m-%d" # Read source as ISO
      output: "%d.%m.%Y" # Push output as European format (e.g. 10.10.2025)
```

#### Column Renaming
To map an evaluated column to a different alias in the target system (e.g., standardizing Legacy SQL naming conventions to generic API payloads), use the `rename` directive. 

```yaml
columns:
  Legacy_Database_Timestamp_Field:
    rename: "created_at" # Output connector will write the datastream using this property name
    dtype: datetime
```
