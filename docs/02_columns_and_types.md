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
