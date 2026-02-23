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

### Date Formatting (`date_format`)
If `dtype` is `date` or `datetime`, a specialized parsing block `date_format` can be attached to guide formatting and handle unparseable strings.

**DO (Safe Fallback Parsing):**
```yaml
columns:
  created_at:
    dtype: datetime
    date_format:
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
    date_format:
      input: "%Y-%m-%d"
      on_parse_error:
        tactic: "fail" # CAUTION! If someone types "2023-13-40", the engine immediately crashes.
```
