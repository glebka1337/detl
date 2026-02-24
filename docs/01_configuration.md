# 1. Global Engine Configuration (`conf`)

The `conf` block dictates early engine behaviors regarding overall dataset structure anomalies. It's the very first block in your YAML file.

### `undefined_columns`
Determines what the engine does to columns found in the input DataFrame that are **not** present in the `columns` schema block.
- **`drop`** (Recommended): Safely ignores extra columns.
- **`keep`**: Allows unmapped columns to pass through untouched.

**DO (Strict Contract):**
```yaml
conf:
  undefined_columns: "drop" # Protects downstream tables from unexpected schema bloat!
```

**DON'T (Loose Contract):**
```yaml
conf:
  undefined_columns: "keep" # Bad idea. If the source file accidentally contains 20 extra garbage columns, they will flow right into your database.
```

### `on_duplicate_rows`
Determines how exact or subset-match duplicate rows are handled. Contains `tactic` and an optional `subset` list.
- **`tactic`**:
  - `keep`: Do nothing.
  - `drop_extras` (Recommended): Drops subsequent duplicates, retaining the first occurrence.
  - `fail`: Halts execution immediately if duplicates are identified.
- **`subset`**: `["col_name1", "col_name2"]` — If provided, duplication is determined strictly based on this combination. If omitted, duplication is evaluated across all columns.

**DO (Smart Deduplication):**
```yaml
conf:
  on_duplicate_rows:
    tactic: "drop_extras"
    subset: ["user_id", "email"] # Only drops rows if BOTH user_id and email are identical.
```

**DON'T (Over-strict execution):**
```yaml
conf:
  on_duplicate_rows:
    tactic: "fail" # DON'T DO THIS on raw data sources! A single accidental duplicate will crash the engine on a 50GB file. Let the engine silently clean it using 'drop_extras'.
```

### `defaults`
Provides the ability to map global type-level fallback policies. This is extremely powerful when combined with `undefined_columns: "keep"`, allowing dynamically inferred columns to inherit rules automatically.

- Maps rules by dataset `dtype` (`string`, `int`, `float`, `date`, `datetime`, `boolean`).
- You can declare default `on_null` tactics and generic `constraints`.

**DO (Global Fallbacks):**
```yaml
conf:
  undefined_columns: "keep"
  defaults:
    string:
      on_null:
        tactic: "fill_value"
        value: "UNKNOWN"
    int:
      on_null:
        tactic: "fill_median"
```
*In the example above, any string column lacking an explicit definition will automatically fall back to "UNKNOWN" when Null, and inferred integers gracefully median-fill.*
