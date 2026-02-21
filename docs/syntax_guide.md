# DETL Syntax & Configuration Guide

Welcome to the comprehensive syntax guide for `detl` (Declarative ETL). This document details every attribute, available data types, and null/constraint tactics. It explicitly outlines how to configure powerful Data Contracts and robust Pipeline chains using best practices.

---

## 1. Global Engine Configuration (`conf`)

The `conf` block dictates early engine behaviors.

### `undefined_columns`
Determines what the engine does to columns found in the input DataFrame that are **not** present in the `columns` schema block.
- **`drop`** (Recommended): Safely ignores extra columns.
- **`keep`**: Allows unmapped columns to pass through untouched.

### `on_duplicate_rows`
Determines how exact or subset-match duplicate rows are handled. Contains `tactic` and an optional `subset` list.
- **`tactic`**:
  - `keep`: Do nothing.
  - `drop_extras` (Recommended): Drops subsequent duplicates, retaining the first occurrence.
  - `fail`: Halts execution immediately if duplicates are identified.
- **`subset`**: `["col_name1", "col_name2"]` — If provided, duplication is determined strictly based on this combination. If omitted, duplication is evaluated across all columns.

---

## 2. Column Typings (`columns`)

The `columns` dictionary is the core schema block. Every defined column mandates a `dtype`.

### Supported `dtype`s
- `string`, `int`, `float`, `boolean`, `date`, `datetime`

### Date Formatting (`date_format`)
If `dtype` is `date` or `datetime`, a specialized parsing block `date_format` can be attached to guide formatting and handle bad strings.

```yaml
created_at:
  dtype: datetime
  date_format:
    in_format: "%Y-%m-%d %H:%M:%S"
    on_parse_error:
      tactic: "drop_row" # Recommended for invalid timestamps
```
**`on_parse_error` Tactics**:
- `fail`: Hard abort.
- `drop_row`: Erases the record entirely if it fails to parse.
- `fill_value`: Fallback to a static valid datetime (requires `value: "1970-01-01 00:00:00"`).

---

## 3. Handling Missing Data (`on_null`)

Defines how native `null` arrays are imputed.

Available `tactic` options:
1. **`drop_row`**: Drops the entire row containing a missing value.
2. **`fail`**: Hard-aborts pipeline (Be very cautious using this on user-provided streams).
3. **`fill_value`**: Hardcodes a static given parameter `value`. (e.g., `value: "Unknown"`).
4. **`fill_mean`**: Replaces nulls with the average (Requires numerical columns).
5. **`fill_median`**: Replaces nulls with the median (Better for skewed numerical distributions like Income).
6. **`fill_most_frequent`**: Replaces nulls with the string or int mode of the column.
7. **`ffill`**: Forward-fills linearly.
8. **`bfill`**: Backward-fills linearly.

```yaml
salary:
  dtype: float
  on_null:
    tactic: fill_median # Intelligently calculates median without breaking lazy-streams.
```

---

## 4. Constraint Enforcement (`constraints`)

Constraints define strict boundaries that evaluate the validity of data and trigger policies (`violate_action`) if those thresholds are crossed.

Every constraint mandates a `violate_action`.
Available `violate_action.tactic`:
- `drop_row`: Obliterates the offending row.
- `fail`: Aborts engine.
- `fill_value`: Overrides the offending cell with `violate_action.value: <x>`.

### Numerical Limits: `min_policy` and `max_policy`
Requires a `threshold: <float|int>`.
```yaml
age:
  dtype: int
  constraints:
    max_policy:
      threshold: 120
      violate_action:
        tactic: drop_row
```

### String Limits: `min_length` and `max_length`
Requires a `length: <int>`. Drop titles that are too short or truncate/drop bloated texts.

### Standardized Formats: `regex`
Matches cell values against custom REGEX patterns. Great for strictly formatting IDs, SSNs, or Phone Numbers. Requires `pattern: <string>`.
```yaml
postal_code:
  dtype: string
  constraints:
    regex:
      pattern: '^\d{5}$'
      violate_action:
        tactic: "fail"
```

### Categorical Boundaries: `allowed_values`
Ensures all values conform strictly to a categorical Enum.
- Config 1: `values: ["A", "B", "C"]` directly inside YAML.
- Config 2: `source: "dictionary.csv"` checks values against a 1D CSV list.

### Uniqueness: `unique`
Identifies columns that strictly must carry unique identities (e.g. Primary Keys).
- Tactics: `drop_extras` (keep first row) or `fail`.
```yaml
user_id:
  dtype: int
  constraints:
    unique:
      tactic: drop_extras
```

### Custom SQL Evaluations: `custom_expr`
Provides raw SQL boolean expression capacity. Best used for cross-column logic if isolated constraints fall short.
```yaml
tax_logic:
  dtype: boolean
  constraints:
    custom_expr:
      expr: "tax_bracket > 0 AND income > 15000"
      violate_action:
        tactic: drop_row
```

---

## 5. Pipeline Transformations (`pipeline`)

The Pipeline operates sequentially *after* all schemas, constraints, duplicate logic, and null policies have fired on the Polars `LazyFrame`. By the time these sequences execute, the data is guaranteed to be clean.

*Note: All pipeline mutations are resolved in a lazy SQLContext.*

### `filter`
Accepts a raw SQL WHERE-clause string.
```yaml
- filter: "age >= 18 AND status = 'active'"
```

### `mutate`
Accepts a dictionary mapping `new_column: SQL AS expression`. It natively modifies or appends new columns.
```yaml
- mutate:
    total_revenue: "price * volume"
    is_vip: "total_revenue > 10000"
```

### `rename`
Accepts a dictionary mapping `old_name: new_name`.
```yaml
- rename:
    old_id: "id"
    txt_desc: "description"
```

### `sort`
Performs `LazyFrame.sort()`. Requires a `by: "column"` parameter, and an optional `order: "asc" | "desc"` parameter (defaults to `asc`).
```yaml
- sort:
    by: "total_revenue"
    order: "desc"
```

---

## Do's and Don'ts Best Practices

### Do's ✅
- **Use `undefined_columns: "drop"`**: Prevents downstream data-warehouses from crashing when unmapped schemas are sent.
- **Leverage native policies instead of SQL**: It is highly optimized to run `min_policy` rather than a `custom_expr`.
- **Enforce Enums safely via `allowed_values`**: Catch dirty inputs (`"AdmIn"`, `"guest1"`) and map `violate_action` back to a `"fill_value": "user"` or simple `drop_row` to guarantee downstream models only receive standard categories.

### Don'ts ❌
- **Avoid Over-using `fail` Tactics**: A data contract is inherently designed to resolve dirty data dynamically using `drop_row` or `fill_median`. Hard-failing pipelines on gigabyte scale streams frequently will disrupt business operations. Save `fail` only for explicitly fatal Primary Key corruptions.
- **Do not mix Data Cleaning with Pipeline Engineering**: Do not attempt to use `- mutate: "COALESCE(age, 32)"` inside the pipeline block. Use the native `columns.age.on_null` schema instead. The Pipeline context must only be utilized for **Business Logic transformations** (sums, advanced filtering, aggregates), preserving code clarity and readability.
