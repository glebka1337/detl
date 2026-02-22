# 4. Constraint Enforcement (`constraints`)

Constraints define strict boundaries that evaluate the validity of data and trigger policies (`violate_action`) if those thresholds are crossed.

Every constraint mandates a `violate_action`.
Available `violate_action.tactic`:
- `drop_row`: Obliterates the offending row.
- `fail`: Aborts engine.
- `fill_value`: Overrides the offending cell with `violate_action.value: <str|int|float>`.
- `fill_min`: Replaces offending records (like underages) with the absolute lowest non-offending value.
- `fill_max`: Replaces offending records with the highest value.

---

### Numerical Boundaries: `min_policy` and `max_policy`
Requires a `threshold: <float|int>`. Strict Pydantic validators will abort if this is mapped to a `string` column.

**✅ DO (Fallback Violators intelligently):**
```yaml
columns:
  age:
    dtype: int
    constraints:
      min_policy:
        threshold: 18
        violate_action:
          tactic: "fill_value"
          value: 18 # Anyone under 18 is artificially capped to 18 minimum.
```

**❌ DON'T (String Fallacies):**
```yaml
columns:
  name:
    dtype: string
    constraints:
      max_policy:
        threshold: 15 # You cannot limit a text string using mathematical `max_policy`. Use `max_length`!
```

---

### String Limits: `min_length` and `max_length`
Requires a `length: <int>`. Drop titles that are too short or truncate/drop bloated texts.

**✅ DO:**
```yaml
columns:
  review_text:
    dtype: string
    constraints:
      max_length:
        length: 500
        violate_action:
          tactic: "drop_row" # Drops overly bloated spam reviews entirely.
```

---

### Standardized Formats: `regex`
Matches cell values against custom REGEX patterns. Great for strictly formatting IDs, SSNs, or Phone Numbers. Requires `pattern: <string>`.

**✅ DO (Regex Strict Checks):**
```yaml
columns:
  postal_code:
    dtype: string
    constraints:
      regex:
        pattern: '^\d{5}$'
        violate_action:
          tactic: "fail" # Immediately breaks if postal boundaries are broken, since this defines core metrics in downstream DWs.
```

---

### Categorical Boundaries: `allowed_values`
Ensures all values conform strictly to a categorical Enum.
- Config 1: `values: ["A", "B", "C"]` directly inside YAML.
- Config 2: `source: "dictionary.csv"` checks values against a 1D CSV list.

**✅ DO (Enforce Enums & Dicts safely):**
```yaml
columns:
  role:
    dtype: string
    constraints:
      allowed_values:
        source: "valid_roles.csv"
        separator: ";" # Optional. Defaults to ",". ONLY valid for .txt and .csv files. Use it if your dictionary uses a custom delimiter.
        violate_action:
          tactic: "fill_value"
          value: "guest" # Catches dirty inputs ("AdmIn", "guest1") and safely maps them to a generic "guest".
```

**❌ DON'T (Separators for Numpy arrays):**
```yaml
columns:
  status:
    dtype: string
    constraints:
      allowed_values:
        source: "allowed_statuses.npy"
        separator: "," # DON'T DO THIS! Numpy binary files (.npy) don't use string delimiters. Pydantic will block this manifest immediately.
        violate_action:
          tactic: "drop_row"
```

---

### Uniqueness: `unique`
Identifies columns that strictly must carry unique identities (e.g. Primary Keys).
- Tactics: `drop_extras` (keep first row) or `fail`.

**✅ DO (Drop duplicates smoothly):**
```yaml
columns:
  user_id:
    dtype: string
    constraints:
      unique:
        tactic: "drop_extras" # If a duplicate ID shows up mid-stream, gently remove it without breaking the pipeline.
```

---

### Custom SQL Evaluations: `custom_expr`
Provides raw SQL boolean expression capacity. Best used for **cross-column logic** if isolated constraints fall short.

**✅ DO (Cross-Column Boolean Enforcement):**
```yaml
columns:
  tax_logic:
    dtype: boolean
    constraints:
      custom_expr:
        expr: "tax_bracket > 0 AND income > 15000"
        violate_action:
          tactic: drop_row # Erases the record if they declare taxes while under poverty limit
```

**❌ DON'T (SQL Overuse):**
```yaml
columns:
  score:
    dtype: int
    constraints:
      custom_expr:
        expr: "score > 10 AND score < 100" 
        # DON'T DO THIS! Use native `min_policy` and `max_policy` instead!
        # Why? The engine natively evaluates min/max at the C++ level.
        # `custom_expr` forcibly engages the heavy SQLContext overhead.
```
