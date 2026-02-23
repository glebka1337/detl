# 3. Handling Missing Data (`on_null`)

Defines how native `null` arrays are imputed.

Available `tactic` options:
1. **`drop_row`**: Drops the entire row containing a missing value.
2. **`fail`**: Hard-aborts pipeline (Be very cautious using this on user-provided streams).
3. **`fill_value`**: Hardcodes a static given parameter `value`. (e.g., `value: "Unknown"`).
4. **`fill_mean`**: Replaces nulls with the average (Requires numerical `int` / `float` columns).
5. **`fill_median`**: Replaces nulls with the median (Better for skewed numerical distributions like Income).
6. **`fill_min`**: Replaces nulls with the minimum value of the column (Requires numerical or date).
7. **`fill_max`**: Replaces nulls with the maximum value of the column (Requires numerical or date).
8. **`fill_most_frequent`**: Replaces nulls with the string or int mode of the column.
9. **`ffill`**: Forward-fills linearly.
10. **`bfill`**: Backward-fills linearly.

---

**DO (Intelligent Median Imputation):**
```yaml
columns:
  salary:
    dtype: float
    on_null:
      tactic: "fill_median" # Safely estimates a salary for missing users without skewing the dataset with extremes.
```

**DO (Fallbacks for Strings):**
```yaml
columns:
  country:
    dtype: string
    on_null:
      tactic: "fill_value"
      value: "Unknown" # Defaults empty strings to strictly 'Unknown'
```

**DON'T (Logical Fallacies):**
```yaml
columns:
  username:
    dtype: string
    on_null:
      tactic: "fill_mean" # DON'T DO THIS. You cannot calculate the 'average' of a string. Pydantic will block this manifest immediately.
```

**DON'T (Missing Required Params):**
```yaml
columns:
  role:
    dtype: string
    on_null:
      tactic: "fill_value" 
      # DON'T DO THIS! You forgot to supply the `value: "guest"` property. Pydantic will block this manifest.
```
