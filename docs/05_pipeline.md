# 5. Pipeline Transformations (`pipeline`)

The Pipeline operates sequentially *after* all schemas, constraints, duplicate logic, and null policies have fired on the Polars `LazyFrame`. By the time these sequences execute, the data is guaranteed to be clean.

*Note: All pipeline mutations are resolved heavily natively in a lazy SQLContext.*

---

### `filter`
Accepts a raw SQL WHERE-clause string.

**✅ DO:**
```yaml
pipeline:
  - filter: "age >= 18 AND status = 'active'" # Filters out rows extremely fast on the LazyFrame.
```

---

### `mutate`
Accepts a dictionary mapping `new_column: SQL AS expression`. It natively modifies or appends new columns.

**✅ DO (Business Aggergations):**
```yaml
pipeline:
  - mutate:
      total_revenue: "price * volume"
      is_vip: "total_revenue > 10000"
```

**❌ DON'T (Data Cleaning):**
```yaml
pipeline:
  - mutate:
      age: "COALESCE(age, 32)" 
      # DON'T MIX CLEANING WITH PIPELINES!
      # Use `columns.age.on_null` schema block with tactic: "fill_median" instead. 
```

---

### `rename`
Accepts a dictionary mapping `old_name: new_name`.

**✅ DO:**
```yaml
pipeline:
  - rename:
      old_legacy_uuid: "id"
```

---

### `sort`
Performs `LazyFrame.sort()`. Requires a `by: "column"` parameter, and an optional `order: "asc" | "desc"` parameter (defaults to `asc`).

**✅ DO:**
```yaml
pipeline:
  - sort:
      by: "total_revenue"
      order: "desc"
```
