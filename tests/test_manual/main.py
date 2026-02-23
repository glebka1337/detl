# %%
import polars as pl

df_raw = pl.read_csv('./airline_route_profitability.csv')

df_raw.head()
# %%
df_raw.collect_schema()
# %%

df_clean = pl.read_csv('./clean_airlines.csv')

df_clean.collect_schema()
# %%
df_clean.select(pl.col('Flight_Date')).dtypes
# %%
