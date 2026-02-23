# %%
import polars as pl

df = pl.read_csv("airline_route_profitability.csv")

df.head()

# %%
df.null_count()

# %%
df.collect_schema()
# %%

df_clean = pl.read_csv('./clean_airlines.csv')
df_clean.select('Catering_Cost').filter(False)
# %%
