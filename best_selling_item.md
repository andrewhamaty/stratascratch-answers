# Risky Projects

## PostgreSQL Solution
```{sql}
with monthly_sales_cte as (
    select
        extract(month from online_retail.invoicedate) as month
      , online_retail.description
      , sum(online_retail.unitprice * online_retail.quantity) as total_paid
    from online_retail
    where online_retail.quantity >= 0
    group by
        month
      , online_retail.description
)
, products_ranked as (
    select
        *
      , dense_rank() over (partition by month order by total_paid desc) as drank
    from monthly_sales_cte
)
select
    month
  , description
  , total_paid
from products_ranked
where drank = 1
order by month asc;

```
## Python-Pandas Solution
```{python}
import pandas as pd

(online_retail
    .loc[online_retail["quantity"] >= 0]
    .assign(month=lambda df: df["invoicedate"].dt.month)
    .assign(total_paid=lambda df: df["unitprice"] * df["quantity"])
    .groupby(["month", "description"], as_index=False)
    .agg(total_paid=("total_paid", "sum"))
    .assign(drank=lambda df: df.groupby("month")["total_paid"]
                               .rank(method="dense", ascending=False))
    .loc[lambda df: df["drank"] == 1]
    .sort_values(by="month", ascending=True)
    .loc[:, ["month", "description", "total_paid"]]
)
```

## pySpark Solution
```{python}
import pyspark
import pyspark.sql.functions as F
from pyspark.sql import Window

window = Window.partitionBy("month").orderBy(F.col("total_paid").desc())

(online_retail
    .filter(F.col("quantity") >= 0)
    .withColumn( "month", F.month(F.col("invoicedate")) )
    .withColumn( "total_paid", F.col("unitprice") * F.col("quantity") )
    .groupBy(["month", "description"])
    .agg( F.sum("total_paid").alias("total_paid") )
    .withColumn( "drank", F.dense_rank().over(window) )
    .filter(F.col("drank") == 1)
    .orderBy("month", ascending=True)
    .select(["month", "description", "total_paid"])
    .toPandas()
)
```
