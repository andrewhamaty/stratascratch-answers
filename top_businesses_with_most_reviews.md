# Risky Projects

## PostgreSQL Solution
```{sql}
with businesses_ranked_cte as (
    select
        yelp_business.name
      , yelp_business.review_count
      , rank() over (order by yelp_business.review_count asc) as ranking
    from yelp_business
)
select
    businesses_ranked_cte.name
  , businesses_ranked_cte.review_count
from businesses_ranked_cte
order by ranking desc
limit 5;
```
## Python-Pandas Solution
```{python}
import pandas as pd

(yelp_business
    .assign(ranking=lambda df:df['review_count'].rank(ascending=True))
    .sort_values(by="ranking", ascending=False)
    .loc[:, ["name", "review_count"]]
    .head(5)
)
```

## pySpark Solution

```{python}
import pyspark
import pyspark.sql.functions as F
from pyspark.sql import Window

(yelp_business
    .withColumn("ranking", F.rank().over(Window.orderBy(F.col("review_count").asc())))
    .orderBy(F.col("ranking"), ascending=False)
    .select(["name", "review_count"])
    .limit(5)
    .toPandas()
)
```
