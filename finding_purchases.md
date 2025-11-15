# Risky Projects

## pySpark Solution
```{python}
import pyspark.sql.functions as F
from pyspark.sql import Window

w = Window.partitionBy("user_id").orderBy(F.desc("created_at"))

(
    amazon_transactions
    .withColumn("next_purchase", F.lag("created_at", 1).over(w))
    .withColumn("days_diff",
                F.datediff(F.col("next_purchase"), F.col("created_at"))
                )
    .filter(F.col("days_diff") <= 7) &
            (F.col("days_diff") > 0)
            )
    .select("user_id")
    .dropDuplicates()
    .toPandas()

)
```
