# Risky Projects

## pySpark Solution
```{python}
import pyspark.sql.functions as F
from pyspark.sql import Window

w = Window.orderBy(
    F.desc(F.col("total_emails")),
    F.asc(F.col("user_id"))
    )

(
    google_gmail_emails
    .select("from_user")
    .withColumnRenamed("from_user", "user_id")
    .groupby("user_id")
    .agg(F.count("user_id").alias("total_emails"))
    .withColumn("activity_rank", F.row_number().over(w))
    .orderBy(
        F.col("activity_rank").asc(),
        F.col("user_id").asc()
        )
    .toPandas()
)
```
