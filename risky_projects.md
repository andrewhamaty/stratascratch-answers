# PySpark Solution

```{python}
import pyspark.sql.functions as F

project_emplotyee_details = (linkedin_projects
    .join(linkedin_emp_projects, 
          on=linkedin_projects["id"] == linkedin_emp_projects["project_id"],
          how="inner")
    .join(linkedin_employees,
          on=linkedin_employees["id"] == linkedin_emp_projects["emp_id"],
          how="inner")
    .withColumn( "project_duration_days", F.datediff(end="end_date", start="start_date") )
    .withColumn( "daily_salary", F.col("salary")/365 )
    .withColumn( "prorated_employee_costs", F.col("daily_salary")*F.col("project_duration_days") )
    .groupBy("title", "budget")
    .agg(F.ceil( F.sum(F.col("prorated_employee_costs")) ).alias("prorated_expense"))
    .filter(F.col("prorated_expense") > F.col("budget"))
    .orderBy("title", ascending=True)
    .toPandas()
)
```