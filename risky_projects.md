# Risky Projects

## PostgreSQL Solution
```{sql}
select
    proj.title
  , proj.budget
  , ceiling(sum(emp.salary * ((proj.end_date - proj.start_date)*1.0 / 365))) as prorated_employee_expense
from linkedin_projects as proj
inner join linkedin_emp_projects as lookup
  on proj.id = lookup.project_id
inner join linkedin_employees as emp
  on emp.id = lookup.emp_id
group by
    proj.title
  , proj.budget
having
    proj.budget < ceiling(sum(emp.salary * ((proj.end_date - proj.start_date)*1.0 / 365)))
order by proj.title asc;
```

## Python-Polars Solution
```{python}
import polars as pl
import pandas as pd

(linkedin_projects
    .join(linkedin_emp_projects, left_on="id", right_on="project_id", how="inner")
    .join(linkedin_employees, left_on="emp_id", right_on="id", how="inner")
    .with_columns( (pl.col("end_date") - pl.col("start_date")).dt.total_days().alias("project_duration_days") )
    .with_columns( (pl.col("salary") / 365).alias("daily_salary") )
    .with_columns( (pl.col("salary") / 365 * (pl.col("end_date") - pl.col("start_date")).dt.total_days()).alias("prorated_employee_costs") )
    .group_by(["title", "budget"])
    .agg(pl.col("prorated_employee_costs").sum().ceil().alias("prorated_expense"))
    .filter(pl.col("prorated_expense") > pl.col("budget"))
    .sort("title", descending=False)
    .collect()
    .to_pandas()
)
```

## pySpark Solution

```{python}
import pyspark.sql.functions as F

(linkedin_projects
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

## R Solution

```{r}
library(dplyr)

linkedin_projects |>
  inner_join(linkedin_emp_projects, by = c("id" = "project_id")) |>
  inner_join(linkedin_employees, by = c("emp_id" = "id")) |>
  mutate(project_duration_days = difftime(end_date, start_date, units = "days")) |>
  mutate(daily_salary = salary/365) |>
  mutate(prorated_employee_costs = daily_salary * project_duration_days) |>
  group_by(title, budget) |>
  summarize(prorated_expense = ceiling(sum(prorated_employee_costs))) |>
  filter(prorated_expense > budget) |>
  arrange(title)
```