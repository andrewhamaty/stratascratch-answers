# PostgreSQL Solution
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