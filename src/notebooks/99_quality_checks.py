# Databricks notebook source
# MAGIC %md
# MAGIC # Quality checks (integration test)
# MAGIC Final task of `orders_job`. Fails the job — and therefore the CI deployment — if the pipeline output is
# MAGIC empty or the expectations dropped too many rows. Used in lab 06.

# COMMAND ----------

dbutils.widgets.text("catalog", "bootcamp_dev")
dbutils.widgets.text("schema", "sales")
dbutils.widgets.text("max_drop_rate", "0.05")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
max_drop_rate = float(dbutils.widgets.get("max_drop_rate"))

# COMMAND ----------

bronze = spark.table(f"`{catalog}`.`{schema}`.orders_bronze").count()
silver = spark.table(f"`{catalog}`.`{schema}`.orders_silver").count()
gold = spark.table(f"`{catalog}`.`{schema}`.revenue_by_region_daily").count()
drop_rate = 1 - silver / bronze if bronze else 1.0

print(f"bronze={bronze} silver={silver} gold={gold} drop_rate={drop_rate:.3%}")

assert bronze > 0, "No orders ingested"
assert gold > 0, "Gold table is empty"
assert drop_rate <= max_drop_rate, f"Expectations dropped {drop_rate:.1%} of rows (limit {max_drop_rate:.0%})"
print("All quality checks passed")