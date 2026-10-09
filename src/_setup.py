# Databricks notebook source
# MAGIC %md
# MAGIC Shared setup: `%run ./_setup` from any lab notebook. Creates `catalog` / `schema` widgets
# MAGIC (leave `schema` empty to use your dev schema), sets them as the session defaults and
# MAGIC exposes `catalog`, `schema` and `raw_path` variables.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(".."))

from bootcamp_lib import context  # noqa: E402

dbutils.widgets.text("catalog", "bootcamp_dev")
dbutils.widgets.text("schema", "")

_user = spark.sql("SELECT current_user()").first()[0]
catalog, schema = context.resolve(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"), _user)
raw_path = f"/Volumes/{catalog}/{schema}/raw"

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")
print(f"user={_user}\ncatalog={catalog}\nschema={schema}\nraw_path={raw_path}")