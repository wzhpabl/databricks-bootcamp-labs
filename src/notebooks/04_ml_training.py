# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Lab 04 · Feature engineering in Unity Catalog + churn model
# MAGIC 1. Compute customer features from `orders_enriched`
# MAGIC 2. Register them as a feature table (primary key `customer_id`)
# MAGIC 3. Build a training set with automatic feature lookups
# MAGIC 4. Train, then register two models:
# MAGIC    * `churn_model_fs` — packaged with its feature metadata (batch scoring with automatic lookups)
# MAGIC    * `churn_model` — plain scikit-learn model for the real-time endpoint (part B)
# MAGIC 5. (Optional) publish features to an online store (uses your one Lakebase project on Free Edition)
# MAGIC
# MAGIC Also run by the `train_churn_model` job (part B).

# COMMAND ----------

# MAGIC %pip install -q databricks-feature-engineering scikit-learn "mlflow>=3.1"
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

import mlflow
from pyspark.sql import functions as F

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

fe = FeatureEngineeringClient()
feature_table = f"{catalog}.{schema}.customer_features"
model_name = f"{catalog}.{schema}.churn_model"          # real-time serving (lab 04)
model_name_fs = f"{catalog}.{schema}.churn_model_fs"    # feature-aware (batch scoring)
dbutils.widgets.text("experiment_path", "")
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# MAGIC %md ## 1 · Compute features

# COMMAND ----------

orders = spark.table("orders_enriched")
as_of = orders.agg(F.max("order_date")).first()[0]

# SOLUTION-BEGIN lab-04: Aggregate orders per customer_id into: total_orders, total_revenue, avg_order_value, days_since_last_order (vs as_of), distinct_categories and mobile_share (share of orders with channel = 'mobile').
features = (
    orders.groupBy("customer_id")
    .agg(
        F.count("*").alias("total_orders"),
        F.sum("amount").cast("double").alias("total_revenue"),
        F.avg("amount").cast("double").alias("avg_order_value"),
        F.datediff(F.lit(as_of), F.max("order_date")).alias("days_since_last_order"),
        F.countDistinct("category").alias("distinct_categories"),
        F.avg(F.when(F.col("channel") == "mobile", 1).otherwise(0)).alias("mobile_share"),
    )
)
# SOLUTION-END
display(features)

# COMMAND ----------

# MAGIC %md ## 2 · Register the feature table

# COMMAND ----------

# SOLUTION-BEGIN lab-04: Create the feature table (fe.create_table with primary_keys=["customer_id"]) the first time, and fe.write_table(mode="merge") when it already exists.
if spark.catalog.tableExists(feature_table):
    fe.write_table(name=feature_table, df=features, mode="merge")
else:
    fe.create_table(
        name=feature_table,
        primary_keys=["customer_id"],
        df=features,
        description="Customer behaviour features for churn prediction (bootcamp lab 04)",
    )
# SOLUTION-END
spark.sql(f"ALTER TABLE {feature_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

# MAGIC %md ## 3 · Training set with automatic feature lookup

# COMMAND ----------

labels = spark.read.json(f"{raw_path}/labels/").select("customer_id", "churned")

# SOLUTION-BEGIN lab-04: Build a training set from labels with a FeatureLookup on the feature table (lookup_key customer_id), label 'churned', excluding customer_id; load it as pandas.
training_set = fe.create_training_set(
    df=labels,
    feature_lookups=[FeatureLookup(table_name=feature_table, lookup_key="customer_id")],
    label="churned",
    exclude_columns=["customer_id"],
)
pdf = training_set.load_df().toPandas().fillna(0)
# SOLUTION-END
pdf.head()

# COMMAND ----------

# MAGIC %md ## 4 · Train, evaluate and register

# COMMAND ----------

from mlflow.tracking import MlflowClient
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split

X = pdf.drop(columns=["churned"])
y = pdf["churned"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42, stratify=y)

mlflow.set_experiment(dbutils.widgets.get("experiment_path") or f"/Users/{_user}/bootcamp-churn")
with mlflow.start_run(run_name="gbt-baseline") as run:
    model = GradientBoostingClassifier(random_state=42).fit(X_train, y_train)
    auc = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])
    mlflow.log_metric("test_auc", auc)
    # SOLUTION-BEGIN lab-04: Log the model with fe.log_model (flavor=mlflow.sklearn, training_set=training_set) registered as model_name_fs, and a plain mlflow.sklearn.log_model registered as model_name with an input example.
    fe.log_model(
        model=model,
        artifact_path="model_fs",
        flavor=mlflow.sklearn,
        training_set=training_set,
        registered_model_name=model_name_fs,
    )
    mlflow.sklearn.log_model(
        sk_model=model,
        name="model",
        registered_model_name=model_name,
        input_example=X_test.head(5),
    )
    # SOLUTION-END
print(f"test AUC = {auc:.3f}")

# COMMAND ----------

client = MlflowClient()
latest = max(int(v.version) for v in client.search_model_versions(f"name='{model_name}'"))
client.set_registered_model_alias(model_name, "champion", latest)
print(f"{model_name} v{latest} is now @champion")

# COMMAND ----------

# MAGIC %md ### Batch scoring with automatic feature lookup
# MAGIC Only customer IDs are passed in — features are looked up from the feature table.

# COMMAND ----------

fs_version = max(int(v.version) for v in client.search_model_versions(f"name='{model_name_fs}'"))

scored = fe.score_batch(model_uri=f"models:/{model_name_fs}/{fs_version}", df=labels.select("customer_id"))

display(scored.select("customer_id", "prediction").limit(20))
(scored.select("customer_id", "prediction")
       .withColumnRenamed("prediction", "churn_predicted")
       .write.mode("overwrite").option("overwriteSchema", True).saveAsTable("churn_predictions"))
spark.sql("COMMENT ON TABLE churn_predictions IS 'Daily churn predictions from churn_model_fs (lab 04)'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · (Optional) Online features
# MAGIC Create an online store once (Free Edition: one Lakebase project per account), then set `publish_online = True` to publish
# MAGIC your feature table and query it with low latency. See the Feature Engineering docs if the API differs in your workspace.

# COMMAND ----------

publish_online = False
if publish_online:
    try:
        store = fe.get_online_store(name="bootcamp-online-store")
    except Exception:  # noqa: BLE001 — first run: create it (scale-to-zero capacity)
        store = fe.create_online_store(name="bootcamp-online-store", capacity="CU_1")
    fe.publish_table(
        online_store=store,
        source_table_name=feature_table,
        online_table_name=f"{feature_table}_online",
    )

# COMMAND ----------

