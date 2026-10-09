# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Lab 02 · Unity Catalog, Domains & Metric Views
# MAGIC Prerequisites: lab 01 completed; you created the groups `bootcamp_analysts` and
# MAGIC `bootcamp_engineers` and added yourself to `bootcamp_engineers` (lab 00, part 2.5).
# MAGIC
# MAGIC UI steps (Domains, glossary, lineage) are in `labs/lab-02-data-governance/README.md`.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

# MAGIC %md ## 1 · Privileges

# COMMAND ----------

# SOLUTION-BEGIN lab-02: Grant USE SCHEMA and SELECT on your schema to bootcamp_analysts, and USE SCHEMA, SELECT, MODIFY and CREATE TABLE to bootcamp_engineers. Then SHOW GRANTS on the schema.
spark.sql(f"GRANT USE SCHEMA, SELECT ON SCHEMA `{catalog}`.`{schema}` TO `bootcamp_analysts`")
spark.sql(f"GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA `{catalog}`.`{schema}` TO `bootcamp_engineers`")
display(spark.sql(f"SHOW GRANTS ON SCHEMA `{catalog}`.`{schema}`"))
# SOLUTION-END

# COMMAND ----------

# MAGIC %md ## 2 · Tag sensitive columns and mask them

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE customer_profiles ALTER COLUMN email SET TAGS ('pii' = 'email');
# MAGIC ALTER TABLE customer_profiles ALTER COLUMN last_name SET TAGS ('pii' = 'name');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SOLUTION-BEGIN lab-02: Create a SQL UDF mask_email(email STRING) that returns the e-mail for members of bootcamp_engineers and '***@<domain>' for everyone else, then apply it as a column mask on customer_profiles.email.
# MAGIC CREATE OR REPLACE FUNCTION mask_email(email STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Shows full e-mail to engineers, domain only to everyone else'
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('bootcamp_engineers') THEN email
# MAGIC   ELSE concat('***@', split_part(email, '@', 2))
# MAGIC END;
# MAGIC
# MAGIC ALTER TABLE customer_profiles ALTER COLUMN email SET MASK mask_email;
# MAGIC -- SOLUTION-END

# COMMAND ----------

# MAGIC %sql
# MAGIC -- You are in bootcamp_engineers: full e-mails. Now remove yourself from the group
# MAGIC -- (Settings → Identity and access → Groups), wait ~1 minute and re-run: e-mails are masked.
# MAGIC -- Add yourself back afterwards.
# MAGIC SELECT customer_id, first_name, email, region FROM customer_profiles LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stretch · the same control with ABAC
# MAGIC Attribute-based policies apply to **every** column tagged `pii`, in every table of the schema,
# MAGIC instead of one table at a time. Follow the ABAC docs for your workspace
# MAGIC (policy syntax and availability vary by release — metastore-level policies are Beta):
# MAGIC https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/
# MAGIC
# MAGIC Optional: create a *tag automation* rule (Beta) that tags every new column named `email` with `pii=email`.

# COMMAND ----------

# ABAC stretch: one policy that masks EVERY column tagged `pii`, in every table of the schema.
# Prerequisites: `pii` is a *governed* tag at the account level (ABAC only references governed tags),
# and you are on serverless compute or DBR 16.4+.

# One generic mask UDF: same contract as any column mask (returns the column's type)
spark.sql(f"""
CREATE OR REPLACE FUNCTION mask_pii(val STRING)
RETURNS STRING
COMMENT 'Redacts any PII-tagged column for non-exempt principals'
RETURN '***'
""")

# Step 2 applied a manual mask on customer_profiles.email — only ONE column mask can resolve
# per column per user, so unset it and let the policy govern it instead.
spark.sql(f"ALTER TABLE `{catalog}`.`{schema}`.customer_profiles ALTER COLUMN email UNSET MASK")
##spark.sql(f"ALTER TABLE `{catalog}`.`{schema}`.customer_profiles ALTER COLUMN email DROP MASK")

# Schema-level policy: matches any column carrying the `pii` tag (any value: email, name, ...)
# and inherits to every table in the schema. Principal-based access lives in TO/EXCEPT, not the UDF.
spark.sql(f"""
CREATE OR REPLACE POLICY pii_mask
ON SCHEMA `{catalog}`.`{schema}`
COMMENT 'Mask every pii-tagged column across the schema'
COLUMN MASK mask_pii
TO `All Users` EXCEPT `bootcamp_engineers`
FOR TABLES
MATCH COLUMNS has_column_tag('pii') AS pii_col
--MATCH COLUMNS has_tag('pii') AS pii_col
ON COLUMN pii_col
""")

display(spark.sql(f"SHOW EFFECTIVE POLICIES ON TABLE `{catalog}`.`{schema}`.customer_profiles"))

# COMMAND ----------

# MAGIC %md ## 3 · A governed metric view

# COMMAND ----------

# SOLUTION-BEGIN lab-02: Create the metric view orders_metrics (CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML) on orders_enriched with dimensions order_month, region, segment, category, channel and measures revenue, order_count and avg_order_value.
metric_yaml = f"""
version: 1.1
comment: Governed sales KPIs for dashboards, Genie Agents and apps
source: {catalog}.{schema}.orders_enriched
dimensions:
  - name: order_month
    expr: DATE_TRUNC('MONTH', order_date)
  - name: region
    expr: region
  - name: segment
    expr: segment
  - name: category
    expr: category
  - name: channel
    expr: channel
measures:
  - name: revenue
    expr: SUM(amount)
  - name: order_count
    expr: COUNT(DISTINCT order_id)
  - name: avg_order_value
    expr: SUM(amount) / COUNT(DISTINCT order_id)
"""
spark.sql(f"CREATE OR REPLACE VIEW orders_metrics WITH METRICS LANGUAGE YAML AS $${metric_yaml}$$")
# SOLUTION-END

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_month, region,
# MAGIC        MEASURE(revenue)         AS revenue,
# MAGIC        MEASURE(avg_order_value) AS aov
# MAGIC FROM orders_metrics
# MAGIC GROUP BY ALL
# MAGIC ORDER BY order_month, region;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Troubleshoot
# MAGIC Break it yourself: `REVOKE SELECT ON TABLE customer_profiles FROM bootcamp_analysts`.
# MAGIC Then use **Catalog Explorer → Permissions** and **Lineage** to find which downstream objects analysts can no
# MAGIC longer read (metric view, dashboard), and grant it back.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Domain & glossary (UI)
# MAGIC
# MAGIC **Add your schema to a `Sales` domain** — Catalog Explorer:
# MAGIC
# MAGIC 1. In the **Catalog** pane, right-click (or the ⋯ menu on) your schema `{catalog}.{schema}` and select **Add to domain**.
# MAGIC    *(Alternatively: open the schema, use the **Add to domain** button on the overview page.)*
# MAGIC 2. Search for or create the domain **Sales** (you can also create it first via **Catalog → Governance → Domains → Create domain**), then click **Confirm**.
# MAGIC 3. Verify: open the **Sales** domain page — your schema should now be listed under **Objects**, and the schema's **Domain** field shows Sales.
# MAGIC
# MAGIC **Create the glossary term `Revenue` and link it to the measure**:
# MAGIC
# MAGIC 1. In Catalog Explorer, go to **Governance → Glossary** (top tab) and click **Create term**.
# MAGIC 2. Set **Term** to `Revenue`, optionally add a short definition (e.g. `Total sales amount from orders (SUM(amount))`), then **Confirm**.
# MAGIC 3. Open the term and click **Assign to data assets** (or open the `orders_metrics` view → **Governance / Glossary** tab).
# MAGIC 4. Find your metric view `{catalog}.{schema}.orders_metrics` and, under it, select the **revenue** measure, then click **Confirm** to link it.
# MAGIC 5. Verify: open `orders_metrics` — its **Glossary** tab shows the **Revenue** term, and the term page lists the `revenue` measure as a linked asset.