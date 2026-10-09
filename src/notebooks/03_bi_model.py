# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Lab 03 · A star schema for BI
# MAGIC Builds a small **star schema** on top of the silver/gold data (see the *Star schema* slide):
# MAGIC `fact_orders` in the middle, `dim_customer`, `dim_product` and `dim_date` around it — with
# MAGIC **primary and foreign key constraints** (informational, `RELY`) that BI tools, Genie and the query optimiser use.
# MAGIC
# MAGIC Prerequisites: lab 01 (pipeline ran) and lab 02 (metric view `orders_metrics`).

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Re-runnable: drop the fact first (its foreign keys reference the dimensions)
# MAGIC DROP TABLE IF EXISTS fact_orders;

# COMMAND ----------

# MAGIC %md ## 1 · Dimensions

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dim_customer
# MAGIC COMMENT 'Customer dimension (no PII — e-mail stays in customer_profiles)'
# MAGIC AS SELECT CAST(customer_id AS BIGINT) AS customer_id, first_name, region, segment, to_date(signup_date) AS signup_date
# MAGIC FROM customers;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dim_product
# MAGIC COMMENT 'Product dimension'
# MAGIC AS SELECT CAST(product_id AS BIGINT) AS product_id, name AS product_name, category, CAST(list_price AS DECIMAL(10, 2)) AS list_price
# MAGIC FROM products;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SOLUTION-BEGIN lab-03: Create dim_date with one row per day of 2026 (explode(sequence(...))) and columns date, year, quarter, month, month_name, week_of_year, day_of_week and is_weekend.
# MAGIC CREATE OR REPLACE TABLE dim_date
# MAGIC COMMENT 'Calendar dimension for 2026'
# MAGIC AS SELECT
# MAGIC   d                                   AS date,
# MAGIC   year(d)                             AS year,
# MAGIC   quarter(d)                          AS quarter,
# MAGIC   month(d)                            AS month,
# MAGIC   date_format(d, 'MMMM')              AS month_name,
# MAGIC   weekofyear(d)                       AS week_of_year,
# MAGIC   date_format(d, 'EEEE')              AS day_of_week,
# MAGIC   dayofweek(d) IN (1, 7)              AS is_weekend
# MAGIC FROM (SELECT explode(sequence(DATE'2026-01-01', DATE'2026-12-31', INTERVAL 1 DAY)) AS d);
# MAGIC -- SOLUTION-END

# COMMAND ----------

# MAGIC %md ## 2 · Fact table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SOLUTION-BEGIN lab-03: Create fact_orders from orders_silver with order_id, customer_id, product_id, order_date, channel, quantity, amount and coupon_code.
# MAGIC CREATE OR REPLACE TABLE fact_orders
# MAGIC COMMENT 'One row per order line — grain: order_id'
# MAGIC AS SELECT order_id, customer_id, product_id, order_date, channel, quantity, amount
# MAGIC --, coupon_code
# MAGIC FROM orders_silver;
# MAGIC -- SOLUTION-END

# COMMAND ----------

# MAGIC %md ## 3 · Keys: primary and foreign key constraints

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE dim_customer ALTER COLUMN customer_id SET NOT NULL;
# MAGIC ALTER TABLE dim_customer ADD CONSTRAINT dim_customer_pk PRIMARY KEY (customer_id) RELY;
# MAGIC ALTER TABLE dim_product  ALTER COLUMN product_id SET NOT NULL;
# MAGIC ALTER TABLE dim_product  ADD CONSTRAINT dim_product_pk PRIMARY KEY (product_id) RELY;
# MAGIC ALTER TABLE dim_date     ALTER COLUMN date SET NOT NULL;
# MAGIC ALTER TABLE dim_date     ADD CONSTRAINT dim_date_pk PRIMARY KEY (date) RELY;
# MAGIC ALTER TABLE fact_orders  ALTER COLUMN order_id SET NOT NULL;
# MAGIC ALTER TABLE fact_orders  ADD CONSTRAINT fact_orders_pk PRIMARY KEY (order_id) RELY;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SOLUTION-BEGIN lab-03: Add foreign keys from fact_orders to dim_customer (customer_id), dim_product (product_id) and dim_date (order_date).
# MAGIC ALTER TABLE fact_orders ADD CONSTRAINT fact_orders_customer_fk FOREIGN KEY (customer_id) REFERENCES dim_customer;
# MAGIC ALTER TABLE fact_orders ADD CONSTRAINT fact_orders_product_fk  FOREIGN KEY (product_id)  REFERENCES dim_product;
# MAGIC ALTER TABLE fact_orders ADD CONSTRAINT fact_orders_date_fk     FOREIGN KEY (order_date)  REFERENCES dim_date;
# MAGIC -- SOLUTION-END

# COMMAND ----------

# MAGIC %md
# MAGIC Open **Catalog → fact_orders → Overview**: the key icons and the *Entity relationship diagram* show the star.
# MAGIC Genie and AI/BI dashboards use these relationships to join correctly.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sanity check: revenue by quarter and segment, straight from the star schema
# MAGIC SELECT d.quarter, c.segment, round(sum(f.amount), 2) AS revenue, count(*) AS orders
# MAGIC FROM fact_orders f
# MAGIC JOIN dim_customer c USING (customer_id)
# MAGIC JOIN dim_date d ON f.order_date = d.date
# MAGIC GROUP BY ALL
# MAGIC ORDER BY d.quarter, revenue DESC;

# COMMAND ----------

