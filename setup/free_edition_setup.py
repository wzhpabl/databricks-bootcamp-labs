# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Lab 00 · Free Edition setup (run once, ~2 minutes)
# MAGIC Creates the three environment catalogs and checks everything the labs need in **your** Free Edition workspace.
# MAGIC Run it on **Serverless** from your Git folder: `setup/free_edition_setup`.
# MAGIC
# MAGIC | Environment | Catalog | Who deploys | Workspace path |
# MAGIC |---|---|---|---|
# MAGIC | dev | `bootcamp_dev` (schema `dev_<you>_sales`) | you, from the workspace or CLI | `/Users/<you>/.bundle/…/dev` |
# MAGIC | staging | `bootcamp_staging` (schema `sales`) | GitHub Actions only | `/Users/<ci identity>/.bundle/…/staging` |
# MAGIC | prod | `bootcamp_prod` (schema `sales`) | GitHub Actions only, after approval | `/Users/<ci identity>/.bundle/…/prod` |

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
me = w.current_user.me()
print(f"Workspace URL : {w.config.host}")
print(f"You           : {me.user_name}")

# COMMAND ----------

# MAGIC %md ## 1 · Environment catalogs

# COMMAND ----------

results = {}
for cat in ["bootcamp_dev", "bootcamp_staging", "bootcamp_prod"]:
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{cat}` COMMENT 'Databricks bootcamp — {cat.split('_')[1]} environment'")
        results[cat] = "ok"
    except Exception as e:  # noqa: BLE001
        results[cat] = f"FAILED: {str(e).splitlines()[0][:160]}"
for k, v in results.items():
    print(f"{k:18} {v}")

if any(v != "ok" for v in results.values()):
    print("""
Could not create catalogs in this workspace.
→ Use the single-catalog fallback: in databricks.yml add  "config/*.yml"  to the include list.
  All environments will then live in the built-in 'workspace' catalog, isolated by schema
  (dev_<you>_sales / staging_sales / prod_sales).""")

# COMMAND ----------

# MAGIC %md ## 2 · Groups for the governance lab (created in the UI, checked here)

# COMMAND ----------

wanted = ["bootcamp_engineers", "bootcamp_analysts"]
existing = {g.display_name for g in w.groups.list(attributes="displayName")}
for g in wanted:
    print(f"{g:20} {'ok' if g in existing else 'MISSING → Settings → Identity and access → Groups → Add group'}")

# COMMAND ----------

# MAGIC %md ## 3 · SQL warehouse (used by dashboards, Genie and bundle lookups)

# COMMAND ----------

names = [wh.name for wh in w.warehouses.list()]
print("Warehouses:", names)
if "Serverless Starter Warehouse" not in names and names:
    print(f"→ Set variable warehouse_name to '{names[0]}' in databricks.yml")

# COMMAND ----------

# MAGIC %md ## 4 · Values for GitHub (lab 00, part 5)

# COMMAND ----------

print(f"""
GitHub → your repo → Settings → Secrets and variables → Actions

  Variables tab:  DATABRICKS_HOST   = {w.config.host}
  Secrets tab:    DATABRICKS_TOKEN  = <personal access token created in lab 00, part 4>

(Or, if you created a service principal in lab 00, part 4 (option B):
  DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET instead of DATABRICKS_TOKEN.)
""")