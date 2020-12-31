# Databricks notebook source
dbutils.fs.unmount("/mnt/landing")

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://landing@autosparkk.blob.core.windows.net",
  mount_point = "/mnt/landing",
  extra_configs = {"fs.azure.account.key.autosparkk.blob.core.windows.net":dbutils.secrets.get(scope = "dbscope", key = "keyfinal")})



# COMMAND ----------

display(dbutils.fs.ls("/mnt/landing"))

# COMMAND ----------

# python
df = spark.read.csv("/mnt/landing/ll2.csv", inferSchema="True",header = "true")

# COMMAND ----------

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

# selecting specific columns those are important and affects the data(selected dataframes)

df_sel = df.select("term", "home_ownership", "installment", "grade", "purpose", "int_rate", "addr_state", "loan_status", "application_type", "loan_amnt", "emp_length", "annual_inc", "dti", "delinq_2yrs","revol_bal", "revol_util", "total_acc", "num_tl_90g_dpd_24m", "dti_joint")


# COMMAND ----------

df_sel.describe("term", "loan_amnt", "emp_length", "annual_inc", "dti", "delinq_2yrs","revol_bal", "revol_util", "total_acc").show()

# COMMAND ----------

df_sel.cache()

# COMMAND ----------

import spark_df_profiling

# COMMAND ----------

report = spark_df_profiling.ProfileReport(df_sel)

# COMMAND ----------

report