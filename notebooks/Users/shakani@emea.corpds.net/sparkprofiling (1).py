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
df = spark.read.csv("/mnt/landing/finaldatatest.csv", inferSchema="True",header = "true")

# COMMAND ----------

#excel_data=spark.read.format("com.crealytics.spark.excel").option("header","true").option("inferSchema","true").load("/mnt/landing/customer subscription.xlsx")

# COMMAND ----------

display(df)

# COMMAND ----------

#%scala

#val df = spark.read
#.format("com.crealytics.spark.excel")
#.option("header", true)
#.load("customer subscription.xlsx")

#display(df)

# COMMAND ----------

# python
#df = spark.read.csv("/mnt/landing", inferSchema="True",header = "true")

# COMMAND ----------

display(df)

# COMMAND ----------

import spark_df_profiling

# COMMAND ----------

#df = spark.table('lc_loan_dataa')
#display(df)

# COMMAND ----------

df.describe().show()

# COMMAND ----------

report = spark_df_profiling.ProfileReport(df)

# COMMAND ----------

report