# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC 
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/WA_Fn_UseC__Telco_Customer_Churn.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option('nanValue', '') \
  .option('nullValue', '') \
  .load(file_location)

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import isnan, when, count, col
df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# COMMAND ----------

# Create a view or table

temp_table_name = "churn_analysis"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

pd_df=df.toPandas()

# COMMAND ----------

import matplotlib.pyplot as plt
plt.clf()
plt.plot(pd_df['tenure'],pd_df['TotalCharges'],'.')
plt.xlabel('tenure')
plt.ylabel('totalcharges')
display()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from churn_analysis

# COMMAND ----------

df.groupBy('churn').count().show()

# COMMAND ----------

df.select('tenure','TotalCharges','MonthlyCharges').describe().show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select gender, churn, count(*) from churn_analysis group by gender, churn

# COMMAND ----------

# MAGIC %sql
# MAGIC select SeniorCitizen, churn, count(*) from churn_analysis group by SeniorCitizen, churn

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT tenure, churn ,count(churn) AS count FROM churn_analysis GROUP BY tenure, churn ORDER BY  tenure

# COMMAND ----------

df.stat.crosstab("SeniorCitizen","InternetService").show()

# COMMAND ----------

df.stat.freqItems(["PhoneService", "MultipleLines", "InternetService", "OnlineSecurity", "onlineBackup", "DeviceProtection", "TechSupport", "StreamingTV", "StreamingMovies"], 0.6).collect()

# COMMAND ----------

# MAGIC %sql
# MAGIC select PaperLessBilling, churn, count(*) from churn_analysis group by PaperlessBilling , churn

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select PaymentMethod, churn, count(*) from churn_analysis group by PaymentMethod, churn 

# COMMAND ----------

churn_df = df
(train_data, test_data) = churn_df.randomSplit([0.7, 0.3], 24)

print("Records for training: " + str(train_data.count()))
print("Records for evaluation: " + str(test_data.count()))

# COMMAND ----------


