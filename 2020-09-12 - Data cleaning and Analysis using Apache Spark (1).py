# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Analyzing and Cleaning large Datasets Using Apache Spark
# MAGIC 
# MAGIC -Data Analysis
# MAGIC 
# MAGIC -Data Cleaning
# MAGIC 
# MAGIC -Spark dataframes funcions
# MAGIC 
# MAGIC -Spark SQL
# MAGIC 
# MAGIC -Descriptive statistics

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/LoanStats_2018Q4-1.csv"
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
  .load(file_location)

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Create a view or table

temp_table_name = "LoanStats"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC select * from `LoanStats`

# COMMAND ----------

df.describe().show()

# COMMAND ----------

# selecting specific columns those are important and affects the data(selected dataframes)

df_sel = df.select("term", "home_ownership", "installment", "grade", "purpose", "int_rate", "addr_state", "loan_status", "application_type", "loan_amnt", "emp_length", "annual_inc", "dti", "delinq_2yrs","revol_bal", "revol_util", "total_acc", "num_tl_90g_dpd_24m", "dti_joint")

# COMMAND ----------

df_sel.describe("term", "loan_amnt", "emp_length", "annual_inc", "dti", "delinq_2yrs","revol_bal", "revol_util", "total_acc").show()

# COMMAND ----------

df_sel.cache()

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct emp_length from LoanStats limit 50