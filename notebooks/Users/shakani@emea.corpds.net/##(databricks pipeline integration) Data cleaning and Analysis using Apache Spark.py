# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Analyzing and Cleaning large Datasets Using Apache Spark
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

# MAGIC 
# MAGIC %sql
# MAGIC 
# MAGIC select * from lc_loan_dt

# COMMAND ----------

df_sel = spark.table('lc_loan_dt')
display(df_sel)

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct emp_length from lc_loan_dt limit 50

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, regexp_extract
from pyspark.sql.functions import col

regex_string='years|year|\\+|\\<'
df_sel.select(regexp_replace(col("emp_length"), regex_string, "").alias("emplength_cleaned"),col("emp_length")).show(10)

# COMMAND ----------

df_sel=df_sel.withColumn("term_cleaned",regexp_replace(col("term"), "months", "")).withColumn("emplen_cleaned",regexp_extract(col("emp_length"),"\\d+", 0))

# COMMAND ----------

df_sel.select('term','term_cleaned','emp_length','emplen_cleaned').show(15)

# COMMAND ----------

df_sel.printSchema()

# COMMAND ----------

table_name="loanstatus_sel"

df_sel.createOrReplaceTempView(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from loanstatus_sel

# COMMAND ----------

#Covariance & Correlation

df_sel.stat.cov('annual_inc', 'loan_amnt')

# COMMAND ----------

df_sel.stat.corr('annual_inc', 'loan_amnt')

# COMMAND ----------

#Crosstab

df_sel.stat.crosstab('loan_status','grade').show()

# COMMAND ----------

#frequency

freq=df_sel.stat.freqItems(['purpose','grade'],0.3)

# COMMAND ----------

freq.collect()

# COMMAND ----------

df_sel.groupby('purpose').count().show()

# COMMAND ----------

df_sel.groupby('purpose').count().orderBy(col('count').desc()).show()

# COMMAND ----------

quantileProbs = [0.25, 0.5, 0.75, 0.9]
relError = 0.05
df_sel.stat.approxQuantile("loan_amnt", quantileProbs, relError)


# COMMAND ----------

quantileProbs = [0.25, 0.5, 0.75, 0.9]
relError = 0
df_sel.stat.approxQuantile("loan_amnt", quantileProbs, relError)

# COMMAND ----------

#***************************************************************
from pyspark.sql.functions import isnan, when, count, col
df_sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_sel.columns]).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select loan_status, count(*) from loanstatus_sel group by loan_status order by 2 desc 

# COMMAND ----------

df_sel=df_sel.na.drop("all", subset=["loan_status"])

# COMMAND ----------

df_sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_sel.columns]).show()

# COMMAND ----------

df_sel.count()

# COMMAND ----------

df_sel.describe("dti","revol_util").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select ceil(REGEXP_REPLACE(revol_util,"\%","")), count(*) from loanstatus_sel group by ceil(REGEXP_REPLACE(revol_util,"\%",""))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from loanstatus_sel where revol_util is null

# COMMAND ----------

df_sel=df_sel.withColumn("revolutil_cleaned", regexp_extract(col("revol_util"), "\\d+", 0))

# COMMAND ----------

df_sel.describe('revol_util','revolutil_cleaned').show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
def fill_avg(df, colname):
  return df.select(colname).agg(avg(colname))

# COMMAND ----------

rev_avg=fill_avg(df_sel,'revolutil_cleaned')

# COMMAND ----------

from pyspark.sql.functions import lit

rev_avg=fill_avg(df_sel,'revolutil_cleaned').first()[0]
df_sel=df_sel.withColumn('rev_avg',lit(rev_avg))

# COMMAND ----------

from pyspark.sql.functions import coalesce
df_sel=df_sel.withColumn('revolutil_cleaned',coalesce(col('revolutil_cleaned'),col('rev_avg')))

# COMMAND ----------

df_sel.describe('revol_util','revolutil_cleaned').show()

# COMMAND ----------

df_sel=df_sel.withColumn("revolutil_cleaned",df_sel["revolutil_cleaned"].cast("double"))

# COMMAND ----------

df_sel.describe('revol_util','revolutil_cleaned').show()

# COMMAND ----------

df_sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_sel.columns]).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from loanstatus_sel where dti is null

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select application_type, dti, dti_joint from loanstatus_sel where dti is null

# COMMAND ----------

df_sel=df_sel.withColumn("dti_cleaned",coalesce(col("dti"),col("dti_joint")))

# COMMAND ----------

df_sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_sel.columns]).show()

# COMMAND ----------

df_sel.groupby('loan_status').count().show()

# COMMAND ----------

df_sel.where(df_sel.loan_status.isin(["Late (31-120 days)", "Charged Off", "In Grace Period", "Late(16-30 days)"])).show()

# COMMAND ----------

df_sel=df_sel.withColumn("bad_loan", when(df_sel.loan_status.isin(["Late (31-120 days)", "Charged Off", "In Grace Period", "Late (16-30 days)"]), 'yes').otherwise('No'))

# COMMAND ----------

df_sel.groupBy('bad_loan').count().show()

# COMMAND ----------

df_sel.filter(df_sel.bad_loan == 'yes').show()

# COMMAND ----------

df_sel.printSchema()

# COMMAND ----------

df_sel_final=df_sel.drop('revol_util','dti','dti_joint')

# COMMAND ----------

df_sel_final.printSchema()

# COMMAND ----------

df_sel.stat.crosstab('bad_loan','grade').show()

# COMMAND ----------

df_sel.describe('dti_cleaned').show()

# COMMAND ----------

permanent_table_name = "lc_loan_datafinal"
df_sel.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from lc_loan_data3

# COMMAND ----------

df_sel.filter(df_sel.dti_cleaned > 100).show()

# COMMAND ----------

dbutils.fs.unmount("/mnt/landing")

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://landing@autosparkk.blob.core.windows.net",
  mount_point = "/mnt/landing",
  extra_configs = {"fs.azure.account.key.autosparkk.blob.core.windows.net":dbutils.secrets.get(scope = "dbscope", key = "keyfinal")})



# COMMAND ----------

#data = df_sel

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // Write the file back to Azure Blob Storage
# MAGIC val df = df_sel
# MAGIC //.option("header","true")
# MAGIC //.option("inferSchema", "true")
# MAGIC //.csv("/mnt/landing/")
# MAGIC 
# MAGIC spark.conf.set("fs.azure.account.key.autosparkk.blob.core.windows.net",dbutils.secrets.get(scope = "dbscope", key = "keyfinal"))
# MAGIC 
# MAGIC // Save to the source container
# MAGIC df.write.mode(SaveMode.Append).json("wasbs://landing@autosparkk.blob.core.windows.net/source/")
# MAGIC 
# MAGIC // Display the output in a table
# MAGIC display(df)

# COMMAND ----------

