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

spark.conf.set(
"fs.azure.account.key.landingautospark.blob.core.windows.net","sVbbV+WuwHHLXRmlegxv8Ke8eEKcJusbFbJGFyx7vrkAy3XWMxxTX/MyCbxt4iarqjsJDtvvf6VgBuDTh0c7pA==")


# COMMAND ----------

infer_schema = "true"
first_row_is_header = "true"
delimiter = ","




df = (spark.read.format("csv")
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
 

     .load("wasbs://landingfiles@landingautospark.blob.core.windows.net/"))
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

lc_df = spark.table('LoanStats')
display(lc_df)

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
# MAGIC select loan_status, count(*) from LoanStats group by loan_status order by 2 desc 

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
# MAGIC select application_type, dti, dti_joint from loanstats where dti is null

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

permanent_table_name = "lc_loan_data3"
df_sel.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from lc_loan_data3

# COMMAND ----------

df_sel.filter(df_sel.dti_cleaned > 100).show()