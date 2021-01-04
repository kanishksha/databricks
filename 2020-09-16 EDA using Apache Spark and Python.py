# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Exploratory Data Analysis(EDA)
# MAGIC -Data Analysis
# MAGIC 
# MAGIC -Exploratory Data Analysis
# MAGIC 
# MAGIC -Converting Spark to Pandas
# MAGIC 
# MAGIC -Charts and plots like bar plot, scatter plot, Maps etc.
# MAGIC 
# MAGIC -seaborn and matplotlib for Python

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.help("dropdown")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET DROPDOWN home_ownership DEFAULT "RENT" CHOICES SELECT DISTINCT home_ownership FROM lc_loan_dataa

# COMMAND ----------

# DBTITLE 1,Lending loan Dataset
# MAGIC 
# MAGIC %sql
# MAGIC 
# MAGIC select * from lc_loan_dataa

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,converting table to spark dataframe
lc_df = spark.table('lc_loan_dataa')
display(lc_df)

# COMMAND ----------

lc_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Descriptive statistics

display(lc_df.describe())

# COMMAND ----------

# DBTITLE 1,Each State with sum Loan Amount
# MAGIC %sql
# MAGIC 
# MAGIC select addr_state, sum(loan_amnt) from lc_loan_dataa group by addr_state

# COMMAND ----------

# DBTITLE 1,count of annual income in each state
from pyspark.sql.functions import isnan, when, count, col, log
display(lc_df.groupBy("addr_state").agg((count(col("annual_inc"))).alias("count")))

# COMMAND ----------

# DBTITLE 1,maximum number of bad loan w.r.t state
# MAGIC %sql
# MAGIC 
# MAGIC select addr_state, count(loan_amnt) from lc_loan_dataa where bad_loan='yes' group by addr_state

# COMMAND ----------

display(lc_df)

# COMMAND ----------

# DBTITLE 1,Loan grade analysis
# MAGIC %sql
# MAGIC 
# MAGIC select grade, sum(loan_amnt) as tot_loan_amnt from lc_loan_dataa group by grade

# COMMAND ----------

# DBTITLE 1,categorizing grade and bad_loan doing sum of loan amount
# MAGIC %sql
# MAGIC select grade, bad_loan, sum(loan_amnt) as tot_loan_amnt from lc_loan_dataa group by grade, bad_loan

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select grade, bad_loan, sum(loan_amnt) as tot_loan_amnt from lc_loan_dataa group by grade, bad_loan

# COMMAND ----------

# DBTITLE 1,storing column exposure in df and checking the bad loans
from pyspark.sql.functions import isnan, when, count, col, log
lc_df = lc_df.withColumn("exposure",when(lc_df.bad_loan=="No",col("revol_bal")).otherwise(-10*col("revol_bal")))
display(lc_df)

# COMMAND ----------

# DBTITLE 1,Bad loan w.r.t grades
display(lc_df.groupBy("bad_loan","grade").agg({"exposure": "sum"}))

# COMMAND ----------

# DBTITLE 1,checking null value across the column
from pyspark.sql.functions import isnan, when, count, col
lc_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in lc_df.columns]).show()

# COMMAND ----------

display(lc_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in lc_df.columns]))

# COMMAND ----------

# DBTITLE 1,Loan amount
# MAGIC %sql
# MAGIC 
# MAGIC select loan_amnt from lc_loan_dataa

# COMMAND ----------

# DBTITLE 1,bad loan distribution
# MAGIC %sql
# MAGIC 
# MAGIC select loan_amnt, bad_loan from lc_loan_dataa

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select loan_amnt from lc_loan_dataa where bad_loan="Yes" 

# COMMAND ----------

# DBTITLE 1,trimming the percentageand registering this particular function in spark udf
from pyspark.sql.types import FloatType

def trim(string):
  
  return string.strip('%')

spark.udf.register("trimperct", trim)

# COMMAND ----------

# DBTITLE 1,calling function above and passing it in for, int_rate to convert it as float value
# MAGIC %sql
# MAGIC 
# MAGIC select int_rate, cast(trimperct(int_rate) as float) as int_rate_float from lc_loan_dataa

# COMMAND ----------

# DBTITLE 1,interest rate when loan is bad
# MAGIC %sql
# MAGIC 
# MAGIC select bad_loan, cast(trimperct(int_rate) as float) as int_rate_float from lc_loan_dataa

# COMMAND ----------

# DBTITLE 1,installments customer are paying
# MAGIC %sql
# MAGIC 
# MAGIC select bad_loan, installment from lc_loan_dataa

# COMMAND ----------

# DBTITLE 1,particular customer owns a home or not and what is bad loan and loan amount
# MAGIC %sql
# MAGIC 
# MAGIC select home_ownership, bad_loan, loan_amnt from lc_loan_dataa

# COMMAND ----------

# DBTITLE 1,customer purpose of loan
# MAGIC %sql
# MAGIC 
# MAGIC select grade, purpose, count(*) as count from lc_loan_dataa group by grade, purpose 

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select loan_amnt, home_ownership, purpose from lc_loan_dataa where home_ownership = getArgument("home_ownership")

# COMMAND ----------

# DBTITLE 1,customer loan _status with grades
display(lc_df.groupBy('grade','loan_status').agg({'loan_status':'count'}))

# COMMAND ----------

# DBTITLE 1,count of bad loans in data
# MAGIC %sql
# MAGIC 
# MAGIC select bad_loan, count(*) from lc_loan_dataa group by bad_loan 

# COMMAND ----------

# DBTITLE 1,installments loan_amnt and bad_loan distribution
# MAGIC %sql
# MAGIC 
# MAGIC select installment, loan_amnt, bad_loan from lc_loan_dataa

# COMMAND ----------

# DBTITLE 1,correlation test between installment and loan amount
lc_df.stat.corr('installment','loan_amnt') 