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

# MAGIC %sql
# MAGIC 
# MAGIC select * from lc_loan_data2

# COMMAND ----------

lc_df = spark.table('lc_loan_data2')
display(lc_df)

# COMMAND ----------

lc_df.printSchema()

# COMMAND ----------

display(lc_df.describe())

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select addr_state, sum(loan_amnt) from lc_loan_data2 group by addr_state

# COMMAND ----------

from pyspark.sql.functions import isnan, when, count, col, log
display(lc_df.groupBy("addr_state").agg((count(col("annual_inc"))).alias("count")))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select addr_state, count(loan_amnt) from lc_loan_data where bad_loan='yes' group by addr_state

# COMMAND ----------

display(lc_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select grade, sum(loan_amnt) as tot_loan_amnt from lc_loan_data2 group by grade

# COMMAND ----------

# MAGIC %sql
# MAGIC select grade, bad_loan, sum(loan_amnt) as tot_loan_amnt from lc_loan_data2 group by grade, bad_loan

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select grade, bad_loan, sum(loan_amnt) as tot_loan_amnt from lc_loan_data2 group by grade, bad_loan

# COMMAND ----------

from pyspark.sql.functions import isnan, when, count, col, log
lc_df = lc_df.withColumn("exposure",when(lc_df.bad_loan=="No",col("revol_bal")).otherwise(-10*col("revol_bal")))
display(lc_df)

# COMMAND ----------

display(lc_df.groupBy("bad_loan","grade").agg({"exposure": "sum"}))

# COMMAND ----------

from pyspark.sql.functions import isnan, when, count, col
lc_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in lc_df.columns]).show()

# COMMAND ----------

display(lc_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in lc_df.columns]))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select loan_amnt from lc_loan_data2

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select loan_amnt, bad_loan from lc_loan_data2

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select loan_amnt from lc_loan_data2 where bad_loan="Yes" 

# COMMAND ----------

from pyspark.sql.types import FloatType

def trim(string):
  
  return string.strip('%')

spark.udf.register("trimperct", trim)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select int_rate, cast(trimperct(int_rate) as float) as int_rate_float from lc_loan_data2

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select bad_loan, cast(trimperct(int_rate) as float) as int_rate_float from lc_loan_data2

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select home_ownership, bad_loan, loan_amnt from lc_loan_data2

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select grade, purpose, count(*) as count from lc_loan_data2 group by grade, purpose

# COMMAND ----------

display(lc_df.groupBy('grade','loan_status').agg({'loan_status':'count'}))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select bad_loan, count(*) from lc_loan_data2 group by bad_loan 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select installment, loan_amnt, bad_loan from lc_loan_data2

# COMMAND ----------

lc_df.stat.corr('installment','loan_amnt') 

# COMMAND ----------

pd_df=lc_df.toPandas()

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
plt.clf()
sns.distplot(pd_df.loc[pd_df['dti'].notnull() & (pd_df['dti']<50), 'dti'])
plt.xlabel('dti')
plt.ylabel('count')
display()

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.enabled","true")

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
plt.clf()
sns.countplot(pd_df.loc[pd_df['total_acc']<120,'total_acc'],order=sorted(pd_df['total_acc'].unique()), saturation=1) 
_, _ = plt.xticks(np.arange(0, 120, 10), np.arange(0, 120, 10))
plt.xlabel('total_acc')
plt.ylabel('count')
display()

# COMMAND ----------

