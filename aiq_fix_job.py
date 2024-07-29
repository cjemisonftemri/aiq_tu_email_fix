# Databricks notebook source
# MAGIC %run /Workspace/Shared/util/storage_account_access 

# COMMAND ----------

import datetime
import pyspark.sql.functions as F
from pyspark.sql.types import *
from typing import List

now = datetime.datetime.utcnow()
date_str = now.strftime("%Y%m%d")

aiq_tu_input_path = analyticsiq_root + "AIQ_HEM_20240715/*.parquet"
aiq_tu_output_path = transunion_root + f"AIQ_HEM_TO_TU_{date_str}"

@F.udf(returnType=StringType())
def remove_too_emails(s: str) -> str:
    tmp = None
    if s:
        l = s.split(";")
        if len(l) > 10:
            tmp = ";".join(l[0:10])
        else:
            tmp = s
    return tmp

df = spark.read.parquet(aiq_tu_input_path)

tmp = df.withColumn("email_resize", remove_too_emails(F.col("Email Address Array")))
tmp = df.drop("Email Address Array")
tmp = df.withColumnRenamed("email_resize", "Email Address Array")
display(tmp)

tmp.write.parquet(aiq_tu_output_path)

