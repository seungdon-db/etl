# Databricks notebook source
from pyspark.sql.types import * 
import json 

#jschema = '{}'
#schema = StructType.fromJson(json.loads(jschema)) 

# COMMAND ----------

cloudfile = {
  "cloudFiles.format":"csv",
  "cloudFiles.useNotifications":"true"
 
}

# COMMAND ----------

df = (spark.readStream.format("cloudFiles")
      .options(**cloudfile)
      .schema(schema)
      .load("##filelocation##"))

# COMMAND ----------

streamQuery=(df.writeStream
             .format("delta")
             .trigger(once=True)
             .outputMode("append")
             .option("checkpointLocation","/mnt./.../_checkpoint")
             .start("##targetLocation##"))


# COMMAND ----------

streamQuery.awaitTermination()
streamQuery.recentProgress
