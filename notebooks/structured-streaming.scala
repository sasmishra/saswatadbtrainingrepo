// Databricks notebook source
import org.apache.spark.eventhubs._
import org.apache.spark.sql.functions._

var eventHubUrl = "Endpoint=sb://iomegaeventhubs.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=tx8T4nhr1QAr6F+q9onHlXYSqACrhCX83cxBqzUqgEA="
val connectionString = ConnectionStringBuilder(eventHubUrl).setEventHubName("messages").build
val eventHubsConf = EventHubsConf(connectionString).setStartingPosition(EventPosition.fromEndOfStream)


// COMMAND ----------

val eventhubs = 
  spark
    .readStream
    .format("eventhubs")
    .options(eventHubsConf.toMap)
    .load()

// COMMAND ----------

eventhubs.printSchema

// COMMAND ----------

var query = 
  eventhubs
   .select(
     get_json_object(($"body").cast("string"), "$.zip").alias("zip"),    
     get_json_object(($"body").cast("string"), "$.hittime").alias("hittime"), 
     date_format(get_json_object(($"body").cast("string"), "$.hittime"), "dd.MM.yyyy").alias("day"))

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger.ProcessingTime

// COMMAND ----------

dbutils.fs.mkdirs("/data")

// COMMAND ----------

val listener =
  query
    .writeStream
    .format("parquet")
    .option("path", "/data/processed")
    .option("checkpointLocation", "/data/checkpoints")
    .partitionBy("zip", "day")
    .trigger(ProcessingTime("25 seconds"))
    .start()

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC DROP TABLE IF EXISTS ProcessedRecords;
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS ProcessedRecords
// MAGIC     (zip string, hittime string, day string)
// MAGIC     STORED AS PARQUET
// MAGIC     LOCATION "/data/processed"

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT zip, count(*) 
// MAGIC FROM ProcessedRecords
// MAGIC GROUP BY zip