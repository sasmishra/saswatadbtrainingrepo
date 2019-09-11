// Databricks notebook source
// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS RawOrders
// MAGIC USING CSV
// MAGIC LOCATION "wasbs://adfdata@iomegastoragev2.blob.core.windows.net/*.csv"
// MAGIC OPTIONS ( header "true" )

// COMMAND ----------

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.config.Config
import org.apache.spark.sql.SaveMode

val orders = spark.sql("SELECT * FROM RawOrders")

val writeConfig = Config(Map(
  "Endpoint" -> "https://iomegacosmosv2.documents.azure.com:443/",
  "Masterkey" -> "0tO9mDWRjkVUryDb5Z1tqPKw65G4c2HeBc851ewXX0U5gzdpx1r9VocadareUt604cJC8EkvR8BoHHawsYRfXQ==",
  "Database" -> "ordersystemdb",
  "Collection" -> "customers",
  "Upsert" -> "true"
))

orders.write.mode(SaveMode.Overwrite).cosmosDB(writeConfig)