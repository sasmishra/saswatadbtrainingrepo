// Databricks notebook source
val appId = "911c604b-b0aa-4e51-a491-5cb0f71fe221"
val tenantId = "381a10df-8e85-43db-86e1-8893b075b027"
val secret = "0ACf-]qp*b3gezW5u@CJ42lMEJRyxc36"
val cdmModel = "ordersystem"
val rawOrders = spark.sql("SELECT * FROM RawOrders")
val outputLocation = "https://iomegadls.dfs.core.windows.net/data/raw-orders"

// COMMAND ----------

rawOrders
  .write
  .format("com.microsoft.cdm")
  .option("entity", "raworders")
  .option("appId", appId)
  .option("appKey", secret)
  .option("tenantId", tenantId)
  .option("cdmFolder", outputLocation)
  .option("cdmModelName", cdmModel)
  .save()

// COMMAND ----------

case class RawOrder(orderid: String, orderdate: String, customer: String, 
                    product: String, billingaddress: String, units: String, remarks: String)

val df = spark
          .read
          .format("com.microsoft.cdm")
          .option("cdmModel", "https://iomegadls.dfs.core.windows.net/data/raw-orders/model.json")
          .option("entity", "raworders")
          .option("appId", appId)
          .option("appKey", secret)
          .option("tenantId", tenantId)
          .load()
          .as[RawOrder]

// COMMAND ----------

df.createOrReplaceTempView("RawOrdersv2")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM RawOrdersv2
// MAGIC WHERE billingaddress in ('Bangalore', 'Mysore', 'Hyderabad')