// Databricks notebook source
// MAGIC %md 
// MAGIC # Learning to load data from DLS to ADB
// MAGIC # Processing the data

// COMMAND ----------

import spark.implicits._

case class Customer (customerid: Int, fullname: String, address: String, credit: Int, status: Boolean, remarks: String)

val customersCsvLocation = "/mnt/data/customers/*.csv"
val customers = 
  spark
    .read
    .option("inferSchema", true)
    .option("header", true)
    .option("sep", ",")
    .csv(customersCsvLocation)
    .as[Customer]

// COMMAND ----------

customers.createOrReplaceTempView("customers")

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC SELECT address AS CustomerLocation, COUNT(*) AS NoOfCustomers FROM customers
// MAGIC GROUP BY address
// MAGIC ORDER BY address

// COMMAND ----------



// COMMAND ----------

val getCustomerType = (credit: Int) => {
  if(credit < 10000) "Silver"
  else if(credit >= 10000 && credit < 25000) "Gold"
  else "Platinum"
}

spark.udf.register("getCustomerType", getCustomerType)

// COMMAND ----------

// MAGIC %sql 
// MAGIC SELECT getCustomerType(credit) AS CustomerType, COUNT(*) AS NoOfCustomers
// MAGIC FROM customers
// MAGIC GROUP BY CustomerType
// MAGIC ORDER BY CustomerType