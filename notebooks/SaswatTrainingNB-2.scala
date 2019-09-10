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

// COMMAND ----------

case class Product(productid: BigInt, title: String, unitsinstock: BigInt, unitprice: BigInt, itemdiscount: BigInt)

val products = 
  spark
    .read
    .option("multiline", true)
    .json("/mnt/data/products/*.json")
    .as[Product]

// COMMAND ----------

display(products)

// COMMAND ----------

val getOrderAmount = (units: Int, unitPrice: Int, itemdiscount: Int) => {
  val total = (units * unitPrice)
  val discount = ((total * itemdiscount) / 100).asInstanceOf[Int]
  (total - discount).asInstanceOf[Int]
}
spark.udf.register("getOrderAmount", getOrderAmount)

// COMMAND ----------

case class Order(orderid: Int, orderdate: String, customer: Int, product: Int, billingaddress: String, units: Int, remarks: String)

val orders = 
  spark
    .read
    .option("inferSchema", true)
    .option("header", true)
    .option("sep", ",")
    .csv("/mnt/data/orders/*.csv")
    .as[Order]

// COMMAND ----------

orders.printSchema

// COMMAND ----------

customers.createOrReplaceTempView("customers")
products.createOrReplaceTempView("products")
orders.createOrReplaceTempView("orders")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT o.orderid AS OrderId, o.orderdate AS OrderDate, c.fullname AS CustomerName, p.title AS ProductTitle,
// MAGIC   c.address AS CustomerLocation, getCustomerType(c.credit) AS CustomerType,
// MAGIC   getOrderAmount(o.units, p.unitprice, p.itemdiscount) AS OrderAmount,
// MAGIC   p.unitprice AS UnitPrice, p.itemdiscount AS ItemDiscount,
// MAGIC   o.billingaddress AS BillingAddress, o.remarks AS OrderRemarks
// MAGIC FROM orders o
// MAGIC INNER JOIN customers c ON c.customerid = o.customer
// MAGIC INNER JOIN products p ON p.productid = o.product
// MAGIC WHERE o.billingaddress IN ( 'Bangalore', 'Trivandrum', 'Hyderabad', 'Mumbai', 'Chennai', 'New Delhi')
// MAGIC ORDER BY OrderAmount

// COMMAND ----------

val sqlStatement = 
"""SELECT o.orderid AS OrderId, o.orderdate AS OrderDate, c.fullname AS CustomerName, p.title AS ProductTitle,
  c.address AS CustomerLocation, getCustomerType(c.credit) AS CustomerType,
  getOrderAmount(o.units, p.unitprice, p.itemdiscount) AS OrderAmount,
  p.unitprice AS UnitPrice, p.itemdiscount AS ItemDiscount,
  o.billingaddress AS BillingAddress, o.remarks AS OrderRemarks
FROM orders o
INNER JOIN customers c ON c.customerid = o.customer
INNER JOIN products p ON p.productid = o.product
WHERE o.billingaddress IN ( 'Bangalore', 'Trivandrum', 'Hyderabad', 'Mumbai', 'Chennai', 'New Delhi')
ORDER BY OrderAmount"""

val processedOrders = spark.sql(sqlStatement)

processedOrders
  .write
  .parquet("/mnt/data/optimized-processed-orders/10092019")

// COMMAND ----------

val parquetProcessedOrders = 
  spark
   .read
    .parquet("/mnt/data/optimized-processed-orders/10092019")

// COMMAND ----------

display(parquetProcessedOrders)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE ProcessedOrders
// MAGIC USING PARQUET
// MAGIC LOCATION "/mnt/data/optimized-processed-orders/10092019"

// COMMAND ----------

// MAGIC  %sql
// MAGIC 
// MAGIC  SELECT CustomerLocation, SUM(OrderAmount) AS TotalOrderAmount
// MAGIC  FROM ProcessedOrders
// MAGIC  GROUP BY CustomerLocation
// MAGIC  ORDER BY CustomerLocation