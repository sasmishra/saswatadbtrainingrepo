// Databricks notebook source
import java.sql.Date
import java.text._
import spark.implicits

// COMMAND ----------

case class CustomerUpdate(customerId: Int, address: String, effectiveDate: Date)
case class Customer(customerId: Int, address: String, current: Boolean, effectiveDate: Date, endDate: Date)

implicit def date(str: String): Date = Date.valueOf(str)

// COMMAND ----------

sql("drop table if exists customers")

// COMMAND ----------

val data = Seq(
  Customer(1, "old address for 1", false, null, "2018-02-01"),
  Customer(1, "current address for 1", true, "2018-02-01", null),
  Customer(2, "current address for 2", true, "2018-02-01", null),
  Customer(3, "current address for 3", true, "2018-02-01", null)
)

data
  .toDF()
  .write
  .format("delta")
  .mode("overwrite")
  .saveAsTable("customers")

// COMMAND ----------

display(table("customers").orderBy("customerId"))

// COMMAND ----------

val updates = Seq(
  CustomerUpdate(1, "new address for 1", "2018-03-03"),
  CustomerUpdate(3, "current address for 3", "2018-04-04"),    // new address same as current address for customer 3
  CustomerUpdate(4, "new address for 4", "2018-04-04")
)

updates
  .toDF()
  .createOrReplaceTempView("updates")

// COMMAND ----------

display(table("updates"))

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC MERGE INTO customers
// MAGIC USING (
// MAGIC   SELECT updates.customerId as mergeKey, updates.*
// MAGIC   FROM updates
// MAGIC   
// MAGIC   UNION ALL
// MAGIC   
// MAGIC   SELECT NULL as mergeKey, updates.*
// MAGIC   FROM updates JOIN customers
// MAGIC   ON updates.customerid = customers.customerid 
// MAGIC   WHERE customers.current = true AND updates.address <> customers.address 
// MAGIC   
// MAGIC ) staged_updates ON customers.customerId = mergeKey
// MAGIC WHEN MATCHED AND customers.current = true AND customers.address <> staged_updates.address THEN  
// MAGIC   UPDATE SET current = false, endDate = staged_updates.effectiveDate    
// MAGIC WHEN NOT MATCHED THEN 
// MAGIC   INSERT(customerid, address, current, effectivedate, enddate) 
// MAGIC   VALUES(staged_updates.customerId, staged_updates.address, true, staged_updates.effectiveDate, null) 

// COMMAND ----------

display(table("customers").orderBy("customerId", "current", "endDate"))