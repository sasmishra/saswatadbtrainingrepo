// Databricks notebook source
val storage_account_name = "iomegastoragev2"
val storage_account_access_key = "22le5kRkFF0iaJjBePjSIlqCRY6Kz7LCr3DXaONm9smXFUoTtfVTJXzor7Dq/TSpfNHDIa2MSwQzDqjNenMPJg=="

spark.conf.set( "fs.azure.account.key." + storage_account_name + ".blob.core.windows.net", storage_account_access_key) 

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE DATABASE IF NOT EXISTS OrdersDB;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS OrdersDB.RawOrders;
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS OrdersDB.RawOrders
// MAGIC   USING CSV
// MAGIC   LOCATION "wasbs://data@iomegastoragev2.blob.core.windows.net/orders/*.csv"
// MAGIC   OPTIONS ("header" "true")

// COMMAND ----------

import java.io._
import java.net._
import java.util._

case class Language(documents: Array[LanguageDocuments], errors: Array[Any]) extends Serializable
case class LanguageDocuments(id: String, detectedLanguages: Array[DetectedLanguages]) extends Serializable
case class DetectedLanguages(name: String, iso6391Name: String, score: Double) extends Serializable

case class Sentiment(documents: Array[SentimentDocuments], errors: Array[Any]) extends Serializable
case class SentimentDocuments(id: String, score: Double) extends Serializable

case class RequestToTextApi(documents: Array[RequestToTextApiDocument]) extends Serializable
case class RequestToTextApiDocument(id: String, text: String, var language: String = "") extends Serializable

import javax.net.ssl.HttpsURLConnection
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import scala.util.parsing.json._

object SentimentDetector extends Serializable {
    val accessKey = "5a8e9f10bf1a4d3f8543a4d5a38e4fe2"
    val host = "https://iomegatextanalytics.cognitiveservices.azure.com/"
    val languagesPath = "/text/analytics/v2.1/languages"
    val sentimentPath = "/text/analytics/v2.1/sentiment"
    val languagesUrl = new URL(host+languagesPath)
    val sentimenUrl = new URL(host+sentimentPath)
    val g = new Gson

    def getConnection(path: URL): HttpsURLConnection = {
        val connection = path.openConnection().asInstanceOf[HttpsURLConnection]
        connection.setRequestMethod("POST")
        connection.setRequestProperty("Content-Type", "text/json")
        connection.setRequestProperty("Ocp-Apim-Subscription-Key", accessKey)
        connection.setDoOutput(true)
        return connection
    }

    def prettify (json_text: String): String = {
        val parser = new JsonParser()
        val json = parser.parse(json_text).getAsJsonObject()
        val gson = new GsonBuilder().setPrettyPrinting().create()
        return gson.toJson(json)
    }

    def processUsingApi(request: RequestToTextApi, path: URL): String = {
        val requestToJson = g.toJson(request)
        val encoded_text = requestToJson.getBytes("UTF-8")
        val connection = getConnection(path)
        val wr = new DataOutputStream(connection.getOutputStream())
        wr.write(encoded_text, 0, encoded_text.length)
        wr.flush()
        wr.close()

        val response = new StringBuilder()
        val in = new BufferedReader(new InputStreamReader(connection.getInputStream()))
        var line = in.readLine()
        while (line != null) {
            response.append(line)
            line = in.readLine()
        }
        in.close()
        return response.toString()
    }

    def getLanguage (inputDocs: RequestToTextApi): Option[Language] = {
        try {
            val response = processUsingApi(inputDocs, languagesUrl)
            val niceResponse = prettify(response)
            val language = g.fromJson(niceResponse, classOf[Language])
            if (language.documents(0).detectedLanguages(0).iso6391Name == "(Unknown)")
                return None
            return Some(language)
        } catch {
            case e: Exception => return None
        }
    }

    def getSentiment (inputDocs: RequestToTextApi): Option[Sentiment] = {
        try {
            val response = processUsingApi(inputDocs, sentimenUrl)
            val niceResponse = prettify(response)
            val sentiment = g.fromJson(niceResponse, classOf[Sentiment])
            return Some(sentiment)
        } catch {
            case e: Exception => return None
        }
    }
}

val toSentiment = (textContent: String) =>
        {
            val inputObject = new RequestToTextApi(Array(new RequestToTextApiDocument(textContent, textContent)))
            val detectedLanguage = SentimentDetector.getLanguage(inputObject)
            
            detectedLanguage match {
                case Some(language) =>
                    if(language.documents.size > 0) {
                        inputObject.documents(0).language = language.documents(0).detectedLanguages(0).iso6391Name
                        val sentimentDetected = SentimentDetector.getSentiment(inputObject)
                        sentimentDetected match {
                            case Some(sentiment) => {
                                if(sentiment.documents.size > 0) {
                                    sentiment.documents(0).score.toString()
                                }
                                else {
                                    "Error happened when getting sentiment: " + sentiment.errors(0).toString
                                }
                            }
                            case None => "Couldn't detect sentiment"
                        }
                    }
                    else {
                        "Error happened when getting language" + language.errors(0).toString
                    }
                case None => "Couldn't detect language"
            }
        }

// COMMAND ----------

spark.udf.register("toSentiment", toSentiment)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT orderid AS OrderId, orderdate AS OrderDate, customer AS CustomerId,
// MAGIC   product AS ProductId, billingaddress AS BillingAddress, units AS NoOfUnits,
// MAGIC   remarks AS OrderRemarks, toSentiment(remarks) AS SentimentScore
// MAGIC FROM OrdersDB.RawOrders

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT DISTINCT(billingaddress)
// MAGIC FROM OrdersDB.RawOrders

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE WIDGET DROPDOWN billingaddress 
// MAGIC DEFAULT "Bangalore" 
// MAGIC CHOICES 
// MAGIC   SELECT DISTINCT billingaddress 
// MAGIC   FROM OrdersDB.RawOrders

// COMMAND ----------

val selectedBillingAddress = dbutils.widgets.get("billingaddress")
val sqlStatement = """
  SELECT orderid AS OrderId, orderdate AS OrderDate, customer AS CustomerId,
    product AS ProductId, billingaddress AS BillingAddress, units AS NoOfUnits,
    remarks AS OrderRemarks, toSentiment(remarks) AS SentimentScore
  FROM OrdersDB.RawOrders
  WHERE BillingAddress = '""" + selectedBillingAddress + "'" 

val results = spark.sql(sqlStatement);

display(results)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ![Microsoft](https://cdn.pixabay.com/photo/2013/02/12/09/07/microsoft-80660_960_720.png)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC \\( f(\beta)= -Y_t^T X_t \beta + \sum log( 1+{e}^{X_t\bullet\beta}) + \frac{1}{2}\delta^t S_t^{-1}\delta\\)
// MAGIC 
// MAGIC where \\(\delta=(\beta - \mu_{t-1})\\)
// MAGIC 
// MAGIC $$\sum_{i=0}^n i^2 = \frac{(n^2+n)(2n+1)}{6}$$

// COMMAND ----------

displayHTML("""<svg width="100" height="100">
   <circle cx="50" cy="50" r="40" stroke="green" stroke-width="4" fill="yellow" />
   Sorry, your browser does not support inline SVG.
</svg>""")

// COMMAND ----------

val colorsRDD = sc.parallelize(
	Array(
		(197,27,125), (222,119,174), (241,182,218), (253,244,239), (247,247,247), 
		(230,245,208), (184,225,134), (127,188,65), (77,146,33)))

val colors = colorsRDD.collect()

displayHTML(s"""
<!DOCTYPE html>
<meta charset="utf-8">
<style>

path {
  fill: yellow;
  stroke: #000;
}

circle {
  fill: #fff;
  stroke: #000;
  pointer-events: none;
}

.PiYG .q0-9{fill:rgb${colors(0)}}
.PiYG .q1-9{fill:rgb${colors(1)}}
.PiYG .q2-9{fill:rgb${colors(2)}}
.PiYG .q3-9{fill:rgb${colors(3)}}
.PiYG .q4-9{fill:rgb${colors(4)}}
.PiYG .q5-9{fill:rgb${colors(5)}}
.PiYG .q6-9{fill:rgb${colors(6)}}
.PiYG .q7-9{fill:rgb${colors(7)}}
.PiYG .q8-9{fill:rgb${colors(8)}}

</style>
<body>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.6/d3.min.js"></script>
<script>

var width = 960,
    height = 500;

var vertices = d3.range(100).map(function(d) {
  return [Math.random() * width, Math.random() * height];
});

var svg = d3.select("body").append("svg")
    .attr("width", width)
    .attr("height", height)
    .attr("class", "PiYG")
    .on("mousemove", function() { vertices[0] = d3.mouse(this); redraw(); });

var path = svg.append("g").selectAll("path");

svg.selectAll("circle")
    .data(vertices.slice(1))
  .enter().append("circle")
    .attr("transform", function(d) { return "translate(" + d + ")"; })
    .attr("r", 2);

redraw();

function redraw() {
  path = path.data(d3.geom.delaunay(vertices).map(function(d) { return "M" + d.join("L") + "Z"; }), String);
  path.exit().remove();
  path.enter().append("path").attr("class", function(d, i) { return "q" + (i % 9) + "-9"; }).attr("d", String);
}

</script>
  """)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT orderid AS OrderId, orderdate AS OrderDate, customer AS CustomerId,
// MAGIC     product AS ProductId, billingaddress AS BillingAddress, units AS NoOfUnits,
// MAGIC     remarks AS OrderRemarks, toSentiment(remarks) AS SentimentScore
// MAGIC   FROM OrdersDB.RawOrders

// COMMAND ----------

val results = spark.sql("""SELECT orderid AS OrderId, orderdate AS OrderDate, customer AS CustomerId,
    product AS ProductId, billingaddress AS BillingAddress, units AS NoOfUnits,
    remarks AS OrderRemarks, toSentiment(remarks) AS SentimentScore
  FROM OrdersDB.RawOrders""")

// COMMAND ----------

// SQL Datawarehouse

val blobStorage = "iomegastoragev2.blob.core.windows.net"
val blobContainer = "data"
val blobAccessKey =  "22le5kRkFF0iaJjBePjSIlqCRY6Kz7LCr3DXaONm9smXFUoTtfVTJXzor7Dq/TSpfNHDIa2MSwQzDqjNenMPJg=="

val tempDir = "wasbs://" + blobContainer + "@" + blobStorage +"/tempDirs"
val acntInfo = "fs.azure.account.key."+ blobStorage

sc.hadoopConfiguration.set(acntInfo, blobAccessKey)

val dwDatabase = "iomegadwh2"
val dwServer = "iomegadwsqlserver.database.windows.net"
val dwUser = "iomegaadmin"
val dwPass = "admin@123"
val dwJdbcPort =  "1433"
val dwJdbcExtraOptions = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
val sqlDwUrl = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass + ";$dwJdbcExtraOptions"
val sqlDwUrlSmall = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass

spark.conf.set("spark.sql.parquet.writeLegacyFormat","true")

results
	.write
	.format("com.databricks.spark.sqldw")
	.option("url", sqlDwUrlSmall)
	.option("dbtable", "ProcessedOrders")       
	.option( "forward_spark_azure_storage_credentials","True")
	.option("tempdir", tempDir)
	.mode("overwrite")
	.save()
	


// COMMAND ----------

// Cosmos DB Integration

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.config.Config

// COMMAND ----------

val readConfig = Config(Map(
  "Endpoint" -> "https://iomegacosmosv2.documents.azure.com:443/",
  "Masterkey" -> "QWqFDS3YlbdvEEnCIkOwcWl4x0DakzDwiEk71z45aROrmHySjcgejxJkdeDr0EHViQYUrFDN1ZBsuWAGBgkZ5w==",
  "Database" -> "ordersystemdb",
  "Collection" -> "container1",
  "query_custom" -> "SELECT * FROM c"
))

val orders = spark.read.cosmosDB(readConfig)

// COMMAND ----------

display(orders)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM OrdersDB.RawOrders

// COMMAND ----------

val rawOrders = spark.sql("SELECT orderid AS OrderId, orderdate, customer, product, billingaddress AS BillingAddress, units, remarks FROM OrdersDB.RawOrders")

// COMMAND ----------

import org.apache.spark.sql.SaveMode

val writeConfig = Config(Map(
  "Endpoint" -> "https://iomegacosmosv2.documents.azure.com:443/",
  "Masterkey" -> "QWqFDS3YlbdvEEnCIkOwcWl4x0DakzDwiEk71z45aROrmHySjcgejxJkdeDr0EHViQYUrFDN1ZBsuWAGBgkZ5w==",
  "Database" -> "ordersystemdb",
  "Collection" -> "container1",
  "Upsert" -> "true"
))

rawOrders.write.mode(SaveMode.Overwrite).cosmosDB(writeConfig)