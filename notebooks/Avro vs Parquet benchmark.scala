// Databricks notebook source
// MAGIC %md # New Databricks Avro support vs Parque benchmark
// MAGIC 
// MAGIC Based in the notebook available here: https://databricks.com/blog/2018/11/30/apache-avro-as-a-built-in-data-source-in-apache-spark-2-4.html .
// MAGIC 
// MAGIC Added comparison with Parquet based on time to read, write and space ocuppied.

// COMMAND ----------

import java.sql.Date
import java.util
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.math.BigDecimal.RoundingMode
import scala.util.Random

import com.google.common.io.Files

import org.apache.spark.sql._
import org.apache.spark.sql.types._

val tempDir = Files.createTempDir() // https://google.github.io/guava/releases/19.0/api/docs/com/google/common/io/Files.html
val avroDir = tempDir + "/avro"
val parquetDir = tempDir + "/parquet"
val numberOfRows = 1000000
val defaultSize = 100 // Size used for items in generated RDD like strings, arrays and maps

val testSchema = StructType(Seq(
  StructField("StringField", StringType, false),
  StructField("IntField", IntegerType, true),
  StructField("dateField", DateType, true),
  StructField("DoubleField", DoubleType, false),
  StructField("DecimalField", DecimalType(10, 10), true),
  StructField("ArrayField", ArrayType(BooleanType), false),
  StructField("MapField", MapType(StringType, IntegerType), true),
  StructField("StructField", StructType(Seq(StructField("id", IntegerType, true))), false)))

/**
 * This function generates a random map(string, int) of a given size.
 */
private def generateRandomMap(rand: Random, size: Int): java.util.Map[String, Int] = {
  val jMap = new util.HashMap[String, Int]()
  for (i <- 0 until size) {
    jMap.put(rand.nextString(5), i)
  }
  jMap
}

/**
 * This function generates a random array of booleans of a given size.
 */
private def generateRandomArray(rand: Random, size: Int): util.ArrayList[Boolean] = {
  val vec = new util.ArrayList[Boolean]()
  for (i <- 0 until size) {
    vec.add(rand.nextBoolean())
  }
  vec
}

private def generateRandomRow(): Row = {
  val rand = new Random()
  Row(rand.nextString(defaultSize), rand.nextInt(), new Date(rand.nextLong()), rand.nextDouble(),
    BigDecimal(rand.nextDouble()).setScale(10, RoundingMode.HALF_UP),
    generateRandomArray(rand, defaultSize).asScala,
    generateRandomMap(rand, defaultSize).asScala, Row(rand.nextInt()))
}

println(s"\n\n\nPreparing for a benchmark test - creating a RDD with $numberOfRows rows\n\n\n")

val testDataFrame = spark.createDataFrame(
  spark.sparkContext.parallelize(0 until numberOfRows).map(_ => generateRandomRow()),
  testSchema)

// COMMAND ----------

/**
 * spark-avro write benchmark.
 */
spark.time {
  testDataFrame.write.format("avro").save(avroDir)
}

// COMMAND ----------

// avro - how much space it takes in Mb
val avroSpace =  dbutils.fs.ls(avroDir).map(_.size).sum/1024/1024 //mb

// COMMAND ----------

spark.time {
  testDataFrame.write.format("parquet").save(parquetDir)
}

// COMMAND ----------

// parquet - how much space it takes in Mb
val parquetSpace =  dbutils.fs.ls(parquetDir).map(_.size).sum/1024/1024 //mb

// COMMAND ----------

/**
 * spark-avro read benchmark.
 */
spark.time {
  spark.read.format("avro").load(avroDir).queryExecution.toRdd.foreach(_ => ())
}

// COMMAND ----------

spark.time {
  spark.read.format("parquet").load(parquetDir).queryExecution.toRdd.foreach(_ => ())
}

// COMMAND ----------

// Clean up output files
import org.apache.commons.io.FileUtils

FileUtils.deleteDirectory(tempDir)