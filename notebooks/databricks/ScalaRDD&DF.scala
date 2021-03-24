// Databricks notebook source
// MAGIC %md # Preprocessing

// COMMAND ----------

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import scala.collection.mutable.ListBuffer
import org.apache.spark.util.LongAccumulator
import scala.io.Source
import sys.process._
import java.io._
import sys.env

// COMMAND ----------

dbutils.widgets.text("username", "", "Please enter your git username")
dbutils.widgets.text("password", "", "Please enter your password")

// COMMAND ----------

// MAGIC %python
// MAGIC username = dbutils.widgets.get("username")
// MAGIC password = dbutils.widgets.get("password")
// MAGIC url = "https://" + username + ":" + password + "@github.com/MaximeRedstone/graphsMapReduce.git"
// MAGIC email = username + username + "@gmail.com"
// MAGIC dbutils.widgets.removeAll()

// COMMAND ----------

// MAGIC %python
// MAGIC import os
// MAGIC !git config --global user.email $email
// MAGIC !git config --global user.name $username
// MAGIC   
// MAGIC if os.path.exists(os.path.join(os.getcwd(), "graphsMapReduce")):
// MAGIC   !cd graphsMapReduce && git pull
// MAGIC else:
// MAGIC   !git clone $url
// MAGIC 
// MAGIC !cd graphsMapReduce/generator && make re

// COMMAND ----------

// MAGIC %sh
// MAGIC cd graphsMapReduce/generator && ./graph_generator 20M ../data/graph_20M.txt 5
// MAGIC ./graph_generator 40M ../data/graph_40M.txt 5
// MAGIC ./graph_generator 60M ../data/graph_60M.txt 5

// COMMAND ----------

// File 1

val filename = "toy_dataset.txt"
val pwd = Paths.get(System.getProperty("user.dir"))
val abs_filepath = Paths.get(pwd.toString(), "graphsMapReduce", "data", filename)

val filestore_name = "dbfs:/FileStore/" + filename
dbutils.fs.rm(filestore_name)
val file = new File(abs_filepath.toString())
val fileContents = Source.fromFile(file).getLines.mkString("\n")
dbutils.fs.put(filestore_name, fileContents)

// COMMAND ----------

// MAGIC %md # RDD version

// COMMAND ----------

def processGraphSortedRDD(filepath: String): Unit = {
  
  def reduce_ccf_sorted(key: Int, values: ListBuffer[Int], accum: LongAccumulator): ListBuffer[(Int, Int)] = {
    
    val min_value = values.remove(0)
    var ret = new ListBuffer[(Int, Int)]()
    if (min_value < key) {
      ret += ((key, min_value))
      
      accum.add(values.length)
      for (i <- 0 until values.length) {
        ret += ((values(i), min_value))
      }
    }
    return ret
  }
  
  val text_file = sc.textFile(filepath).filter(x => !(x contains "#"))
  val text_file_split = text_file.map(_.split("\t"))
  var input = text_file_split.map(s => (s(0).toInt, s(1).toInt))
  
  var accum = sc.longAccumulator("Accumulator")
  accum.add(1)
  while (accum.value > 0) {
    
    accum.reset()
    println(s"Start loop, $accum.value")

    // CCF-Iterate
    val it_map = input.flatMap(x => List(List(x._1, x._2), List(x._2, x._1)) )
    val it_map_tuple = it_map.map(x => (x(0), x(1)))
    val it_groupby = it_map_tuple.groupByKey().mapValues(x => x.to[ListBuffer].sorted)
    val it_reduce = it_groupby.flatMap(x => reduce_ccf_sorted(x._1, x._2, accum))
        
    // CCF-Dedup
    val ded_map = it_reduce.map(x => ((x._1, x._2), None))
    val ded_groupby = ded_map.groupByKey()
    input = ded_groupby.map(x => (x._1._1, x._1._2))
   
    val ret = input.collect()
    println(s"End loop, $accum.value")
  }
  println("Processed file")
}

// COMMAND ----------

processGraphSortedRDD(filestore_name)

// COMMAND ----------

// MAGIC %md # DF version

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import java.time.LocalDateTime
import java.sql.Timestamp
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.collect_list
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.spark.util.LongAccumulator
import scala.io.Source
import spark.sqlContext.implicits._
import org.apache.spark.sql.functions.split

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{ MapType, IntegerType, ArrayType, StructField, StructType }

// COMMAND ----------

def graphsDF(filestore_name: String): DataFrame = {
  
  val acc = sc.longAccumulator("accumulator")

  def reduce_ccf(key: Int, values: Seq[Int]): ListBuffer[(Int, Int)] = {
    val list_values = values.to[ListBuffer]
    val min_value = list_values.remove(list_values.indexOf(list_values.min))
    var ret = ListBuffer[(Int, Int)]()
    if (min_value < key) { 
      ret += ((key, min_value))
      for (i <- 0 until list_values.length) {
        acc.add(1)
        ret += ((list_values(i), min_value))
      }
    }
    return ret
  }

  val schema = StructType(List(StructField("key", IntegerType, true), StructField("value", IntegerType, true)))
  
  var df = spark.read.format("csv")
      .option("header", "false") 
      .option("delimiter", "\t")
      .schema(schema)
      .load(filestore_name)
  
  df = df.na.drop()  
              
  val reducer = udf {(x: Int, y: Seq[Int]) => reduce_ccf(x, y)}
  var current = 0L
  var extracted_val = 1L
  var previous = 0L
  
  while (extracted_val != 0) {
    
    previous = acc.value
    
    /* CCF-Iterate */
    val df_inverter = df.select(col("value").alias("key"), col("key").alias("value"))
    df = df.union(df_inverter)
    
    /* Creates the (key, [values]) pairs */
    df = df.groupBy("key").agg(collect_list("value").alias("value")) 
    acc.reset()
    df = df.select(reducer(col("key"), col("value")).alias("reducer"))
    df.collect()
    extracted_val = acc.value
    
    /* Explode & clear nones */
    df = df.select(explode(col("reducer")))
    df = df.na.drop()
    df = df.select(col("col._1").alias("key"), col("col._2").alias("value"))

    /* CCF - Dedup */
    df = df.dropDuplicates()
    df = df.select(col("key"), col("value"))
    current = acc.value
    
    extracted_val = current - previous
    println(s"Extracted val: $extracted_val")
    println("-------")
  }

  return df
}

// COMMAND ----------

val filename = "toy_dataset.txt"
// val filename = "graph_5M.txt"

val pwd = Paths.get(System.getProperty("user.dir"))
val abs_filepath = Paths.get(pwd.toString(), "graphsMapReduce", "data", filename)

val filestore_name = "dbfs:/FileStore/" + filename
dbutils.fs.rm(filestore_name)
val file = new File(abs_filepath.toString())
val fileContents = Source.fromFile(file).getLines.mkString("\n")
dbutils.fs.put(filestore_name, fileContents)

// COMMAND ----------

val df = graphsDF(filestore_name)

// COMMAND ----------

df.show()
