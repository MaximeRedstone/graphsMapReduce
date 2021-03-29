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

import org.apache.spark.sql.DataFrame
import java.time.LocalDateTime
import java.sql.Timestamp
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.collect_list
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

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
import org.apache.spark.TaskContext

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
// MAGIC cd graphsMapReduce/generator 
// MAGIC ./graph_generator 20M ../data/graph_20M.txt 5
// MAGIC ./graph_generator 30M ../data/graph_30M.txt 5
// MAGIC ./graph_generator 40M ../data/graph_40M.txt 5
// MAGIC ./graph_generator 60M ../data/graph_60M.txt 5
// MAGIC ./graph_generator 10M ../data/graph_10M.txt 5
// MAGIC ./graph_generator 50M ../data/graph_50M.txt 5

// COMMAND ----------

case class Log(filename: String, algo_type: String, loop_counter: Int, command: String, end_of_command: Timestamp, accum: Long, length: Int)

def add_log(df: DataFrame, filename: String, algo_type: String, loop_counter: Int, command: String, end_of_command: Timestamp, accum: Long, length: Int): DataFrame = {
    val new_log = new Log(filename, algo_type, loop_counter, command, end_of_command, accum, length)
    val new_df = Seq(new_log).toDF()
    var union_df = df.union(new_df)
    return union_df
}

// COMMAND ----------

// MAGIC %md # RDD version

// COMMAND ----------

def processGraphSorted(filepath: String, df_logs: DataFrame, logs: Boolean): DataFrame = {
  
  def reduce_ccf_sorted(key: Int, values: ListBuffer[Int], accum: LongAccumulator): ListBuffer[(Int, Int)] = {
    
    println(values)
    val min_value = values.remove(0)
    var ret = ListBuffer[(Int, Int)]()
    if (min_value < key) {
      ret += ((key, min_value))
      
      accum.add(values.length)
      for (i <- 0 until values.length) {
        ret += ((values(i), min_value))
      }
    }
    return ret
  }
    
  var text_file = sc.textFile(filepath)
  text_file = text_file.filter(x => !(x contains "#"))
  val text_file_split = text_file.map(_.split("\t"))
  var df_logs_ret = df_logs
  
  var now = LocalDateTime.now()
  var time = Timestamp.valueOf(now);
  df_logs_ret = add_log(df_logs_ret, filepath, "scala-rdd-sorted", 0, "start", time, 0, 0)
  var input = text_file_split.map(s => (s(0).toInt, s(1).toInt))

  var accum = sc.longAccumulator("Accumulator")
  accum.add(1)
  var loop_counter = 1
  while (accum.value > 0) {
    
    accum.reset()
    println(s"Start loop, $accum.value")

    // CCF-Iterate
    val it_map = input.flatMap(x => List(List(x._1, x._2), List(x._2, x._1)) )
    val it_map_tuple = it_map.map(x => (x(0), x(1)))
    if (logs == true) {
      var debug = it_map_tuple.collect()
      now = LocalDateTime.now()
      time = Timestamp.valueOf(now);
      df_logs_ret = add_log(df_logs_ret, filepath, "scala-rdd-sorted", loop_counter, "it_map", time, accum.value, debug.length)
    }
    
    val it_groupby = it_map_tuple.groupByKey().mapValues(x => x.to[ListBuffer].sorted)
    if (logs == true) {
      var debug = it_groupby.collect()
      now = LocalDateTime.now()
      time = Timestamp.valueOf(now);
      df_logs_ret = add_log(df_logs_ret, filepath, "scala-rdd-sorted", loop_counter, "it_groupby", time, accum.value, debug.length)      
    }
    
    val it_reduce = it_groupby.flatMap(x => reduce_ccf_sorted(x._1, x._2, accum))
    if (logs == true) {
      var debug = it_reduce.collect()
      now = LocalDateTime.now()
      time = Timestamp.valueOf(now);
      df_logs_ret = add_log(df_logs_ret, filepath, "scala-rdd-sorted", loop_counter, "it_reduce", time, accum.value, debug.length)
    }

    // CCF-Dedup
//     val ded_map = it_reduce.map(x => ((x._1, x._2), None))
// //     ded_map.collect()
//     now = LocalDateTime.now()
//     time = Timestamp.valueOf(now);
// //     df_logs = add_log(df_logs, filepath, "min", loop_counter, "ded_map", time, accum.value)
    
//     val ded_groupby = ded_map.groupByKey()
// //     ded_groupby.collect()
//     now = LocalDateTime.now()
//     time = Timestamp.valueOf(now);
//     df_logs = add_log(df_logs, filepath, "min", loop_counter, "ded_groupby", time, accum.value)
    
//     input = ded_groupby.map(x => (x._1._1, x._1._2))
    
    input = it_reduce.distinct()
    val ret = input.collect()
    now = LocalDateTime.now()
    time = Timestamp.valueOf(now);
    df_logs_ret = add_log(df_logs_ret, filepath, "scala-rdd-sorted", loop_counter, "ded_reduce", time, accum.value, ret.length)
        
    println(s"End loop at $now, accum_value is $accum.value")
    loop_counter += 1
  }
  println("Processed file")
  return df_logs_ret
}

// COMMAND ----------

// MAGIC %md # DF version

// COMMAND ----------

def graphsDF(filestore_name: String, df_logs: DataFrame, logs:Boolean): DataFrame = {
  
  var acc = sc.longAccumulator("accumulator")

  def reduce_ccf(key: Int, values: Seq[Int]): ListBuffer[(Int, Int)] = {
    println(s"in udf, values = $values")
    val list_values = values.to[ListBuffer]
    val min_value = list_values.remove(list_values.indexOf(list_values.min))
    var ret = ListBuffer[(Int, Int)]()
    if (min_value < key) { 
      ret += ((key, min_value))
      acc.add(list_values.length)
      for (i <- 0 until list_values.length) {
        ret += ((list_values(i), min_value))
      }
    }
    return ret
  }

  val schema = StructType(List(StructField("key", IntegerType, true), StructField("value", IntegerType, true)))
  var df_logs_ret = df_logs
  
  var df = spark.read.format("csv")
      .option("header", "false") 
      .option("delimiter", "\t")
      .schema(schema)
      .load(filestore_name)
  
  df = df.na.drop()
  
  var now = LocalDateTime.now()
  var time = Timestamp.valueOf(now);
  df_logs_ret = add_log(df_logs_ret, filestore_name, "scala-df", 0, "start", time, 0, 0)
              
  val reducer = udf {(x: Int, y: Seq[Int]) => reduce_ccf(x, y)}.asNondeterministic()
  var extracted_val = 1L
  var loop_counter = 1
  
  while (extracted_val != 0) {
    
    acc.reset()
    
    /* CCF-Iterate */
    val df_inverter = df.select(col("value").alias("key"), col("key").alias("value"))
    val df_it_map = df.union(df_inverter)
    if (logs == true) {
      acc.reset()
      var debug = df_it_map.collect()
      now = LocalDateTime.now()
      time = Timestamp.valueOf(now);
      df_logs_ret = add_log(df_logs_ret, filestore_name, "scala-df", loop_counter, "it-map", time, acc.value, debug.length)      
    }
    
    /* Creates the (key, [values]) pairs */
    val df_it_groupby = df_it_map.groupBy("key").agg(collect_list("value").alias("value")) 
    if (logs == true) {
      acc.reset()
      var debug = df_it_groupby.collect()
      now = LocalDateTime.now()
      time = Timestamp.valueOf(now);
      df_logs_ret = add_log(df_logs_ret, filestore_name, "scala-df", loop_counter, "it-groupby", time, acc.value, debug.length)   
    }
    
    acc = sc.longAccumulator("accumulator")
    var df_it_reduce = df_it_groupby.withColumn("reducer", reducer(col("key"), col("value")))//.persist(StorageLevel.MEMORY_ONLY)
    
    /* Explode & clear nones */
    df_it_reduce = df_it_reduce.select(explode(col("reducer")))
    df_it_reduce =df_it_reduce.na.drop()
    df_it_reduce = df_it_reduce.select(col("col._1").alias("key"), col("col._2").alias("value"))
    if (logs == true) {
      var debug = df_it_reduce.collect()
      now = LocalDateTime.now()
      time = Timestamp.valueOf(now);
      df_logs_ret = add_log(df_logs_ret, filestore_name, "scala-df", loop_counter, "it-reduce", time, acc.value, debug.length)      
    }

    /* CCF - Dedup */
    var df_dedup = df_it_reduce.dropDuplicates()
    df = df_dedup.select(col("key"), col("value"))

    var collected = df.collect()
    df = spark.createDataFrame(sc.parallelize(collected), schema)
    extracted_val = acc.value
    
    now = LocalDateTime.now()
    time = Timestamp.valueOf(now);
    df_logs_ret = add_log(df_logs_ret, filestore_name, "scala-df", loop_counter, "ded_reduce", time, extracted_val, collected.length)
    
    loop_counter += 1
    println(s"Extracted val: $extracted_val")
    println("-------")
  }

  return df_logs_ret
}

// COMMAND ----------

val filename = "web-Google.txt"

val filestore_name = "dbfs:/FileStore/" + filename
dbutils.fs.rm(filestore_name)

val pwd = Paths.get(System.getProperty("user.dir"));
val abs_filepath = Paths.get(pwd.toString(), "graphsMapReduce", "data", filename);

val file = new File(abs_filepath.toString())
val fileContents = Source.fromFile(file).getLines.mkString("\n")
dbutils.fs.put(filestore_name, fileContents)
var df_logs = Seq.empty[Log].toDF()

df_logs = processGraphSorted(filestore_name, df_logs, false)
df_logs = graphsDF(filestore_name, df_logs, false)

// COMMAND ----------

display(df_logs)
