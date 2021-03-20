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


// val filename = "graph_20M.txt"
// val pwd = Paths.get(System.getProperty("user.dir"))
// val abs_filepath = Paths.get(pwd.toString(), "graphsMapReduce", "data", filename)

// val filestore_name = "dbfs:/FileStore/" + filename
// dbutils.fs.rm(filestore_name)
// val file = new File(abs_filepath.toString())
// val fileContents = Source.fromFile(file).getLines.mkString("\n")
// dbutils.fs.put(filestore_name, fileContents)


// File 2
// val filename = "graph_40M.txt"
// val pwd = Paths.get(System.getProperty("user.dir"))
// val abs_filepath = Paths.get(pwd.toString(), "graphsMapReduce", "data", filename)

// val filestore_name = "dbfs:/FileStore/" + filename
// dbutils.fs.rm(filestore_name)
// val file = new File(abs_filepath.toString())
// val fileContents = Source.fromFile(file).getLines.mkString("\n")
// dbutils.fs.put(filestore_name, fileContents)
// processGraphSorted(filestore_name)

// // File 3
// val filename = "graph_60M.txt"
// val pwd = Paths.get(System.getProperty("user.dir"))
// val abs_filepath = Paths.get(pwd.toString(), "graphsMapReduce", "data", filename)

// val filestore_name = "dbfs:/FileStore/" + filename
// dbutils.fs.rm(filestore_name)
// val file = new File(abs_filepath.toString())
// val fileContents = Source.fromFile(file).getLines.mkString("\n")
// dbutils.fs.put(filestore_name, fileContents)
// processGraphSorted(filestore_name)

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

// MAGIC %sh
// MAGIC ls eventlogs

// COMMAND ----------

// MAGIC %python
// MAGIC import subprocess
// MAGIC import os
// MAGIC 
// MAGIC event_dir = str(subprocess.check_output(['ls', 'eventlogs']))
// MAGIC event_dir = int(event_dir.split("\'")[1][:-2])
// MAGIC json_filename = 'eventlog_scala_rdd.json'
// MAGIC full_old_path = os.path.join('eventlogs', str(event_dir), 'eventlog')
// MAGIC full_new_path = os.path.join('eventlogs', str(event_dir), str(json_filename))
// MAGIC print(f"event_dir = {event_dir} \n full_old_path = {full_old_path} \n full_new_path = {full_new_path}")
// MAGIC !ls eventlogs/$event_dir

// COMMAND ----------

// MAGIC %sh
// MAGIC cd graphsMapReduce/generator && make fclean

// COMMAND ----------

// MAGIC %sh
// MAGIC cd graphsMapReduce && git pull

// COMMAND ----------

// MAGIC %python
// MAGIC !pip install logzero/
// MAGIC !mv $full_old_path $full_new_path
// MAGIC !cp eventlogs/$event_dir/$json_filename graphsMapReduce/$json_filename

// COMMAND ----------

// MAGIC %python
// MAGIC !cd graphsMapReduce && git pull
// MAGIC !cd graphsMapReduce/src && python3 main.py -e $event_dir -j $json_filename -c _scala -f graph_20M.txt ##### CHANGE FILENAME ######

// COMMAND ----------

// MAGIC %python
// MAGIC !cd graphsMapReduce/generator && make fclean
// MAGIC !cd graphsMapReduce/generator && git add -A
// MAGIC !cd graphsMapReduce/generator && git config --global user.email "sparkistansparkistan@gmail.com"
// MAGIC !cd graphsMapReduce/generator && git config --global user.name "sparkistan"
// MAGIC !cd graphsMapReduce/generator && git commit -m "Added logs"
// MAGIC !cd graphsMapReduce/generator && git push $url

// COMMAND ----------

// MAGIC %md # DF version

// COMMAND ----------

import org.apache.spark.sql.functions._

def processGraphSortedDF(filepath: String): Unit = {
  
  val schema = new StructType()
      .add("key",IntegerType,true)
      .add("value",IntegerType,true)
  
  val df = spark.read.format("csv").load(filepath, headers=False, delimiter='\t', schema=schema)

  var counter = 1

  while (true) {
    
    println("Starting new loop")
    
    // Creates the (value, key) pairs and adds them to the dataset
    df = df.na.drop()
    val df_inverter = df.select(col("value").alias("key"), col("key").alias("value"))
    df = df.union(df_inverter)
    
    // Creates the (key, [values]) pairs
    df = df.groupBy("key").agg(collect_list("value").alias("value")).sort("key")

    // Removes the lines verifying key < min(values)
    df = df.withColumn("min", array_min("value"))
    val df_check = df.where("min<key")
    val length_sum = df_check.select(size("value")).groupBy().sum().collect()(0)(0)

    // The counter is the stop condition, acting as an accumulator: when it is null, the loop will exit
    counter = length_sum - df_check.count()
    println("Counter value for this loop: ", counter)
    println("-----------")

    if (counter == 0) {
      df = df.withColumn("value", explode("value"))
      df.collect()
      break
    }

    else:
      df = df_check.withColumn("value", explode("value"))


    // Generating the (key, min_value) pairs in df3 and renaming them (key, value)

    val df_key_min = df.select("key", col("min").alias("value")).dropDuplicates()

    val df_value_min = df.select(col("value").alias("key"), col("min").alias("value"))

    val df_value_min = df_value_min.where("key!=value")

    val df = df_key_min.union(df_value_min)

    println("Final df :")
    val df = df.dropDuplicates()
    df.show()     
  }
}

// COMMAND ----------

processGraphSortedRDD(filestore_name)

// COMMAND ----------

// MAGIC %md
// MAGIC #Scala DataFrame Implementation (Max)

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

case class Log(filename: String, algo_type: String, loop_counter: Int, command: String, end_of_command: Timestamp, accum: Long)

def add_log(df: DataFrame, filename: String, algo_type: String, loop_counter: Int, command: String, end_of_command: Timestamp, accum: Long): DataFrame = {
    val new_log = new Log(filename, algo_type, loop_counter, command, end_of_command, accum)
    val new_df = Seq(new_log).toDF()
    var union_df = df.union(new_df)
    return union_df
}

def graphsDF(filestore_name: String): DataFrame = {

  def reduce_ccf(key: Int, values: Seq[Int]): ListBuffer[(Int, Int, Int)] = {
    val list_values = values.to[ListBuffer]
    val min_value = list_values.remove(list_values.indexOf(list_values.min))
    var ret = ListBuffer[(Int, Int, Int)]()
    if (min_value < key) {
      ret += ((key, min_value, 0))
      for (i <- 0 until list_values.length) {
        ret += ((list_values(i), min_value, 1))
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

  var extracted_val = 1L
  val reducer = udf {(x: Int, y: Seq[Int]) => reduce_ccf(x, y)}

  while (extracted_val != 0) {

    /* CCF-Iterate */
    val df_inverter = df.select(col("value").alias("key"), col("key").alias("value"))
    df = df.union(df_inverter)
    
    /* Creates the (key, [values]) pairs */
    df = df.groupBy("key").agg(collect_list("value").alias("value"))  
    df = df.withColumn("reducer", reducer(col("key"), col("value"))).select(col("reducer"))
  
    /* Explode & clear nones */
    df = df.select(explode(col("reducer")))
    df = df.na.drop()
    df = df.select(col("col._1").alias("key"), col("col._2").alias("value"), col("col._3").alias("accumulator"))

    /* CCF - Dedup */
    df = df.dropDuplicates()
    extracted_val = df.agg(sum("accumulator")).first.getLong(0)
    df = df.select(col("key"), col("value"))
    println(extracted_val)
  }

  return df
}

// COMMAND ----------

// MAGIC 
// MAGIC %sh
// MAGIC git config --global user.email $email
// MAGIC git config --global user.name $username
// MAGIC 
// MAGIC git clone https://sparkistan:Nutella336kJ@github.com/MaximeRedstone/graphsMapReduce.git
// MAGIC cd graphsMapReduce/generator && make re

// COMMAND ----------

// MAGIC %sh
// MAGIC head -5 graphsMapReduce/data/graph_5M.txt

// COMMAND ----------

// val filename = "toy_dataset.txt"
val filename = "graph_5M.txt"

val pwd = Paths.get(System.getProperty("user.dir"))
val abs_filepath = Paths.get(pwd.toString(), "graphsMapReduce", "data", filename)

val filestore_name = "dbfs:/FileStore/" + filename
dbutils.fs.rm(filestore_name)
val file = new File(abs_filepath.toString())
val fileContents = Source.fromFile(file).getLines.mkString("\n")
dbutils.fs.put(filestore_name, fileContents)

// COMMAND ----------

// MAGIC %fs ls dbfs:/FileStore/

// COMMAND ----------

val df = graphsDF(filestore_name)

// COMMAND ----------

df.show()

// COMMAND ----------


