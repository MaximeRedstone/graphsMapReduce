// Databricks notebook source
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

def processGraphSorted(filepath: String): Unit = {
  
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

// MAGIC %sh
// MAGIC cd graphsMapReduce/generator && ./graph_generator 20M ../data/graph_20M.txt 5
// MAGIC ./graph_generator 40M ../data/graph_40M.txt 5
// MAGIC ./graph_generator 60M ../data/graph_60M.txt 5

// COMMAND ----------

// File 1
val filename = "graph_20M.txt"
val pwd = Paths.get(System.getProperty("user.dir"))
val abs_filepath = Paths.get(pwd.toString(), "graphsMapReduce", "data", filename)

val filestore_name = "dbfs:/FileStore/" + filename
dbutils.fs.rm(filestore_name)
val file = new File(abs_filepath.toString())
val fileContents = Source.fromFile(file).getLines.mkString("\n")
dbutils.fs.put(filestore_name, fileContents)
processGraphSorted(filestore_name)

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
