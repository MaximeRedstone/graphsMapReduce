# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
from IPython import get_ipython

# %% [markdown]
# # Pre-processing
# 
# Loading of the graph generator

# %%
import os
import warnings
from datetime import datetime
import subprocess
from time import time

import pandas as pd
import numpy as np
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DataType, FloatType, MapType
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F 


# %%
#filestore_name = os.path.join('..', 'data', 'graph_5M.txt')
#filestore_name = os.path.join('.', 'graph_5M.txt')
# filestore_name = os.path.join('.', 'toy_dataset.txt')

# filestore_name = os.path.join('.', 'web-Google.txt')

# print(filestore_name)


# %%
# sc = SparkContext()
# spark = SparkSession.builder.master('local[3]').getOrCreate()

# %% [markdown]
# # RDD implementation

def add_log(df, filepath, algo_type, loop_counter, command, timestamp, accum, length):
    
    single_row = pd.DataFrame(columns=['filename', 'nb_cores', 'loop_counter', 'command', 'end_of_command', 'accum', 'length'])
    single_row.loc[0] = [filepath, algo_type, loop_counter, command, timestamp, accum, length]
    with_row = pd.concat([df, single_row])
    return with_row

def processGraphRDDLogs(filepath, df_logs, nb_cores, spark, sc):
  
    def reduce_ccf_min(x):
        key = x[0]
        values = x[1]
        min_value = values.pop(np.argmin(values))
        ret = []
        if min_value < key:
            ret.append((key, min_value))
            accum.add(len(values))
            for value in values:
                ret.append((value, min_value))
        return (ret)

    text_file = sc.textFile(filepath)
    text_file = text_file.filter(lambda x: "#" not in x)
    
    df_logs = add_log(df_logs, filepath, nb_cores, 0, "start", datetime.now(), 0, 0)
    text_file_split = text_file.map(lambda x: x.split())
    input = text_file_split.map(lambda x: (int(x[0]), int(x[1])))

    accum = sc.accumulator(1)
    loop_counter = 1
    while accum.value != 0:

        accum.value = 0
        print(f"----------\nStart loop at {datetime.now()}, accum_value is {accum.value}")

        # CCF-Iterate
        it_map = input.flatMap(lambda x: ((x[0], x[1]), (x[1], x[0])))
#         it_map.collect()
#         df_logs = add_log(df_logs, filepath, "rdd", loop_counter, "it_map", datetime.now(), accum.value)
        
        it_groupby = it_map.groupByKey().mapValues(list)
#         it_groupby.collect()
#         df_logs = add_log(df_logs, filepath, "rdd", loop_counter, "it_groupby", datetime.now(), accum.value)
        
        it_reduce = it_groupby.flatMap(lambda x: reduce_ccf_min(x))
#         it_reduce.collect()
#         df_logs = add_log(df_logs, filepath, "rdd", loop_counter, "it_reduce", datetime.now(), accum.value)
        
        
#         df_logs = add_log(df_logs, filepath, "rdd", loop_counter, "dedup", datetime.now(), accum.value)

        # CCF-Dedup
  
        input = it_reduce.distinct()
        ret = input.collect()
          
          
#         ded_map = it_reduce.map(lambda x: ((x[0], x[1]), None))
#         ded_map.collect()
#         df_logs = add_log(df_logs, filepath, "rdd", loop_counter, "ded_map", datetime.now(), accum.value)
        
#         ded_groupby = ded_map.groupByKey().mapValues(list)
#         ded_groupby.collect()
#         df_logs = add_log(df_logs, filepath, "rdd", loop_counter, "ded_groupby", datetime.now(), accum.value)
        
#         input = ded_groupby.map(lambda x: (x[0][0], x[0][1]))        
#         viz = input.collect()

        df_logs = add_log(df_logs, filepath, nb_cores, loop_counter, "ded_reduce", datetime.now(), accum.value, len(ret))

        print(f"End loop at {datetime.now()}, accum_value is {accum.value}")
        loop_counter += 1

    print(f"----------\nProcessed file at {datetime.now()}\n----------")
    return df_logs

def processGraphDFLogsV2(filepath, df_logs, spark, sc):
  
  def reduce_ccf(key, values):
    min_value = values.pop(values.index(min(values)))
    ret = {}
    ret[key] = min_value
    if min_value < key:
      for value in values:
        acc.add(1)
        ret[value] = min_value
    else:
      ret = None
    return ret

  reducer = F.udf(lambda x, y: reduce_ccf(x, y), MapType(IntegerType(), IntegerType()))

  schema = StructType([
      StructField("key", IntegerType(), True),
      StructField("value", IntegerType(), True)])

  df = spark.read.format('csv').load(filepath, headers=False, delimiter='\t', schema=schema)
  df = df.na.drop()
  df_logs = add_log(df_logs, filepath, "python-df", 0, "start", datetime.now(), 0, 0)

  acc = sc.accumulator(1)
  loop_counter = 1
  
  while acc.value != 0:
    
    acc.value = 0
    print(f"----------\nStart loop at {datetime.now()}, accumulator value is {acc.value}")
    
    # CCF-Iterate
    df_inverter = df.select(F.col('value').alias('key'), F.col('key').alias('value'))
    df = df.union(df_inverter)
    
#     if logs==True:
#       acc.value = 0
#       debug = df.collect()
#       df_logs_ret = add_log(df_logs, filepath, "python-df", loop_counter, "it-map", datetime.now(), acc.value, len(debug)) 

    df = df.groupBy('key').agg(F.collect_list('value').alias('value'))
    df = df.withColumn('reducer', reducer('key', 'value')).select('reducer')
    df = df.select(F.explode('reducer'))
    df = df.na.drop()
    
    # CCF - Dedup
    df = df.distinct()
    collected = df.collect()
    df = spark.createDataFrame(sc.parallelize(collected), schema)
    print(f"End loop at {datetime.now()}, final value is {acc.value}")
    df_logs = add_log(df_logs, filepath, "python-df", loop_counter, "ded_reduce", datetime.now(), acc.value, len(collected))
    loop_counter += 1
  
  print(f"----------\nProcessed file at {datetime.now()}\n----------")
  return df_logs

def run_different_instances(df_logs, cores_list):

  for nb_cores in cores_list:
    print(f'Launching new spark context with {nb_cores} cores.')
    conf = (SparkConf().set("spark.cores.max", str(nb_cores)))
    sc = SparkContext(conf=conf)
    #sc.setLogLevel("INFO")
    spark = SparkSession.builder.master(f'local[{nb_cores}]').getOrCreate()
    df_logs = processGraphRDDLogs(filestore_name, df_logs, nb_cores, spark, sc)
    sc.stop()
    print('Stopped current context.')

  return df_logs


filestore_name = 'graph_30M_5.txt'

df_logs_local = pd.DataFrame(columns=['filename', 'nb_cores', 'loop_counter', 'command', 'end_of_command', 'accum', 'length'])
df_logs_local = run_different_instances(df_logs_local, [2, 3, 4])
# filestore_name = 'web-Google.txt'

# df_logs_rdd = processGraphRDDLogs(filestore_name, df_logs)
# df_logs_google = processGraphDFLogsV2(filestore_name, df_logs_rdd)
df_logs_local.to_csv('python_local_30M_5.csv', index=False)
# %%
