import os
import logging
import logzero
from logzero import logger
import argparse
from spark_logger import SparkLogger

parser = argparse.ArgumentParser(description='Clean the logs of Spark processes')
parser.add_argument('-e', '--eventdir', type=str, help='path to the log directory')
args = parser.parse_args()

filepath = os.path.join("..", "..", "eventlogs", args.eventdir, "eventlog.json")
root_path = os.path.join("..", "cleaned_logs")

spark_logger = SparkLogger(filepath)
spark_logger.generate_database()
spark_logger.write_files(root_path)