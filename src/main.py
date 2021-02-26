import os
import logging
import logzero
from logzero import logger
from spark_logger import SparkLogger

filepath = os.path.join("..", "cluster_logs", "eventlogs", "2222592368926707400", "eventlog.json")
spark_logger = SparkLogger(filepath)
spark_logger.generate_database()

root_path = os.path.join("..", "cleaned_logs")
if not os.path.exists(root_path):
    os.mkdir(root_path)

spark_logger.write_files(root_path)