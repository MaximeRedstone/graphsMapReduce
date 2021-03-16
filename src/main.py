import os
import logging
import logzero
from logzero import logger
import argparse
from spark_logger import SparkLogger

parser = argparse.ArgumentParser(description='Clean the logs of Spark processes')
parser.add_argument('-e', '--eventdir', type=str, help='path to the log directory')
parser.add_argument('-c', '--comment', type=str, help='comment on run', default="")
parser.add_argument('-j', '--json_filename', type=str, help='filename saved json', default="")
parser.add_argument('-f', '--txt_filename', type=str, help='filename of txt file run', default="")
parser.add_argument('-p', '--full_path', type=str, help='filepath of json logs', default="")
args = parser.parse_args()

# filepath = os.path.join("..", "..", "eventlogs", args.eventdir, args.json_filename)
filepath = args.full_path
root_path = os.path.join("..", "cleaned_logs")

spark_logger = SparkLogger(filepath, args.txt_filename)
spark_logger.generate_database()
spark_logger.write_files(root_path, comment=args.comment)