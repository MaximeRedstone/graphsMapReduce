import argparse
from datetime import datetime

parser = argparse.ArgumentParser(description='Cleaner and tokenizer of raw text stored as json file')
parser.add_argument('-t', '--timestamp', nargs='*', type=int, help='timestamp')
args = parser.parse_args()

for timestamp in args.timestamp:
    print(datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S.%f'))