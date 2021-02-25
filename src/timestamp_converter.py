import argparse
from datetime import datetime
import time
# time.gmtime(1346114717972/1000.)

parser = argparse.ArgumentParser(description='Cleaner and tokenizer of raw text stored as json file')
parser.add_argument('-t', '--timestamp', nargs='*', type=int, help='timestamp')
args = parser.parse_args()

# if you encounter a "year is out of range" error the timestamp
# may be in milliseconds, try `ts /= 1000` in that case
for timestamp in args.timestamp:
    print(datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S'))