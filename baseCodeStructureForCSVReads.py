import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymysql
from threading import Lock
from collections import defaultdict
import csv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from getDBConnection import create_db_connection
from csv_utils import append_to_csv
from getAllWarehouse import getAllWarehouse
from getAllArsenal import getAllArsenal

CURRENT_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CSV_FILES")
CSV_LOCK = Lock()  # Thread-safe CSV writes
BATCH_SIZE = 500

def safe_append_to_csv(filename, rows):
    """Thread-safe CSV append"""
    with CSV_LOCK:
        append_to_csv(filename, rows, output_dir=CURRENT_DIRECTORY)

 def process_row(row):
        try:
            pass
        except Exception as e:
            print(f"❌ Error processing row {row}: {e}")

def process_csv_parallel(filename, max_workers=10):
    """Process CSV rows in parallel"""
    rows = []
    try:
        with open(filename, newline="") as infile:
            reader = csv.DictReader(infile)
            rows = list(reader)  # Read all rows first
    except Exception as e:
        print(f"❌ Error reading CSV {filename}: {e}")
        return

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_row, row) for row in rows]
        for future in as_completed(futures):
            # Wait for all tasks to complete, exceptions are already printed inside process_row
            future.result()



if __name__ == "__main__":
    filename = ""
    process_csv_parallel(filename)
