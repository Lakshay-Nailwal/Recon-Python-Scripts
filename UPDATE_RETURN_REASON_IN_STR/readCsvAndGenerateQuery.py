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
CHUNK_SIZE = 3000

def safe_append_to_csv(filename, rows):
    """Thread-safe CSV append"""
    with CSV_LOCK:
        append_to_csv(filename, rows, output_dir=CURRENT_DIRECTORY)

def process_row(row):
    try:
        pass
    except Exception as e:
        print(f"❌ Error processing row {row}: {e}")

def process_csv(filename):
    tenantReasonMap = defaultdict(list)
    tenantToInvoiceNoMap = defaultdict(list)
    duplicateInvoiceNoMap = {}
    with open(filename, newline="") as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            if (row["invoice_no"] == ""):
                continue
            tenantReasonMap[(row["tenant"] , row["prReason"])].append(row['item_id'])
            if (row["invoice_no"] in duplicateInvoiceNoMap):
                continue
            duplicateInvoiceNoMap[row["invoice_no"]] = True
            tenantToInvoiceNoMap[row["tenant"]].append(row['invoice_no'])

    for (tenant, reason), item_ids in tenantReasonMap.items():
        for i in range(0, len(item_ids), CHUNK_SIZE):
            chunk = item_ids[i:i+CHUNK_SIZE]
            updateQuery = f"UPDATE {tenant}.inward_invoice_item SET return_reason = '{reason}', updated_on = NOW() WHERE id IN ({','.join(map(str, chunk))}) AND return_reason IS NULL;"
            safe_append_to_csv("updateReturnReasonInStrV2.sql", [updateQuery])

    for tenant, invoiceNos in tenantToInvoiceNoMap.items():
        for i in range(0, len(invoiceNos), CHUNK_SIZE):
            chunk = invoiceNos[i:i+CHUNK_SIZE]
            ids_str = ",".join(f"'{x}'" for x in chunk)
            updateQuery = f"UPDATE {tenant}.inward_invoice SET status = 'StockHidden', updated_on = NOW() WHERE invoice_no IN ({ids_str}) AND status IN ('StockHidden-qty-verified');"
            safe_append_to_csv("updateStockHiddenStatusV2.sql", [updateQuery])


if __name__ == "__main__":

    filename = "/Users/lakshay.nailwal/Desktop/ReconScripts/UPDATE_RETURN_REASON_IN_STR/CSV_FILES/updateReturnReasonInStrV19.csv"
    process_csv(filename)
