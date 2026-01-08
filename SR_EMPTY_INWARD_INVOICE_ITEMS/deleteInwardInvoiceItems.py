import requests
import time
import json
import pymysql
import sys
import os
import csv
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from getDBConnection import create_db_connection

INPUT_FILE = "/Users/lakshay.nailwal/Desktop/ReconScripts/SR_EMPTY_INWARD_INVOICE_ITEMS/CSV_FILES/srEmptyInwardInvoiceItems_v3.csv"
OUTPUT_CSV = "/Users/lakshay.nailwal/Desktop/ReconScripts/SR_EMPTY_INWARD_INVOICE_ITEMS/CSV_FILES/deleteInwardInvoiceItems_v3.csv"

# Thread-safe lock for CSV writing
csv_lock = threading.Lock()


def load_unique_rows():
    unique_set = set()
    unique_rows = []

    with open(INPUT_FILE, "r", newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row["invoice_id"], row["all_return_order_id"], row["tenant"])
            if key not in unique_set:
                unique_set.add(key)
                unique_rows.append(row)

    return unique_rows


def fetchRectificationInvoices(tenant, invoice_id):
    connection = create_db_connection(tenant)
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    cursor.execute(
        "SELECT * FROM inward_invoice WHERE id = %s AND purchase_type = 'RECTIFICATION'",
        (invoice_id,)
    )
    result = cursor.fetchone()

    cursor.close()
    connection.close()
    return result


def process_single_row(row, total_rows, index):
    tenant = row["tenant"]
    invoice_id = row["invoice_id"]
    order_id = row["all_return_order_id"]

    print(f"➡️ {index}/{total_rows} | Processing tenant={tenant} | order={order_id} | invoice={invoice_id}")

    rectificationInvoice = fetchRectificationInvoices(tenant, invoice_id)

    if rectificationInvoice is None:
        print(f"❌ No rectification invoice found for tenant {tenant} and invoice {invoice_id}")
        return None

    delete_query = (
        f"DELETE FROM {tenant}.inward_invoice_item "
        f"WHERE invoice_id = {rectificationInvoice['id']} "
        f"AND barcode = 'dummy';"
    )

    return (tenant, invoice_id, order_id, delete_query)


def process():
    rows = load_unique_rows()
    total = len(rows)
    print(f"Total unique rows to process: {total}")

    # Open CSV once for all threads
    with open(OUTPUT_CSV, "w", newline='', encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["tenant", "invoice_id", "order_id", "delete_query"])

        # ThreadPool with configurable worker count
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []

            for i, row in enumerate(rows, start=1):
                futures.append(
                    executor.submit(process_single_row, row, total, i)
                )

            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    with csv_lock:
                        writer.writerow(result)


if __name__ == "__main__":
    process()
