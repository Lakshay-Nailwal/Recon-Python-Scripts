import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymysql
from threading import Lock
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


def fetchPurchaseIssueItems(debit_note_number, tenant):
    conn = create_db_connection(tenant)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(
        """
        SELECT pii.* , pi.debit_note_number , pi.partner_detail_id , pi.status , pi.pr_type , pi.invoice_sequence_type 
        FROM purchase_issue_item pii 
        JOIN purchase_issue pi ON pi.id = pii.purchase_issue_id 
        WHERE pi.debit_note_number = %s
        """,
        (debit_note_number,),
    )
    purchase_issue_items = cursor.fetchall()
    cursor.close()
    conn.close()
    return purchase_issue_items


def analysePurchaseIssueItems(purchase_issue_items, tenant):
    ucodeBatchToMrpMap = {}
    for purchase_issue_item in purchase_issue_items:
        ucodeBatch = (purchase_issue_item["ucode"], purchase_issue_item["batch"])
        currentMrpInMap = ucodeBatchToMrpMap.get(ucodeBatch)
        if currentMrpInMap is None:
            ucodeBatchToMrpMap[ucodeBatch] = purchase_issue_item["mrp"]
        else:
            if currentMrpInMap != purchase_issue_item["mrp"]:
                purchase_issue_item["tenant"] = tenant
                purchase_issue_item["mrpUsedToCheck"] = currentMrpInMap
                safe_append_to_csv("sameUcodeDifferentMrpInPrItemsV2.csv", [purchase_issue_item])


def process_row(row):
    """Process a single CSV row"""
    try:
        tenant = row["source_tenant"]
        debit_note_number = row["source_debit_note_number"]
        purchase_issue_items = fetchPurchaseIssueItems(debit_note_number, tenant)
        if purchase_issue_items:
            analysePurchaseIssueItems(purchase_issue_items, tenant)
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
            future.result()  # Exceptions are printed inside process_row


if __name__ == "__main__":
    # Run for CSV in parallel
    process_csv_parallel("/Users/lakshay.nailwal/Desktop/ReconScripts/DC_CREATED_STR_NOT_CREATED/CSV_FILES/dcCreatedStrNotCreated.csv", max_workers=20)
