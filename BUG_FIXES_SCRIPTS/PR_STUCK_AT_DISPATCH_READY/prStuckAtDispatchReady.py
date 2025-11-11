import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymysql
from threading import Lock
from collections import defaultdict
import csv

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from getDBConnection import create_db_connection
from csv_utils import append_to_csv
from getAllWarehouse import getAllWarehouse
from getAllArsenal import getAllArsenal

CURRENT_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CSV_FILES")
CSV_LOCK = Lock()  # Thread-safe CSV writes
BATCH_SIZE = 500

def safe_append_to_csv(filename, rows):
    """Thread-safe CSV append"""
    # with CSV_LOCK:
    append_to_csv(filename, rows, output_dir=CURRENT_DIRECTORY)


def getChildPdisForReferenceDebitNoteNumbers(tenant, referenceDebitNoteNumbers):
    try:
        conn = create_db_connection(tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        placeholders = ','.join(['%s'] * len(referenceDebitNoteNumbers))
        cursor.execute(f"SELECT distinct debit_note_number , child_tenant_partner_detail_id FROM purchase_issue WHERE debit_note_number IN ({placeholders})", tuple(referenceDebitNoteNumbers))
        return cursor.fetchall()
    except Exception as e:
        print(f"❌ Error fetching child PDIS for tenant {tenant}: {e}")
        return []

def process_csv(filename):
    """Process CSV rows in parallel"""
    rows = []
    referenceDebitNoteNumberForTenant = defaultdict(list)
    referenceDebitNoteNumberToDestTenantMap = {}
    try:
        with open(filename, newline="") as infile:
            reader = csv.DictReader(infile)
            rows = list(reader)  # Read all rows first
        for row in rows:
            referenceDebitNoteNumberForTenant[row["source_tenant"]].append(row["reference_debit_note_number"])
            referenceDebitNoteNumberToDestTenantMap[row["reference_debit_note_number"]] = row["dest_tenant"]
    except Exception as e:
        print(f"❌ Error reading CSV {filename}: {e}")
        return

    for tenant, referenceDebitNoteNumbers in referenceDebitNoteNumberForTenant.items():
        childPdisForReferenceDebitNoteNumbers = getChildPdisForReferenceDebitNoteNumbers(tenant, referenceDebitNoteNumbers)
        referenceDebitNoteNumberToChildPdiMap = {}
        for row in childPdisForReferenceDebitNoteNumbers:
            referenceDebitNoteNumberToChildPdiMap[row["debit_note_number"]] = row["child_tenant_partner_detail_id"]
            safe_append_to_csv("prStuckAtDispatchReadyWithChildPdi.csv", {"source_tenant": tenant, "reference_debit_note_number": row["debit_note_number"], "dest_tenant": referenceDebitNoteNumberToDestTenantMap[row["debit_note_number"]], "child_tenant_partner_detail_id": row["child_tenant_partner_detail_id"]})
    
if __name__ == "__main__":
    filename = "/Users/lakshay.nailwal/Desktop/ReconScripts/BUG_FIXES_SCRIPTS/PR_STUCK_AT_DISPATCH_READY/CSV_FILES/B3_Cases.csv"
    process_csv(filename)
