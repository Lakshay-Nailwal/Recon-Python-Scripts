import sys
import os
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import pymysql

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from getDBConnection import create_db_connection
from csv_utils import append_to_csv

# --------------------------- Config --------------------------- #
CURRENT_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CSV_FILES")
CSV_LOCK = Lock()  # Thread-safe CSV writes
VAULT_LOCK = Lock()  # Thread-safe vault cache
THREADS = 8  # Number of threads for parallel processing
OUTPUT_CSV = "updatePurchaseIssueItemsAndVaultV6.csv"

# --------------------------- Globals -------------------------- #
seenDebitNoteNumber = {}  # Cache for vault DC details

# --------------------------- Helpers ------------------------- #
def safe_append_to_csv(filename, rows):
    """Thread-safe CSV append"""
    with CSV_LOCK:
        append_to_csv(filename, rows, output_dir=CURRENT_DIRECTORY)

def fetchPrItems(referenceDebitNoteNumber, tenant, ucode, batch):
    """Fetch eligible PR items from tenant DB (new connection per call)"""
    conn = create_db_connection(tenant)
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        query = """
            SELECT 
                pi.id AS purchase_issue_id, 
                pii.id AS purchase_issue_item_id, 
                pii.ucode, 
                pii.batch, 
                pii.return_quantity, 
                pi.debit_note_number, 
                pi.invoice_sequence_type, 
                pi.status, 
                pii.amount,
                pi.partner_detail_id
            FROM purchase_issue_item pii 
            JOIN purchase_issue pi ON pi.id = pii.purchase_issue_id 
            WHERE pi.reference_debit_note_number = %s 
                AND pii.ucode = %s 
                AND pii.batch = %s 
                AND pi.status NOT IN ('cancelled', 'DELETED')
            ORDER BY (CASE WHEN pi.status = 'DELIVERED' THEN 1 ELSE 0 END)
        """
        cursor.execute(query, (referenceDebitNoteNumber, ucode, batch))
        result = cursor.fetchall()
        cursor.close()
        return result
    finally:
        conn.close()

def fetchDcDetailsFromVault(debitNoteNumber, tenant):
    """Fetch DC details from vault DB with caching (new connection per call)"""
    with VAULT_LOCK:
        if debitNoteNumber in seenDebitNoteNumber:
            return seenDebitNoteNumber[debitNoteNumber]

    conn = create_db_connection("vault")
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(
            "SELECT * FROM delivery_challan WHERE dc_number = %s AND tenant = %s", 
            (debitNoteNumber, tenant)
        )
        result = cursor.fetchone()
        cursor.close()
        with VAULT_LOCK:
            seenDebitNoteNumber[debitNoteNumber] = result
        return result
    finally:
        conn.close()

def process_row(row):
    """Process one CSV row and write updates to CSV with all eligiblePrItem fields and vault DC info"""
    try:

        referenceDebitNoteNumber = row["reference_debit_note_number"]
        tenant = row["tenant"]
        ucode = row["ucode"]
        batch = row["batch"]
        actualQuantity = int(row["inward_quantity"])

        eligiblePrItems = fetchPrItems(referenceDebitNoteNumber, tenant, ucode, batch)
        if not eligiblePrItems:
            return

        rows_to_write = []
        for eligiblePrItem in eligiblePrItems:
            maxQtyCovered = min(actualQuantity, eligiblePrItem["return_quantity"])
            updatePrItemQuantity = max(0, int(eligiblePrItem["return_quantity"]) - maxQtyCovered)

            row_to_write = dict(eligiblePrItem)  # all fields
            row_to_write["tenant"] = tenant
            row_to_write["update_quantity"] = updatePrItemQuantity

            dcDetails = fetchDcDetailsFromVault(eligiblePrItem["debit_note_number"], tenant)
            if dcDetails:
                row_to_write["dc_status_on_vault"] = dcDetails.get("status")
                row_to_write["dc_amount_on_vault"] = dcDetails.get("amount")
                row_to_write["dc_pending_amount_on_vault"] = dcDetails.get("pending_amount")
            else:
                row_to_write["dc_status_on_vault"] = None
                row_to_write["dc_amount_on_vault"] = None
                row_to_write["dc_pending_amount_on_vault"] = None

            rows_to_write.append(row_to_write)

            actualQuantity -= maxQtyCovered
            if actualQuantity <= 0:
                break

        if rows_to_write:
            safe_append_to_csv(OUTPUT_CSV, rows_to_write)

    except Exception as e:
        print(f"❌ Error processing row {row}: {e}")

def process_csv(filename):
    """Read CSV and process all rows"""
    with open(filename, newline="") as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)

    print(f"✅ Total rows: {len(rows)}")

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = [executor.submit(process_row, row) for row in rows]
        for future in as_completed(futures):
            future.result()  # raise exception if any

# --------------------------- Main ---------------------------- #
if __name__ == "__main__":
    input_csv = "/Users/lakshay.nailwal/Desktop/ReconScripts/BUG_FIXES_SCRIPTS/DUPLICATE_PR_FOR_STR/CSV_FILES/strB3WarehouseNegativeDeltaV6.csv"
    process_csv(input_csv)
    print(f"✅ Processing complete. Updates written to '{OUTPUT_CSV}'.")
