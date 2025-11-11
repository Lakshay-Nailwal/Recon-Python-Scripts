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

def checkForDuplicate(tenant, referenceDebitNoteNumbers):
    try:
        conn = create_db_connection(tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        placeholders = ','.join(['%s'] * len(referenceDebitNoteNumbers))
        cursor.execute(
            f""" 
WITH pr_agg AS (
    SELECT 
        pi.reference_debit_note_number,
        pii.ucode,
        pii.batch,
        SUM(pii.return_quantity) AS pr_quantity,
        SUM(pii.amount) AS pr_amount
    FROM purchase_issue pi
    JOIN purchase_issue_item pii 
        ON pii.purchase_issue_id = pi.id
    WHERE pi.status NOT IN ('dormant', 'pending', 'DELETED', 'cancelled')
      AND pi.invoice_date IS NOT NULL
      AND pi.invoice_date >= '2025-04-01'
      AND pi.pr_type <> 'REGULAR_EASYSOL'
      AND pi.reference_debit_note_number IN ({placeholders})
    GROUP BY pi.reference_debit_note_number, pii.ucode, pii.batch
),
inward_agg AS (
    SELECT 
        ii.id AS source_invoice_id,
        ii.invoice_no AS reference_debit_note_number,
        iii.code AS ucode,
        iii.batch,
        SUM(iii.quantity) AS inward_quantity,
        SUM(iii.net_amount) AS inward_amount
    FROM inward_invoice ii
    JOIN inward_invoice_item iii 
        ON ii.id = iii.invoice_id
    WHERE ii.purchase_type IN ('ICSReturn', 'StockTransferReturn')
      AND ii.status NOT IN ('CANCELLED', 'DELETED')
    GROUP BY ii.id, ii.invoice_no, iii.code, iii.batch
)
SELECT 
    pr.reference_debit_note_number,
    pr.ucode,
    pr.batch,
    pr.pr_quantity,
    ia.inward_quantity,
    COALESCE(pr.pr_quantity - ia.inward_quantity, pr.pr_quantity) AS delta_quantity,
    ia.inward_amount,
    pr.pr_amount,
    CASE 
        WHEN ia.source_invoice_id IS NULL THEN 1
        ELSE 0
    END AS invoice_deleted
FROM pr_agg pr
LEFT JOIN inward_agg ia
    ON pr.reference_debit_note_number = ia.reference_debit_note_number
    AND pr.ucode = ia.ucode
    AND pr.batch = ia.batch
WHERE ia.inward_quantity IS NULL
   OR pr.pr_quantity <> ia.inward_quantity
ORDER BY pr.reference_debit_note_number, pr.ucode, pr.batch;
            """, tuple(referenceDebitNoteNumbers)
        )
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
        result = checkForDuplicate(tenant, referenceDebitNoteNumbers)
        if result:
            safe_append_to_csv("checkForDuplicate.csv", result)
        
if __name__ == "__main__":
    filename = "/Users/lakshay.nailwal/Desktop/ReconScripts/BUG_FIXES_SCRIPTS/PR_STUCK_AT_DISPATCH_READY/CSV_FILES/B3_Cases.csv"
    process_csv(filename)
