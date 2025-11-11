import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymysql
from pymysql.cursors import DictCursor
from threading import Lock

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from getDBConnection import create_db_connection
from csv_utils import append_to_csv, save_to_csv
from getAllWarehouse import getAllWarehouse
from getAllArsenal import getAllArsenal

CURRENT_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CSV_FILES")
CSV_LOCK = Lock()  # Thread-safe CSV writes
BATCH_SIZE = 500

def safe_append_to_csv(filename, rows):
    """Thread-safe CSV append"""
    with CSV_LOCK:
        append_to_csv(filename, rows, output_dir=CURRENT_DIRECTORY)


SQL_QUERY = """ 
WITH pr_agg AS (
    SELECT 
        pi.reference_debit_note_number,
        pii.ucode,
        pii.batch,
        SUM(pii.return_quantity) AS pr_quantity,
        SUM(pii.amount) AS pr_amount,
        GROUP_CONCAT(distinct pi.source_invoice_id) AS invoice_ids
    FROM purchase_issue pi
    JOIN purchase_issue_item pii 
        ON pii.purchase_issue_id = pi.id
    WHERE pi.status NOT IN ('dormant', 'pending', 'DELETED', 'cancelled')
      AND pi.invoice_date IS NOT NULL
      AND pi.invoice_date >= '2025-04-01'
      AND pi.pr_type <> 'REGULAR_EASYSOL'
      AND pi.reference_debit_note_number IS NOT NULL
    GROUP BY pi.reference_debit_note_number, pii.ucode, pii.batch
),
inward_agg AS (
    SELECT 
        ii.invoice_no AS reference_debit_note_number,
        iii.code AS ucode,
        iii.batch,
        SUM(iii.quantity) AS inward_quantity,
        SUM(iii.net_amount) AS inward_amount,
        GROUP_CONCAT(distinct iii.invoice_id) AS invoice_ids
    FROM inward_invoice ii
    JOIN inward_invoice_item iii 
        ON ii.id = iii.invoice_id
    WHERE ii.purchase_type IN ('ICSReturn', 'StockTransferReturn')
      AND ii.status NOT IN ('CANCELLED', 'DELETED')
    GROUP BY ii.invoice_no, iii.code, iii.batch
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
    pr.invoice_ids,
    ia.invoice_ids
FROM pr_agg pr
LEFT JOIN inward_agg ia
    ON pr.reference_debit_note_number = ia.reference_debit_note_number
    AND pr.ucode = ia.ucode
    AND pr.batch = ia.batch
WHERE ia.inward_quantity IS NULL
   OR pr.pr_quantity <> ia.inward_quantity
ORDER BY pr.reference_debit_note_number, pr.ucode, pr.batch;
"""

def process_tenant(tenant):
    """Run SQL query for a tenant and save results"""
    try:
        connection = create_db_connection(tenant)
        cursor = connection.cursor(DictCursor)
        cursor.execute(SQL_QUERY)
        result = cursor.fetchall()
        cursor.close()
        connection.close()
        if result:
            for r in result:
                r["tenant"] = tenant
            safe_append_to_csv(f"strB3WarehouseNegativeDeltaV6.csv", result)
        
    except Exception as e:
        print(f"❌ Error running query for tenant {tenant}: {e}")

def processAllTenants(tenants, max_workers=10):
    """Run query for all tenants concurrently"""
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_tenant, tenant): tenant for tenant in tenants}
        for future in as_completed(futures):
            tenant = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"❌ Exception in tenant {tenant}: {e}")


if __name__ == "__main__":
    tenants = ["th213" , "th400" , "th223" , "th437"]
    processAllTenants(tenants, max_workers=4)
