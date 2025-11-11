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

SQL_QUERY = """
    SELECT pi.debit_note_number , pi.partner_detail_id , pi.status , pi.pr_type, pi.invoice_sequence_type FROM purchase_issue pi 
    LEFT JOIN purchase_issue_item pii ON pii.purchase_issue_id = pi.id
    WHERE debit_note_number IS NOT NULL
    AND pi.status NOT IN ('cancelled', 'DELETED')
    AND pi.pr_type <> 'REGULAR_EASYSOL'
    AND pi.invoice_date >= '2025-05-28'
    AND pii.id is null
    GROUP BY pi.debit_note_number
"""

def process_result(result, tenant):
    for row in result:
        row["tenant"] = tenant
        safe_append_to_csv("dcWIthZeroItemsInPRV2.csv", row)

def process_tenant(tenant):
    """Run SQL query for a tenant and save results"""
    try:
        conn = create_db_connection(tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(SQL_QUERY)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        process_result(result, tenant)
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
    tenants = getAllWarehouse() + getAllArsenal()
    processAllTenants(tenants, max_workers=10)
