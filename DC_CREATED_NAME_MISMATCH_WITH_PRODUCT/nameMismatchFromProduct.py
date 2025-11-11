import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymysql
from threading import Lock

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
    SELECT distinct ii.invoice_no, ii.status, ii.id , ii.purchase_type , iii.code , iii.name , pd.name as product_name
    FROM inward_invoice ii
    JOIN inward_invoice_item iii ON iii.invoice_id = ii.id
    JOIN product pd on pd.code = iii.code
    AND ii.created_on >= '2025-09-24'
    AND pd.name <> iii.name
"""

def process_tenant(tenant):
    """Run SQL query for a tenant and save results"""
    try:
        conn = create_db_connection(tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(SQL_QUERY)
        results = cursor.fetchall()
        for row in results:
            row["tenant"] = tenant
            safe_append_to_csv("nameMismatchFromProductV7.csv", row)
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