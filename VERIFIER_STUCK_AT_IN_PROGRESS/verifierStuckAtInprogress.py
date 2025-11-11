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
    SELECT distinct vt.id , vt.status , vt.reference_type , vt.reference_id , vt.source_type , vt.source_id , vti.status as item_status , vti.ucode , vti.batch_number , vti.case_id , vti.case_type FROM verifier_task vt 
    JOIN verifier_task_item vti on vti.verifier_task_id = vt.id
    WHERE vt.status = 'IN_PROGRESS' AND vt.updated_on >= '2025-10-27' AND vt.updated_on <= '2025-10-30'
"""

def process_tenant(tenant):
    """Run SQL query for a tenant and save results"""
    try:
        conn = create_db_connection(tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(SQL_QUERY)
        rows = cursor.fetchall()
        print(f"Found {len(rows)} rows for tenant {tenant}")
        if(rows):
            for row in rows:
                row["tenant"] = tenant
            safe_append_to_csv("verifier_task_v5.csv", rows)
        conn.close()
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
    tenants = getAllWarehouse()
    processAllTenants(tenants, max_workers=10)