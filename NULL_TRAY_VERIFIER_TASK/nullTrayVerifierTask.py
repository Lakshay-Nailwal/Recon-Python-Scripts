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
SELECT vt.id , ct.tray_id , vt.status , ct.reference_type , ct.id as conversion_task_id
FROM verifier_task vt
JOIN conversion_task ct ON ct.id = vt.reference_id and vt.reference_type = 'CONVERSION_TASK'
WHERE vt.tray_id IS NULL and ct.reference_type like '%_ISSUE'
"""

def process_tenant(tenant):
    """Run SQL query for a tenant and save results"""
    try:
        conn = create_db_connection(tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(SQL_QUERY)
        results = cursor.fetchall()
        cursor.close()
        conn.close()

        if(len(results) == 0):
            return

        for result in results:
            result["tenant"] = tenant
            result["update_VerifierTask"] = f"UPDATE {tenant}.verifier_task SET tray_id = '{result['tray_id']}' , updated_on = NOW() WHERE id = {result['id']} and tray_id is null;"
            result["update_taskTray"] = f"UPDATE {tenant}.task_tray SET updated_on = NOW() , task_type = 'VERIFIER_TASK' , status = 'IN_USE' , task_id = {result['id']} WHERE tray_id = '{result['tray_id']}' and task_id is NULL and status = 'FREE' and task_type is NULL;"

        print(f"✅ Found {len(results)} rows for tenant {tenant}")
        safe_append_to_csv("nullTrayVerifierTask_v3.csv", results)

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
