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
select tt.task_id as verifier_task_id , tt.tray_id , tt.updated_on from task_tray tt 
LEFT JOIN verifier_task vt on vt.id = tt.task_id 
where tt.status = 'IN_USE' and tt.task_type = 'VERIFIER_TASK' 
and vt.id is null
and tt.updated_on <= NOW() - INTERVAL 18 hour
and tt.updated_on >= NOW() - INTERVAL 30 day
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
            result["free_tray"] = f"UPDATE {tenant}.task_tray SET updated_on = NOW() , status = 'FREE' , task_id = NULL , task_type = NULL WHERE tray_id = '{result['tray_id']}' and task_id = {result['verifier_task_id']} and status = 'IN_USE' and task_type = 'VERIFIER_TASK';"

        print(f"✅ Found {len(results)} rows for tenant {tenant}")
        safe_append_to_csv("freeTrayForVtStuck_v2.csv", results)

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
