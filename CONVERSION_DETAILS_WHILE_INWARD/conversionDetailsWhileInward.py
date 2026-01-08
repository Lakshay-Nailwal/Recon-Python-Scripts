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
    select ct.id as conversion_task_id ,ct.reference_id , ct.reference_type , ct.status , ct.created_on , ct.tray_id , count(cti.id) as total_item , sum(cti.qty_per_case) as net_quantity from conversion_task ct join conversion_task_item_v2 cti on cti.conversion_task_id = ct.id where ct.reference_type like '%_ISSUE' and ct.status <> 'CANCELLED'
    and ct.created_on >= NOW() - INTERVAL 30 DAY
group by ct.id
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
        safe_append_to_csv("conversionDetailsWhileInward_v2.csv", results)

        print(f"✅ Found {len(results)} rows for tenant {tenant}")
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
