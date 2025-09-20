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

SQL_QUERY = """
    SELECT distinct pi.debit_note_number
    FROM purchase_issue pi
    JOIN purchase_issue_item pii ON pii.purchase_issue_id = pi.id
    JOIN delivery_challan dc ON dc.dc_number = pi.debit_note_number
    WHERE pi.invoice_date >= '2025-08-25'
"""

def safe_append_to_csv(filename, rows):
    """Thread-safe CSV append"""
    with CSV_LOCK:
        append_to_csv(filename, rows, output_dir=CURRENT_DIRECTORY)


def runQuery(tenant):
    """Run SQL query for a tenant and save results"""
    try:
        print(f"🔹 Running query for tenant: {tenant}")
        conn = create_db_connection(tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(SQL_QUERY)
        result = cursor.fetchall()
        if result:
            for r in result:
                r["tenant"] = tenant
            safe_append_to_csv("deliveryChallanNormalInArsenalPRV2.csv", result)
        cursor.close()
        conn.close()
        print(f"✅ Finished tenant: {tenant} ({len(result)} rows)")
    except Exception as e:
        print(f"❌ Error running query for tenant {tenant}: {e}")


def processAllTenants(tenants, max_workers=10):
    """Run query for all tenants concurrently"""
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(runQuery, tenant): tenant for tenant in tenants}
        for future in as_completed(futures):
            tenant = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"❌ Exception in tenant {tenant}: {e}")


if __name__ == "__main__":
    tenants = getAllArsenal()
    processAllTenants(tenants, max_workers=10)
