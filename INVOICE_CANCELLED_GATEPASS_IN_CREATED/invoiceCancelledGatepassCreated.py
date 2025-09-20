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
    SELECT ii.id as inward_invoice_id , ii.invoice_no , ii.created_on , ii.status 
    , gi.status as gatepass_status , gi.id as gatepass_invoice_id , gi.gatepass_id , ii.total as inward_invoice_total
    FROM inward_invoice ii join 
    gatepass_invoice gi on gi.no = ii.invoice_no and gi.gatepass_id = ii.gatepass_id
    WHERE gi.status <> 'CANCELLED'
    AND ii.status IN ('CANCELLED' , 'DELETED')
    AND ii.created_on >= '2025-05-27'
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
        results = cursor.fetchall()
        if results:
            for result in results:
                result["tenant"] = tenant
                cancelled_query = (
                    f"UPDATE {tenant}.gatepass_invoice SET status = 'CANCELLED' "
                    f"WHERE id = {result['gatepass_invoice_id']} and no = '{result['invoice_no']}' "
                    f"and status = '{result['gatepass_status']}';"
                )
                result["cancelled_query"] = cancelled_query
                safe_append_to_csv("invoiceCancelledGatepassCreated.csv", result)
        cursor.close()
        conn.close()
        print(f"✅ Finished tenant: {tenant} ({len(results)} rows)")
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
    tenants = getAllWarehouse() + getAllArsenal()
    processAllTenants(tenants, max_workers=10)
