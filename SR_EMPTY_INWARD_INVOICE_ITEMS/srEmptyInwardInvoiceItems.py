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

SQL_QUERY = """
SELECT ari.ucode,
       ari.batch,
       ari.all_return_order_id,
       ii.id as invoice_id,
       ii.status as invoice_status
FROM all_return_item ari
JOIN all_return_order aro
    ON aro.id = ari.all_return_order_id
JOIN inward_invoice ii
    ON ii.invoice_no = CONCAT(ari.all_return_order_id, '-SR') AND purchase_type = 'SALES_RETURN'
LEFT JOIN inward_invoice_item iii
    ON iii.invoice_id = ii.id
   AND iii.code = ari.ucode
   AND iii.batch = ari.batch
WHERE aro.updated_on >= '2025-11-19'
  AND aro.updated_on <=  '2025-11-27'
  AND ari.status = 'ACCEPTED'
  AND ari.barcode IS NULL
  AND iii.id IS NULL;
"""

def safe_append_to_csv(filename, rows):
    """Thread-safe CSV append"""
    with CSV_LOCK:
        append_to_csv(filename, rows, output_dir=CURRENT_DIRECTORY)


def process_tenant(tenant):
    """Run SQL query for a tenant and save results"""
    try:
        connection = create_db_connection(tenant)
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(SQL_QUERY)
        result = cursor.fetchall()
        cursor.close()
        connection.close()

        if len(result) == 0:
            return

        for row in result:
            row["tenant"] = tenant
        safe_append_to_csv("srEmptyInwardInvoiceItems_v8.csv", result)
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
