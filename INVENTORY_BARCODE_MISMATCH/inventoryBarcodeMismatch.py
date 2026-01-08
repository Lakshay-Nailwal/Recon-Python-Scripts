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
    SELECT
    t.ucode,
    t.batch,
    t.bin,
    t.net_qty,
    SUM(pi.available + pi.order_blocked) AS pi_net_qty,
    ABS(SUM(pi.available + pi.order_blocked) - t.net_qty) AS difference,
    CASE WHEN t.sources LIKE '%REPRINT%' THEN 'REPRINT' ELSE 'OTHER' END AS isReprintDone
FROM (
    SELECT
        ucode,
        batch,
        bin,
        SUM(qty_per_case) AS net_qty,
        GROUP_CONCAT(DISTINCT source) AS sources
    FROM barcode
    WHERE status = 'AVAILABLE'
    GROUP BY ucode, batch, bin
) t
JOIN product_inventory pi
    ON pi.code  = t.ucode
   AND pi.batch = t.batch
   AND pi.bin   = t.bin
GROUP BY
    t.ucode,
    t.batch,
    t.bin,
    t.net_qty,
    t.sources
HAVING
    t.net_qty <> SUM(pi.available + pi.order_blocked)

"""

def process_tenant(tenant):
    """Run SQL query for a tenant and save results"""
    try:
        print(f"🔹 Running query for tenant: {tenant}")
        db_connection = create_db_connection(tenant)
        cursor = db_connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(SQL_QUERY)
        results = cursor.fetchall()
        print(f"✅ Found {len(results)} results for tenant {tenant}")
        if results:
            for r in results:
                r["tenant"] = tenant
            safe_append_to_csv("inventory_barcode_mismatch_v4.csv", results)
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
    processAllTenants(tenants, max_workers=5)
