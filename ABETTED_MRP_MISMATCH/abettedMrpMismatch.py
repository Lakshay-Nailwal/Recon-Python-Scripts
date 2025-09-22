import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymysql
from threading import Lock

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from getDBConnection import create_db_connection
from csv_utils import append_to_csv, delete_csv
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
    SELECT p.code , pl.batch , pl.old_mrp , p.old_gst , pl.mrp as new_mrp , p.igst as new_gst
    FROM product p
    JOIN product_lot pl 
        ON pl.product_id = p.id
    WHERE CAST(pl.old_mrp AS DOUBLE) / (1 + CAST(p.old_gst AS DOUBLE) / 100)
            - CAST(pl.mrp AS DOUBLE) / (1 + CAST(p.igst AS DOUBLE) / 100) > 1;
"""

fileName = "abetted_mrp_mismatch.csv"

def process_tenant(tenant):
    """Run SQL query for a tenant and save results"""
    try:
        db_connection = create_db_connection(tenant)
        cursor = db_connection.cursor(pymysql.cursors.DictCursor)

        cursor.execute(SQL_QUERY)

        while True:
            rows = cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break
            rows = [dict(r, tenant=tenant) for r in rows]
            safe_append_to_csv(fileName, rows)

        print(f"✅ Processed tenant {tenant}")
    except Exception as e:
        print(f"❌ Error running query for tenant {tenant}: {e}")
    finally:
        try:
            cursor.close()
            db_connection.close()
        except:
            pass


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
