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

CURRENT_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CSV_FILES_V3")
CSV_LOCK = Lock()  # Thread-safe CSV writes
BATCH_SIZE = 500

def safe_append_to_csv(filename, rows):
    """Thread-safe CSV append"""
    with CSV_LOCK:
        append_to_csv(filename, rows, output_dir=CURRENT_DIRECTORY)

QUERY1 = "SELECT p.id, p.code, p.cgst, p.sgst, p.igst, pg.gst, p.updated_on, p.dp_updated_at FROM product p JOIN mercury.ucode_gst_temp pg ON p.code = pg.ucode WHERE  ( p.cgst <> pg.gst / 2 OR p.sgst <> pg.gst / 2 OR p.igst <> pg.gst );"

QUERY2 = "SELECT pl.id, pl.product_id, pl.mrp, ROUND( (pl.old_mrp / (1 + (p.old_gst / 100))) * (1 + ((p.cgst + p.sgst) / 100)), 2 ) AS expected_mrp, pl.updated_on FROM product_lot pl JOIN product p ON pl.product_id = p.id WHERE pl.mrp - ROUND( (pl.old_mrp / (1 + (p.old_gst / 100))) * (1 + ((p.cgst + p.sgst) / 100)), 2 ) > 1;"

QUERY3 = "SELECT pi.id, pi.product_id, pi.mrp, ROUND((pi.old_mrp / (1 + (p.old_gst / 100))) * (1 + ((p.cgst + p.sgst) / 100)), 2) AS expected_mrp, pi.updated_on, pi.dp_updated_at FROM product_inventory pi JOIN product p ON pi.product_id = p.id WHERE pi.mrp - ROUND((pi.old_mrp / (1 + (p.old_gst / 100))) * (1 + ((p.cgst + p.sgst) / 100)), 2) > 1;"

def process_tenant(tenant):
    """Run SQL query for a tenant and save results"""
    try:
        print(f"🔹 Running query for tenant: {tenant}")
        db_connection = create_db_connection(tenant)
        cursor = db_connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(QUERY1)
        results = cursor.fetchall()
        if results:
            for r in results:
                r["tenant"] = tenant
            safe_append_to_csv("product_mismatch_gst_v6.csv", results)
        cursor.execute(QUERY2)
        results = cursor.fetchall()
        if results:
            for r in results:
                r["tenant"] = tenant
            safe_append_to_csv("product_lot_mismatch_mrp_v6.csv", results)
        cursor.execute(QUERY3)
        results = cursor.fetchall()
        if results:
            for r in results:
                r["tenant"] = tenant
            safe_append_to_csv("product_inventory_mismatch_mrp_v6.csv", results)
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
