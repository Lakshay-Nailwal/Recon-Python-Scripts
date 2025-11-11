import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymysql
from threading import Lock
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from getDBConnection import create_db_connection
from csv_utils import append_to_csv
from getAllWarehouse import getAllWarehouse
from getAllArsenal import getAllArsenal
from whid import whid

# === CONFIG ===
CURRENT_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CSV_FILES")
CSV_LOCK = Lock()
MAX_WORKERS = 10
CSV_FILENAME = "product_lot_barcode_update_v8.csv"

# === SQL ===
SQL_QUERY = """
    SELECT vti.bar_code 
    FROM verifier_task vt 
    JOIN verifier_task_item vti ON vti.verifier_task_id = vt.id
    WHERE vt.reference_type = 'CONVERSION_TASK'
      AND vt.created_on >= "2025-11-03"
      AND vt.created_on <= "2025-11-06"
"""

# === HELPERS ===
def safe_append_to_csv(filename, rows):
    """Thread-safe CSV append."""
    with CSV_LOCK:
        append_to_csv(filename, rows, output_dir=CURRENT_DIRECTORY)

def getWhid(tenant):
    """Fetch whid for tenant from mapping."""
    return whid.get(tenant, "")

def validateBarcodeFamily(barcode_family, tenant):
    SQL_QUERY = """
    select * from product_lot_barcode where id = %s
    """
    conn = create_db_connection(tenant)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(SQL_QUERY, (barcode_family,))
    row = cursor.fetchone()
    return row

def getMaxBarcodeCount(barcode_family, tenant):
    SQL_QUERY = """
        SELECT vti.*
        FROM verifier_task_item vti
        WHERE vti.bar_code LIKE CONCAT(%s, '%%')
        ORDER BY LENGTH(vti.bar_code) DESC, vti.bar_code DESC
        LIMIT 1;
    """
    conn = create_db_connection(tenant)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(SQL_QUERY, (barcode_family,))
    row = cursor.fetchone()

    if not row or not row.get("bar_code"):
        return None  # default if no barcode found

    bar_code = row["bar_code"]

    # Extract suffix part after barcode_family
    suffix_str = bar_code[len(barcode_family):]

    try:
        suffix_int = int(suffix_str) if suffix_str else 0
    except ValueError:
        suffix_int = 0  # handle non-numeric suffix safely

    return suffix_int


# === CORE FUNCTION ===
def process_tenant(tenant):
    """Run SQL query for a single tenant and save results."""
    conn = None
    cursor = None

    try:
        conn = create_db_connection(tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(SQL_QUERY)
        rows = cursor.fetchall()

        print(f"✅ Found {len(rows)} rows for tenant {tenant}")

        if not rows:
            return

        processed_rows = []
        warehouse_id = getWhid(tenant)

        processed_family = []

        for row in rows:

            bar_code = row.get("bar_code")
            if not bar_code or not warehouse_id:
                continue

            row["tenant"] = tenant
            row["whid"] = warehouse_id
            row["barcode_family"] = bar_code[: 9 + max(2, len(warehouse_id))]

            if row["barcode_family"] in processed_family:
                continue
            processed_family.append(row["barcode_family"])

            barcodeFamilyRow = validateBarcodeFamily(row["barcode_family"], tenant)
            if not barcodeFamilyRow:
                continue

            maxBarcodeCount = getMaxBarcodeCount(row["barcode_family"], tenant)

            if maxBarcodeCount is None:
                continue

            if maxBarcodeCount <= barcodeFamilyRow["quantity"]:
                continue

            row["max_barcode_count"] = maxBarcodeCount
            row["current_barcode_count"] = barcodeFamilyRow["quantity"]

            safe_append_to_csv(CSV_FILENAME, [{"tenant": tenant, "whid": warehouse_id, "barcode_family": row["barcode_family"], "max_barcode_count": maxBarcodeCount, "current_barcode_count": barcodeFamilyRow["quantity"]}])

    except Exception as e:
        print(f"❌ Error running query for tenant {tenant}: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# === RUN ALL TENANTS ===
def processAllTenants(tenants, max_workers=MAX_WORKERS):
    """Run query for all tenants concurrently."""
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_tenant, tenant): tenant for tenant in tenants}
        for future in as_completed(futures):
            tenant = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"❌ Exception in tenant {tenant}: {e}")

# === ENTRY POINT ===
if __name__ == "__main__":
    tenants = ["th224"]
    start_time = datetime.now() - timedelta(hours=20)
    print(f"\n📦 Fetching data for last 20 hours (since {start_time.strftime('%Y-%m-%d %H:%M:%S')})")
    print(f"Processing {len(tenants)} tenants with {MAX_WORKERS} threads...\n")
    processAllTenants(tenants, max_workers=MAX_WORKERS)
    print("\n✅ Completed fetching and saving data.\n")