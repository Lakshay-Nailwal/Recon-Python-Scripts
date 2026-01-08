import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymysql
import json
from threading import Lock
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from getDBConnection import create_db_connection
from csv_utils import append_to_csv

CURRENT_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CSV_FILES")
CSV_LOCK = Lock()

def safe_append_to_csv(filename, rows):
    with CSV_LOCK:
        append_to_csv(filename, rows, output_dir=CURRENT_DIRECTORY)


BIN_TYPES = [
    "CONVERSION_TASK",
    "INVOICE",
    "ITEM_FOUND_TASK",
    "JIT_BIN_RACKER_TASK",
    "PICKER_TASK",
    "RECTIFICATION",
    "VERIFIER_ISSUE",
    "VERIFIER_TASK"
]

SQL_QUERY = """
SELECT request_payload, response_payload, type, bin_allocation_id, bin_allocation_type
FROM bin_allocation_metric
WHERE created_on >= '2025-11-29 00:00:00'
  AND created_on <= '2025-11-30 00:00:00'
  AND bin_allocation_type = %s
"""

RETRY_ERRORS = {2013, 2006, 2055}  # Lost conn + server gone

def execute_with_retry(cursor, query, args, retries=3, delay=2):
    for attempt in range(1, retries + 1):
        try:
            cursor.execute(query, args)
            return cursor.fetchall()
        except pymysql.err.OperationalError as e:
            if e.args[0] in RETRY_ERRORS:
                print(f"⚠️ MySQL lost connection. Retrying {attempt}/{retries}...")
                time.sleep(delay)
                continue
            raise e
        except Exception:
            print(f"⚠️ Query failed, retrying {attempt}/{retries}...")
            time.sleep(delay)
            continue
    raise Exception("❌ Failed query after retries")


def extract_details(request_payload_str, record_type, response_payload_str):
    try:
        req = json.loads(request_payload_str)
        resp = json.loads(response_payload_str)

        error_primary = req.get("errorMessageFromPrimary")

        # Qty logic
        if record_type == "PRIMARY":
            resp_data = resp.get("data", [])
            total_qty = sum(item.get("qty", 0) for item in resp_data)
        else:
            cb = req.get("codeBatch", [])
            total_qty = sum(item.get("quantity", 0) for item in cb)

        return total_qty, error_primary

    except Exception as e:
        return 0, f"Parsing error: {e}"


def process_bin_type(tenant, alloc_type):
    print(f"🔹 Processing {alloc_type} for {tenant}...")

    rows = []

    try:
        conn = create_db_connection(tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        results = execute_with_retry(cursor, SQL_QUERY, (alloc_type,))

        cursor.close()
        conn.close()

        for r in results:
            total_qty, error_primary = extract_details(
                r["request_payload"], r["type"], r["response_payload"]
            )

            rows.append({
                "tenant": tenant,
                "type": r["type"],
                "bin_allocation_id": r["bin_allocation_id"],
                "bin_allocation_type": r["bin_allocation_type"],
                "total_quantity": total_qty,
                "error_message_primary": error_primary
            })

        # Write immediately for this type
        safe_append_to_csv("bin_allocation_metric_summary_v3.csv", rows)

    except Exception as e:
        print(f"❌ Error processing {alloc_type} for {tenant}: {e}")


def process_tenant(tenant):
    print(f"\n================= Processing Tenant: {tenant} =================\n")

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(process_bin_type, tenant, t) for t in BIN_TYPES]

        for f in as_completed(futures):
            f.result()


def processAllTenants(tenants, max_workers=10):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_tenant, tenant): tenant for tenant in tenants}

        for future in as_completed(futures):
            future.result()


if __name__ == "__main__":
    tenants = ["th214"]
    processAllTenants(tenants)
