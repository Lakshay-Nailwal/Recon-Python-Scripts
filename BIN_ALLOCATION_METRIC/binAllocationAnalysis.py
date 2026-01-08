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
QUERY_CHUNK_SIZE = 100

def safe_append_to_csv(filename, rows):
    with CSV_LOCK:
        append_to_csv(filename, rows, output_dir=CURRENT_DIRECTORY)

BIN_TYPES = [
    "INVOICE"
]

SQL_QUERY = """
SELECT request_payload, response_payload, type, bin_allocation_id, bin_allocation_type
FROM bin_allocation_metric
WHERE created_on >= '2025-11-29 00:00:00'
  AND created_on <= '2025-11-30 00:00:00'
  AND bin_allocation_type = %s
ORDER BY id
"""

RETRY_ERRORS = {2013, 2006, 2055}  # Lost conn + server gone

def execute_with_retry(cursor, query, args, retries=3, delay=2):
    for attempt in range(1, retries + 1):
        try:
            cursor.execute(query, args)
            return
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

        ucodeBatchList = set()

        # Qty logic
        if record_type == "PRIMARY":
            resp_data = resp.get("data", [])
            for item in resp_data:
                ucodeBatchList.add(item.get("ucode"))
        else:
            cb = req.get("codeBatch", [])
            for item in cb:
                ucodeBatchList.add(item.get("ucode"))

        return len(ucodeBatchList), error_primary

    except Exception as e:
        return None, f"Parsing error: {e}"

def process_bin_type(tenant, alloc_type):
    print(f"🔹 Processing {alloc_type} for {tenant}...")

    try:
        conn = create_db_connection(tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        # Execute query once
        execute_with_retry(SQL_QUERY, (alloc_type,))

        while True:
            # Fetch chunk of 100 rows
            result_chunk = cursor.fetchmany(QUERY_CHUNK_SIZE)

            if not result_chunk:
                break

            rows = []

            for r in result_chunk:
                total_ucode, error_primary = extract_details(
                    r["request_payload"], r["type"], r["response_payload"]
                )

                rows.append({
                    "tenant": tenant,
                    "type": r["type"],
                    "bin_allocation_id": r["bin_allocation_id"],
                    "bin_allocation_type": r["bin_allocation_type"],
                    "total_ucode": total_ucode,
                    "error_message_primary": error_primary
                })

            safe_append_to_csv("bin_allocation_analysis.csv", rows)

        cursor.close()
        conn.close()

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
