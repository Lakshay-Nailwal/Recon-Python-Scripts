import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymysql
from threading import Lock
import csv

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


def fetchIdempotentDigest(invoice_no, tenant):
    conn = create_db_connection("mercury")
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SELECT * FROM idempotent_digest WHERE metadata = %s", (invoice_no + " + " + tenant,))
    return cursor.fetchone()


def process_csv():
    """Run SQL query for a tenant and save results"""
    try:
        with open("/Users/lakshay.nailwal/Desktop/ReconScripts/DELETE_STR_FROM_DESTINATION/CSV_FILES/input.csv", "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                print("dest_tenant : ", row["dest_tenant"], "invoice_no : ", row["invoice_no"], "source_tenant : ", row["source_tenant"])
                idempotentDigest = fetchIdempotentDigest(row["invoice_no"], row["source_tenant"])
                if idempotentDigest:
                    deleteDigestQuery = f"DELETE FROM mercury.idempotent_digest WHERE id = '{idempotentDigest['id']}';"
                    idempotentDigest["deleteDigestQuery"] = deleteDigestQuery
                    safe_append_to_csv("deleteIdempotentDigestBackUpForSTR.csv", idempotentDigest)

    except Exception as e:
        print(f"❌ Error running query: {e}")


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
    process_csv()
    # tenants = getAllWarehouse() + getAllArsenal()
    # processAllTenants(tenants, max_workers=10)
