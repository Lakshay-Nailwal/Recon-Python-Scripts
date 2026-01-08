import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import pymysql

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from getDBConnection import create_db_connection
from csv_utils import append_to_csv
from getAllWarehouse import getAllWarehouse
from getAllArsenal import getAllArsenal


# ==============================
# CONFIG
# ==============================
BATCH_SIZE = 500
MAX_WORKERS = 10

CURRENT_DIRECTORY = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "CSV_FILES"
)

CSV_LOCK = Lock()


# ==============================
# INPUT PDIs
# ==============================
pdis = [
    216949,215943,209597,208301,208253,206526,205988,203825,202958,200382,
    199913,194465,194189,194005,193886,193852,193783,192168,186889,186200,
    182859,181847,173887,173545,167532,167339,167165,166989,166495,166244,
    165618,165337,165329,165138,163330,148638,142190,137652,133925,133777,
    127784,126622,126505,126151,125955,125948,120624,120110,114163,110518,
    110332,110325,109287,108885,107309,107221,107109,106257,106182,105091,
    104361,103851,103120,103103,102842,102155,101836,99856,98071,97759,
    96821,96575,93236,89828,89804,89782,89664,89158,89073,88745,88385,
    86456,86445,61486,60319,47049,37910,37527,36663,36619,36090,36084,
    35844,33248,32128,30242,30001,28441,27249,26935,26080,25895,25876,
    25553,23408,23179,22956,22943,22932,22233,21844,19815,19769,19761,
    19760,19377,19151,18678,18135,17297,13653,10801,10116,8601,8600,
    8596,8592,8591,8494,8477,8474,8414,8286,8247,8126,8124,8114,8060,
    8038,4039,3713,3580,3566,2775,2436,2072,1708,1261,1249,465,425,
    381,300,299,298,286,282,277,274,266,231,72,66,61,49,15,8660,
    8617,8526,8525,8312,8306,8107
]


# ==============================
# SQL
# ==============================
SQL_QUERY = """
    SELECT
        id AS purchase_issue_id,
        partner_detail_id,
        status,
        debit_note_number,
        invoice_date,
        pr_type,
        created_on,
        updated_on
    FROM purchase_issue
    WHERE partner_detail_id IN ({})
    AND created_on >= '2025-04-01';
"""


# ==============================
# UTILS
# ==============================
def chunk_list(data, size):
    """Yield successive chunks from list"""
    for i in range(0, len(data), size):
        yield data[i:i + size]


def safe_append_to_csv(filename, rows):
    """Thread-safe CSV write"""
    with CSV_LOCK:
        append_to_csv(filename, rows, output_dir=CURRENT_DIRECTORY)


# ==============================
# CORE LOGIC
# ==============================
def process_tenant(tenant, pdis):
    try:
        conn = create_db_connection(tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        for batch in chunk_list(pdis, BATCH_SIZE):
            placeholders = ",".join(["%s"] * len(batch))
            query = SQL_QUERY.format(placeholders)

            cursor.execute(query, batch)
            results = cursor.fetchall()

            if results:
                for row in results:
                    row["tenant"] = tenant

                safe_append_to_csv("purchase_issues_v2.csv", results)

        cursor.close()
        conn.close()

        print(f"✅ Completed tenant: {tenant}")

    except Exception as e:
        print(f"❌ Error for tenant {tenant}: {e}")


def processAllTenants(tenants, pdis, max_workers=MAX_WORKERS):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_tenant, tenant, pdis): tenant
            for tenant in tenants
        }

        for future in as_completed(futures):
            tenant = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"❌ Exception in tenant {tenant}: {e}")


# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    tenants = getAllWarehouse() + getAllArsenal()
    print(f"🚀 Processing {len(tenants)} tenants...")
    processAllTenants(tenants, pdis)
    print("🎯 All tenants processed")
