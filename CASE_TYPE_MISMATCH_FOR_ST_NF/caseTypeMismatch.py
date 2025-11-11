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

# childWh    parentWh
# th999    th998
# th411    th402
# th427    th224
# th435    th214
# th410    th402
# th428    th224
# th418    th214

mapping =  {
    "th999": "th998",
    "th411": "th402",
    "th427": "th224",
    "th435": "th214",
    "th410": "th402",
    "th428": "th224",
    "th418": "th214",
}

def process_result(result, childWh, parentWh):
    """Process the result"""
    # with CSV_LOCK:
    for row in result:
        row["child_wh"] = childWh
        row["parent_wh"] = parentWh
    append_to_csv(f"case_type_mismatch_for_st_nf.csv", result, output_dir=CURRENT_DIRECTORY)


def process_tenant(tenant):
    """Run SQL query for a tenant and save results"""
    try:

        for childWh, parentWh in mapping.items():
            SQL_QUERY = f"""
                select * from {childWh}.invoice_barcode ib inner join {childWh}.barcode b on b.barcode=ib.barcode
                inner join {parentWh}.stock_transfer_scanned_item sc on sc.bar_code=ib.barcode
                where sc.case_type != b.case_type;
            """
            conn = create_db_connection(tenant)
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(SQL_QUERY)
            result = cursor.fetchall()
            cursor.close()
            conn.close()
            process_result(result, childWh , parentWh)
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
    # tenants = getAllWarehouse() + getAllArsenal()
    # processAllTenants(tenants, max_workers=10)
    process_tenant("th435")
