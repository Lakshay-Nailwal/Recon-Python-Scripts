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
import pandas as pd

CURRENT_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CSV_FILES")
CSV_LOCK = Lock()  # Thread-safe CSV writes
BATCH_SIZE = 500

def safe_append_to_csv(filename, rows):
    """Thread-safe CSV append"""
    with CSV_LOCK:
        append_to_csv(filename, rows, output_dir=CURRENT_DIRECTORY)


def process_tenant(tenant):
    """Run SQL query for a tenant and save results"""
    try:
        pass
    except Exception as e:
        print(f"❌ Error running query for tenant {tenant}: {e}")

def execute_query(query , tenant, filename):
    """Execute query"""
    conn = create_db_connection(tenant)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    rows = cursor.fetchall()
    safe_append_to_csv(filename, rows)
    cursor.close()
    conn.close()

def getBackUp(tenant , delivery_challan_number):
    q1 = f"""
        SELECT * FROM delivery_challan WHERE dc_number = '{delivery_challan_number}'
    """
    q2 = f"""
        SELECT * FROM delivery_challan_details dcd WHERE dcd.delivery_challan_number = '{delivery_challan_number}'
    """
    q3 = f"""
        SELECT dctrt.* FROM delivery_challan_tax_rates dctrt JOIN delivery_challan_details dcd ON dctrt.deliver_challan_details_id = dcd.id
         WHERE dcd.delivery_challan_number = '{delivery_challan_number}'
    """
    execute_query(q1,tenant, "backup_queries_delivery_challan.csv")
    execute_query(q2,tenant, "backup_queries_delivery_challan_details.csv")
    execute_query(q3,tenant, "backup_queries_delivery_challan_tax_rates.csv")


def process_csv():
    """Process CSV file"""
    df = pd.read_csv("ARSENAL_PR_IS_DCN/CSV_FILES/deliveryChallanNormalInArsenalPRV2.csv")

    for index, row in df.iterrows():
        tenant = row["tenant"]
        delivery_challan_number = row["debit_note_number"]
        delete_query_1 = f"DELETE FROM {tenant}.delivery_challan WHERE dc_number = '{delivery_challan_number}';\n"
        delete_query_2 = f"DELETE FROM {tenant}.delivery_challan_details dcd WHERE dcd.delivery_challan_number = '{delivery_challan_number}';\n"
        delete_query_3 = f"DELETE FROM {tenant}.delivery_challan_tax_rates dctrt JOIN delivery_challan_details dcd ON dctrt.deliver_challan_details_id = dcd.id WHERE dcd.delivery_challan_number = '{delivery_challan_number}';\n"
        safe_append_to_csv("delete_queries.sql", [delete_query_3, delete_query_2, delete_query_1])
    
        getBackUp(tenant , delivery_challan_number)


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
    # processAllTenants(tenants, max_workers=10)
