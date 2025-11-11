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
from enum import Enum


CURRENT_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CSV_FILES")
CSV_LOCK = Lock()  # Thread-safe CSV writes
BATCH_SIZE = 500

def safe_append_to_csv(filename, rows):
    """Thread-safe CSV append"""
    with CSV_LOCK:
        append_to_csv(filename, rows, output_dir=CURRENT_DIRECTORY)


SQL_QUERY = """
    SELECT pii.id , return_reason , pii.assigned_bin FROM purchase_issue_item pii
    JOIN purchase_issue pi ON pi.id = pii.purchase_issue_id
     WHERE pii.item_type IS NULL
    AND pi.status NOT IN ('cancelled', 'DELETED')
    AND (pi.debit_note_number IS NULL or pi.debit_note_number = '')
"""

class PrePurchaseIssueItemTypeEnum(Enum):
    DAMAGED = "DAMAGED"
    EXPIRED = "EXPIRED"
    SALEABLE = "SALEABLE"

def get_item_type(return_reason: str) -> PrePurchaseIssueItemTypeEnum:
    reason = return_reason.lower()
    if "damage" in reason:
        return PrePurchaseIssueItemTypeEnum.DAMAGED
    elif "expir" in reason:
        return PrePurchaseIssueItemTypeEnum.EXPIRED
    else:
        return PrePurchaseIssueItemTypeEnum.SALEABLE

def process_result(result , tenant):
    for row in result:
        item_type = get_item_type(row["return_reason"]).value
        update_query = f"UPDATE {tenant}.purchase_issue_item SET item_type = '{item_type}' , updated_on = NOW() WHERE id = {row['id']} and return_reason = '{row['return_reason']}' and item_type is null;"
        safe_append_to_csv("itemTypeNullHandlingV3.csv", {"tenant":tenant , "id":row["id"], "return_reason":row["return_reason"], "item_type":item_type, "assigned_bin":row["assigned_bin"], "update_query":update_query})

def process_tenant(tenant):
    """Run SQL query for a tenant and save results"""
    try:
        connection = create_db_connection(tenant)
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(SQL_QUERY)
        result = cursor.fetchall()
        connection.close()

        process_result(result, tenant)
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
