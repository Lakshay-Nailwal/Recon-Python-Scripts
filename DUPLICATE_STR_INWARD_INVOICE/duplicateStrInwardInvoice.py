import sys
import os
import pymysql
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from getDBConnection import create_db_connection
from csv_utils import append_to_csv
from getAllWarehouse import getAllWarehouse
from getAllArsenal import getAllArsenal

CURRENT_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CSV_FILES")

# Global lock for thread-safe CSV writes
csv_lock = Lock()

def fetchInwardInvoiceForTenant(tenant):
    conn = create_db_connection(tenant)
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    QUERY = """
        SELECT 
            ii.invoice_no,
            GROUP_CONCAT(DISTINCT ii.status SEPARATOR ', ')       AS statuses,
            GROUP_CONCAT(ii.created_on SEPARATOR ', ')            AS created_ons,
            GROUP_CONCAT(DISTINCT ii.created_by SEPARATOR ', ')   AS created_by,
            GROUP_CONCAT(ii.total SEPARATOR ', ')                 AS total_amount_invoice,
            COUNT(*) AS total_count
        FROM inward_invoice ii
        WHERE ii.purchase_type IN ('StockTransferReturn', 'ICSReturn')
        AND ii.status NOT IN ('CANCELLED', 'DELETED')
        AND ii.created_on >= '2025-08-26'
        GROUP BY ii.invoice_no
        HAVING COUNT(*) > 1
        ORDER BY total_count DESC
    """
    cursor.execute(QUERY)
    return cursor.fetchall()

def processTenant(tenant):
    print(f"Processing tenant: {tenant}")

    inwardInvoices = fetchInwardInvoiceForTenant(tenant)
    for inwardInvoice in inwardInvoices:
        with csv_lock:  # ✅ thread-safe CSV write
            append_to_csv(
                "duplicateStrInwardInvoice.csv",
                {"tenant": tenant, "invoice_no": inwardInvoice["invoice_no"], "statuses": inwardInvoice["statuses"], "created_ons": inwardInvoice["created_ons"], "created_by": inwardInvoice["created_by"], "total_amount_invoice": inwardInvoice["total_amount_invoice"], "total_count": inwardInvoice["total_count"]}
            ,None, CURRENT_DIRECTORY, False)
    return tenant

def fetchDuplicateStrInwardInvoiceForAllTenants(tenants, max_workers=10):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(processTenant, tenant): tenant for tenant in tenants}
        for future in as_completed(futures):
            tenant = futures[future]
            try:
                future.result()
                print(f"✅ Finished tenant: {tenant}")
            except Exception as e:
                print(f"❌ Error processing tenant {tenant}: {e}")

if __name__ == "__main__":
    theas = getAllWarehouse()
    arsenals = getAllArsenal()
    all_tenants = theas + arsenals
    fetchDuplicateStrInwardInvoiceForAllTenants(all_tenants, max_workers=10)
