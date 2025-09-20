import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymysql
from threading import Lock
from collections import defaultdict

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


def fetchStockTransferDispatchedForTenant(tenant):
    conn = create_db_connection(tenant)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SELECT * FROM stock_transfer WHERE status in ('DISPATCHED' , 'PACKED') and invoice_no is not null and created_on >= '2025-08-22'")
    stockTransfers = cursor.fetchall()
    cursor.close()
    conn.close()
    return stockTransfers

def processInwardInvoiceBatch(stockTransferBatch, destTenant , tenant):
    print(f"Processing batch for tenant {destTenant} with {len(stockTransferBatch)} stock transfers")
    conn = create_db_connection(destTenant)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    invoiceNos = [stockTransfer["invoice_no"] for stockTransfer in stockTransferBatch]
    placeholders = ','.join(['%s'] * len(invoiceNos))
    cursor.execute(f"SELECT invoice_no FROM inward_invoice WHERE invoice_no IN ({placeholders}) and status not in ('CANCELLED', 'DELETED')", tuple(invoiceNos,))
    inwardInvoices = cursor.fetchall()
    cursor.close()
    conn.close()

    inwardInvoiceNos = [invoice["invoice_no"] for invoice in inwardInvoices]

    for st in stockTransferBatch:
        if( st["invoice_no"] not in inwardInvoiceNos):
            st["tenant"] = destTenant
            st["source_tenant"] = tenant
            safe_append_to_csv("stDispatchInwardNotCreated.csv", st)

def process_tenant(tenant):
    """Run SQL query for a tenant and save results"""
    try:
        stockTransfers = fetchStockTransferDispatchedForTenant(tenant)

        if(len(stockTransfers) == 0):
            return

        destTenantToStockTransferMap = defaultdict(list)
        for stockTransfer in stockTransfers:
            destTenantToStockTransferMap[stockTransfer["dest_id"]].append(stockTransfer)

        for destTenant, stockTransfers in destTenantToStockTransferMap.items():
            batches = [stockTransfers[i:i+BATCH_SIZE] for i in range(0, len(stockTransfers), BATCH_SIZE)]
            with ThreadPoolExecutor(max_workers=10) as batch_executor:
                futures = [batch_executor.submit(processInwardInvoiceBatch, batch, destTenant , tenant) for batch in batches]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        print(f"❌ Error in batch for tenant {destTenant}: {e}")
        
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
