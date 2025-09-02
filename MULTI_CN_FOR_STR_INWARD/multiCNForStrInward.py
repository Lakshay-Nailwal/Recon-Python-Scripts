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

def fetchInwardInvoicesForTenant(tenant):
    conn = create_db_connection(tenant)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SELECT id as invoice_id , invoice_no , created_on FROM inward_invoice WHERE purchase_type in ('StockTransferReturn' , 'ICSReturn') and status = 'live' and created_on >= '2025-05-28'")
    inwardInvoices = cursor.fetchall()
    cursor.close()
    conn.close()
    return inwardInvoices

def fetchCNsForInwardInvoices(invoiceIdList, tenant):
    noteTypes = ['ICS_RETURN' , 'ST_RETURN']
    connection = create_db_connection("vault")
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    placeholders_invoice = ','.join(['%s'] * len(invoiceIdList))
    placeholders_note = ','.join(['%s'] * len(noteTypes))
    sql = f"""
    SELECT return_order_id, note_type , partner_detail_id , GROUP_CONCAT(debit_note_number SEPARATOR ', ') as debit_note_numbers , GROUP_CONCAT(credit_note_number SEPARATOR ', ') as credit_note_numbers
    FROM debitnote 
    WHERE return_order_id IN ({placeholders_invoice}) 
    AND note_type IN ({placeholders_note}) 
    AND tenant = %s
    GROUP BY return_order_id
    HAVING COUNT(DISTINCT debit_note_number) > 1
    """
    cursor.execute(sql, invoiceIdList + noteTypes + [tenant])
    cnResult = cursor.fetchall()
    cursor.close()
    connection.close()

    return cnResult

def processInwardInvoiceBatch(inwardInvoiceBatch, tenant):
    invoiceIdList = [inwardInvoice["invoice_id"] for inwardInvoice in inwardInvoiceBatch]
    cnResult = fetchCNsForInwardInvoices(invoiceIdList, tenant)
    for cn in cnResult:
        safe_append_to_csv("multiCNForStrInward.csv", {"return_order_id": cn["return_order_id"], "note_type": cn["note_type"], "partner_detail_id": cn["partner_detail_id"], "debit_note_numbers": cn["debit_note_numbers"], "credit_note_numbers": cn["credit_note_numbers"], "tenant": tenant})

def process_tenant(tenant):
    """Run SQL query for a tenant and save results"""
    try:
        # TODO: Fetch all inward invoices for the tenant
        inwardInvoices = fetchInwardInvoicesForTenant(tenant)

        if len(inwardInvoices) == 0:
            print(f"No inward invoices found for tenant {tenant}")
            return

        print(f"Processing tenant: {tenant} with inward invoices: {len(inwardInvoices)}")

        batches = [inwardInvoices[i:i+BATCH_SIZE] for i in range(0, len(inwardInvoices), BATCH_SIZE)]
        with ThreadPoolExecutor(max_workers=10) as batch_executor:
            futures = [batch_executor.submit(processInwardInvoiceBatch, batch, tenant) for batch in batches]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"❌ Error in batch for tenant {tenant}: {e}")
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
