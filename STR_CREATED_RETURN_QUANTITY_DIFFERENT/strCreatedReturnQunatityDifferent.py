import sys
import os
import pymysql
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from threading import Lock

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from getDBConnection import create_db_connection
from csv_utils import append_to_csv
from getAllWarehouse import getAllWarehouse
from getAllArsenal import getAllArsenal
from pdi import pdiToTenantMap

CURRENT_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CSV_FILES")

# Global lock for thread-safe CSV writes
csv_lock = Lock()


def fetchInwardInvoiceForTenant(tenant):
    conn = create_db_connection(tenant)
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        QUERY = """
            WITH unique_invoices AS (
                SELECT 
                    ii.invoice_no,
                    ii.id,
                    COUNT(*) AS cnt
                FROM inward_invoice ii
                WHERE ii.purchase_type IN ('StockTransferReturn', 'ICSReturn')
                AND ii.status NOT IN ('CANCELLED', 'DELETED')
                AND ii.created_on >= '2025-08-26'
                GROUP BY ii.invoice_no
                HAVING COUNT(*) = 1
            )

            SELECT 
                ii.invoice_no,
                ii.purchase_type,
                ii.status,
                ii.partner_detail_id,
                iii.code,
                iii.batch,
                SUM(iii.quantity) AS total_quantity
            FROM inward_invoice ii
            JOIN inward_invoice_item iii 
                ON ii.id = iii.invoice_id
            JOIN unique_invoices u 
                ON ii.id = u.id
            GROUP BY 
                ii.invoice_no, 
                ii.purchase_type, 
                ii.status, 
                ii.partner_detail_id, 
                iii.code, 
                iii.batch
            ORDER BY ii.invoice_no
        """
        cursor.execute(QUERY)
        return cursor.fetchall()
    except Exception as e:
        print(f"Error fetching inward invoice for tenant: {e}")
        return []   
    finally:
        conn.close()


def getReturnQuantityInPR(tenant, ucode, batch, invoice_no):
    conn = create_db_connection(tenant)
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        QUERY = """
            SELECT SUM(pii.return_quantity) AS total_return_qty
            FROM purchase_issue_item pii
            JOIN purchase_issue pi ON pii.purchase_issue_id = pi.id
            WHERE LPAD(pii.ucode , 6 , '0') = %s
              AND pii.batch = %s
              AND pi.debit_note_number = %s
              AND pi.status NOT IN ('cancelled', 'DELETED')
        """
        cursor.execute(QUERY, (ucode, batch, invoice_no))
        row = cursor.fetchone()
        return row["total_return_qty"] if row and row["total_return_qty"] is not None else 0
    except Exception as e:
        print(f"Error getting return quantity in PR: {e}")
        return 0
    finally:
        conn.close()


def processTenant(tenant):
    print(f"Processing tenant: {tenant}")

    validInwardInvoices = fetchInwardInvoiceForTenant(tenant)
    ucodeBatchCountInInwardInvoice = defaultdict(int)

    if not validInwardInvoices:
        return
    
    invoiceNoToStatusMap = defaultdict(str)
    invoiceNoToPdiMap = defaultdict(str)
    for invoice in validInwardInvoices:
        invoiceNoToStatusMap[invoice["invoice_no"]] = invoice["status"]
        invoiceNoToPdiMap[invoice["invoice_no"]] = invoice["partner_detail_id"]

    for invoice in validInwardInvoices:
        ucodeBatchCountInInwardInvoice[(invoice["code"], invoice["batch"], invoice["invoice_no"])] += invoice["total_quantity"]
    for (ucode, batch, invoice_no), returnQuantity in ucodeBatchCountInInwardInvoice.items():
        pdi = invoiceNoToPdiMap[invoice_no]
        source_tenant = pdiToTenantMap[str(pdi)]
        
        if(source_tenant == "" or source_tenant == None): print(f"source_tenant is empty for invoice_no: {invoice_no}")
        returnQuantityInPR = getReturnQuantityInPR(source_tenant, ucode, batch, invoice_no)

        if returnQuantity != returnQuantityInPR:
            diff = returnQuantityInPR - returnQuantity
            with csv_lock:  # ✅ thread-safe CSV write
                append_to_csv(
                    "strCreatedReturnQunatityDifferent.csv",
                    {
                        "tenant": tenant,
                        "ucode": ucode,
                        "batch": batch,
                        "invoice_no": invoice_no,
                        "returnQuantityInInwardInvoice": returnQuantity,
                        "returnQuantityInPR": returnQuantityInPR,
                        "diff": diff,
                        "status": invoiceNoToStatusMap[invoice_no],
                        "source_tenant": source_tenant
                    },
                    None,
                    CURRENT_DIRECTORY,
                    False,
                )
    return tenant


def fetchStrCreatedReturnQunatityDifferentForAllTenants(tenants, max_workers=10):
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
    fetchStrCreatedReturnQunatityDifferentForAllTenants(all_tenants, max_workers=10)
