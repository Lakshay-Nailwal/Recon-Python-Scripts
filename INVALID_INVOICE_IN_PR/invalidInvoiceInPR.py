import sys
import os
import csv
import pymysql
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pdi import pdiToTenantMap
from getDBConnection import create_db_connection
from csv_utils import append_to_csv
from getAllWarehouse import getAllWarehouse
from getAllArsenal import getAllArsenal

CURRENT_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CSV_FILES")


# Thread lock for safe CSV writes
csv_lock = threading.Lock()

def fetchPurchaseIssues(tenant, pdis):
    if not pdis:
        return []
    try:
        conn = create_db_connection(tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        placeholders = ','.join(['%s'] * len(pdis))
        query = f"""
            SELECT id, partner_detail_id, tray_id, invoice_id, invoice_no,
                   invoice_sequence_type, pr_type, invoice_date, invoice_tenant, status , debit_note_number , created_on , updated_on
            FROM purchase_issue
            WHERE pr_type <> 'REGULAR_EASYSOL'
              AND status not in ('cancelled', 'DELETED')
              AND partner_detail_id IN ({placeholders})
              AND (invoice_date >= '2025-08-01' || created_on >= '2025-08-01')
              AND (debit_note_number IS NULL OR debit_note_number = '')
        """
        cursor.execute(query, tuple(pdis))
        return cursor.fetchall()
    except Exception as e:
        print(f"Error fetching purchase issues for tenant {tenant}: {e}")
        return []
    finally:
        cursor.close()
        conn.close()


def validateInvoiceWithConnection(cursor, invoiceId, pdi):
    if invoiceId is None:
        return True
    cursor.execute("""
        SELECT purchase_type, partner_detail_id
        FROM inward_invoice
        WHERE id = %s
    """, (invoiceId,))
    invoice = cursor.fetchone()
    if not invoice:
        return False
    if(invoice["purchase_type"] not in ("ICS", "StockTransfer")):
        return True
    return (
        invoice["purchase_type"] in ("ICS", "StockTransfer") and
        str(invoice["partner_detail_id"]) == str(pdi)
    )

def validate_invoice(pi, tenant):
    pdi = str(pi['partner_detail_id'])
    if pdi not in pdiToTenantMap:
        return
    dest_tenant = pdiToTenantMap[pdi]
    try:
        conn = create_db_connection(tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        if not validateInvoiceWithConnection(cursor, pi['invoice_id'], pdi):
            isInvoiceTenantSame = pi['invoice_tenant'] == tenant
            row = {
                "dest_tenant": dest_tenant, "source_tenant": tenant, "purchase_issue_id": pi['id'], "invoice_id": pi['invoice_id'], "invoice_no": pi['invoice_no'],
                "pr_type": pi['pr_type'], "invoice_tenant": pi['invoice_tenant'], "is_invoice_tenant_same": isInvoiceTenantSame, "status": pi['status'] , "debit_note_number": pi['debit_note_number'] , "created_on": pi['created_on'] , "updated_on": pi['updated_on']
            }
            with csv_lock:
                append_to_csv(
                    "invalidInvoiceInPR.csv",
                    row, None, CURRENT_DIRECTORY, False
                )
    except Exception as e:
        print(f"Error validating invoice for PI {pi['id']} in tenant {tenant}: {e}")
    finally:
        cursor.close()
        conn.close()

def fetchInvalidInvoiceInPR(tenant):
    if tenant in ('th303' , 'th997' , 'th438'):
        return
    pdis = list(pdiToTenantMap.keys())
    purchaseIssues = fetchPurchaseIssues(tenant, pdis)
    if not purchaseIssues:
        return

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for pi in purchaseIssues:
            if pi['invoice_id'] is not None:
                futures.append(executor.submit(validate_invoice, pi, tenant))
        for f in as_completed(futures):
            f.result()


def fetchInvalidInvoiceInPRForAllTenants(tenants, max_workers=10):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetchInvalidInvoiceInPR, tenant): tenant for tenant in tenants}
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
    fetchInvalidInvoiceInPRForAllTenants(all_tenants, max_workers=10)
