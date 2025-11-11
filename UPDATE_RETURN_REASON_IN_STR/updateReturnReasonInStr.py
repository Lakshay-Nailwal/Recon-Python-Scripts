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
from pdi import pdiToTenantMap

CURRENT_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CSV_FILES")
CSV_LOCK = Lock()  # Thread-safe CSV writes
BATCH_SIZE = 500

def safe_append_to_csv(filename, rows):
    """Thread-safe CSV append"""
    with CSV_LOCK:
        append_to_csv(filename, rows, output_dir=CURRENT_DIRECTORY)

SQL_QUERY = """
   SELECT ii.invoice_no , ii.status , ii.created_on , ii.id , iii.id  as item_id , iii.code , iii.batch , iii.quantity , ii.partner_detail_id from inward_invoice ii join inward_invoice_item iii
   on iii.invoice_id = ii.id
   WHERE ii.purchase_type in ('ICSReturn', 'StockTransferReturn')
   and iii.return_reason is null
   and ii.status in ('StockHidden' , 'StockHidden-qty-verified')
   and ii.created_on >= '2025-05-28'
"""

def fetchPRItems(invoiceNo , sourceTenant):
    conn = create_db_connection(sourceTenant)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SELECT pii.ucode , pii.batch , pii.return_quantity , pii.return_reason FROM purchase_issue pi join purchase_issue_item pii on pii.purchase_issue_id = pi.id WHERE (pi.invoice_no = %s or pi.debit_note_number = %s) order by pii.ucode , pii.batch , pii.return_quantity", (invoiceNo,invoiceNo))
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results


def analyseResults(results , tenant):
    invoiceNoToItemsMap = defaultdict(list)
    for row in results:
        if row["invoice_no"] == "":
            continue

        invoiceNoToItemsMap[row["invoice_no"]].append(row)
    for invoiceNo , items in invoiceNoToItemsMap.items():
        pdi = items[0]["partner_detail_id"]
        sourceTenant = pdiToTenantMap[str(pdi)]
        print(f"Processing invoice {invoiceNo} for tenant {tenant} from {sourceTenant}")
        prItems = fetchPRItems(invoiceNo, sourceTenant)

        if(len(prItems) == 0):
            safe_append_to_csv("prItemsNotFound.csv" , {"tenant" : tenant , "sourceTenant" : sourceTenant , "invoice_no" : invoiceNo , "item_id" : items[0]["item_id"] , "ucode" : items[0]["code"].zfill(6) , "batch" : items[0]["batch"] , "prReason" : None})
            continue

        ucodeBatchToReasonMap = defaultdict()
        for prItem in prItems:
            ucodeBatchToReasonMap[(prItem["ucode"].zfill(6), prItem["batch"])] = prItem["return_reason"]

        print(f"Found {len(ucodeBatchToReasonMap)} return reasons for invoice {invoiceNo}")

        print(f"Found {len(items)} inward invoice items for invoice {invoiceNo}")

        clubInvoiceItemIdForSameReason = defaultdict(list)
        
        for item in items:
            ucode = item["code"].zfill(6)
            batch = item["batch"]
            prReason = ucodeBatchToReasonMap[(ucode , batch)]

            if(prReason is None):
                safe_append_to_csv("StrReturnReasonNotFound.csv" , {"tenant" : tenant , "sourceTenant" : sourceTenant , "invoice_no" : item["invoice_no"] , "item_id" : item["item_id"] , "ucode" : ucode , "batch" : batch , "prReason" : prReason})
                continue

            print(f"Found return reason {prReason} for item {ucode} {batch}")
            
            safe_append_to_csv("updateReturnReasonInStrV21.csv" , {"tenant" : tenant , "sourceTenant" : sourceTenant , "invoice_no" : item["invoice_no"] , "item_id" : item["item_id"] , "ucode" : ucode , "batch" : batch , "prReason" : prReason})
    

def process_tenant(tenant):
    """Run SQL query for a tenant and save results"""
    try:
        # tenant_num = int(tenant.lstrip("th"))  # removes "th" and converts to int
        # if tenant_num < 436:
        #     return 
        conn = create_db_connection(tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(SQL_QUERY)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        analyseResults(results , tenant)
    except Exception as e:
        print(f"❌ Error running query for tenant {tenant}: {e}")


def processAllTenants(tenants, max_workers=0):
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
    tenants = getAllWarehouse()
    processAllTenants(tenants, max_workers=1)
