import sys
import os
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from getDBConnection import create_db_connection
from csv_utils import append_to_csv
from getAllWarehouse import getAllWarehouse
from getAllArsenal import getAllArsenal
from pdi import pdiToTenantMap

# Use the keys (partner_detail_ids), not the values (tenants)
partner_detail_ids = list(pdiToTenantMap.keys())
partner_detail_ids_sql = ",".join(f"'{pdi}'" for pdi in partner_detail_ids)

CURRENT_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CSV_FILES")

# Global CSV lock for thread safety
csv_lock = Lock()


def safe_append_to_csv(filename, row):
    """Thread-safe CSV writer"""
    with csv_lock:
        append_to_csv(filename, row, None, CURRENT_DIRECTORY, None)


def getPurchaseIssuesWhereDCisNotGenerated(tenant):
    SQL_QUERY = f"""
        SELECT DISTINCT pi.partner_detail_id, LPAD(pii.ucode, 6, '0') as ucode
        FROM {tenant}.purchase_issue pi
        JOIN {tenant}.purchase_issue_item pii
            ON pii.purchase_issue_id = pi.id
        WHERE pi.status NOT IN ('cancelled', 'DELETED')
        AND pi.partner_detail_id IN ({partner_detail_ids_sql})
        AND (pi.debit_note_number IS NULL OR pi.debit_note_number = '')
        AND pi.created_on >= '2025-08-01'
    """
    conn = create_db_connection("mercury")
    cursor = conn.cursor()
    cursor.execute(SQL_QUERY)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result


def getPurchaseIssuesWhereDCisGenerated(tenant):
    SQL_QUERY = f"""
        SELECT DISTINCT pi.partner_detail_id, LPAD(pii.ucode, 6, '0') as ucode
        FROM {tenant}.purchase_issue pi
        JOIN {tenant}.purchase_issue_item pii
            ON pii.purchase_issue_id = pi.id
        WHERE pi.status NOT IN ('cancelled', 'DELETED')
        AND pi.partner_detail_id IN ({partner_detail_ids_sql})
        AND pi.debit_note_number IS NOT NULL
        AND pi.debit_note_number != ''
        AND pi.created_on >= '2025-08-01'
    """
    conn = create_db_connection("mercury")
    cursor = conn.cursor()
    cursor.execute(SQL_QUERY)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result


def getInwardInvoices(tenant, ucodes, batch_size=500):
    """Fetch inward invoices for ucodes in batches to avoid SQL IN explosion"""
    results = []
    conn = create_db_connection("mercury")
    cursor = conn.cursor()

    for i in range(0, len(ucodes), batch_size):
        batch = ucodes[i:i + batch_size]
        placeholders = ",".join(f"'{u}'" for u in batch)
        SQL_QUERY = f"""
            SELECT DISTINCT iii.code
            FROM {tenant}.inward_invoice ii
            JOIN {tenant}.inward_invoice_item iii
                 ON iii.invoice_id = ii.id
            WHERE iii.code IN ({placeholders})
            AND ii.status = 'live'
            AND ii.purchase_type NOT IN ('ICSReturn' , 'StockTransferReturn')
        """
        cursor.execute(SQL_QUERY)
        results.extend([row[0] for row in cursor.fetchall()])

    cursor.close()
    conn.close()
    return results


def handleUcodeMissingWhenDCisNotGenerated(tenant):
    purchaseIssues = getPurchaseIssuesWhereDCisNotGenerated(tenant)
    pdiToUcodeMap = defaultdict(set)
    for partner_detail_id, ucode in purchaseIssues:
        pdiToUcodeMap[partner_detail_id].add(ucode)

    for partner_detail_id, ucodes in pdiToUcodeMap.items():
        dest_tenant = pdiToTenantMap[str(partner_detail_id)]
        inwardInvoiceItems = set(getInwardInvoices(dest_tenant, list(ucodes)))
        for ucode in ucodes:
            if ucode not in inwardInvoiceItems:
                print(f"processing tenant: {tenant} , partner_detail_id: {partner_detail_id} , ucode: {ucode} , dest_tenant: {dest_tenant}")
                safe_append_to_csv(
                    "ucodeNeverInwardForDCNotGenerated.csv",
                    {"tenant": tenant,
                     "partner_detail_id": partner_detail_id,
                     "ucode": ucode,
                     "dest_tenant": dest_tenant}
                )


def handleUcodeMissingWhenDCisGenerated(tenant):
    purchaseIssues = getPurchaseIssuesWhereDCisGenerated(tenant)
    pdiToUcodeMap = defaultdict(set)
    for partner_detail_id, ucode in purchaseIssues:
        pdiToUcodeMap[partner_detail_id].add(ucode)

    for partner_detail_id, ucodes in pdiToUcodeMap.items():
        dest_tenant = pdiToTenantMap[str(partner_detail_id)]
        inwardInvoiceItems = set(getInwardInvoices(dest_tenant, list(ucodes)))
        for ucode in ucodes:
            if ucode not in inwardInvoiceItems:
                print(f"processing tenant: {tenant} , partner_detail_id: {partner_detail_id} , ucode: {ucode} , dest_tenant: {dest_tenant}")
                safe_append_to_csv(
                    "ucodeNeverInwardForDCGenerated.csv",
                    {"tenant": tenant,
                     "partner_detail_id": partner_detail_id,
                     "ucode": ucode,
                     "dest_tenant": dest_tenant}
                )


def process_tenant(tenant):
    print(f"Processing tenant: {tenant}")
    handleUcodeMissingWhenDCisNotGenerated(tenant)
    handleUcodeMissingWhenDCisGenerated(tenant)
    return tenant


if __name__ == "__main__":
    tenants = []
    tenants.extend(getAllWarehouse())
    tenants.extend(getAllArsenal())

    max_workers = 10

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_tenant = {executor.submit(process_tenant, tenant): tenant for tenant in tenants}

        for future in as_completed(future_to_tenant):
            tenant = future_to_tenant[future]
            try:
                result = future.result()
                print(f"✅ Finished processing {result}")
            except Exception as e:
                print(f"❌ Error processing {tenant}: {e}")
                traceback.print_exc()
