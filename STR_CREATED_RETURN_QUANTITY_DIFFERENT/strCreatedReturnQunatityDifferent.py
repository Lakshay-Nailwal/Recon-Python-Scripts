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
csv_lock = Lock()  # Thread-safe CSV writing
BATCH_SIZE = 500  # Number of purchase issues per batch


def getTotalQuantityInInwardInvoice(tenant, ucode, batch, invoice_no):
    conn = create_db_connection(tenant)
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        QUERY = """
            SELECT SUM(iii.quantity) AS total_quantity
            FROM inward_invoice_item iii
            JOIN inward_invoice ii ON iii.invoice_id = ii.id
            WHERE iii.code = %s
              AND iii.batch = %s
              AND ii.invoice_no = %s
              AND ii.status NOT IN ('CANCELLED', 'DELETED')
        """
        cursor.execute(QUERY, (ucode, batch, invoice_no))
        row = cursor.fetchone()
        return int(row["total_quantity"] or 0) if row else 0
    finally:
        conn.close()


def processPurchaseIssueBatch(batch_purchaseIssues, tenant):
    """Process a batch of purchase issues for a tenant"""
    ucodeBatchCount = defaultdict(int)

    # Aggregate total amounts per ucode + batch + invoice + partner
    for purchaseIssue in batch_purchaseIssues:
        key = (
            purchaseIssue["ucode"],
            purchaseIssue["batch"],
            purchaseIssue["debit_note_number"],
            purchaseIssue["partner_detail_id"]
        )
        ucodeBatchCount[key] += int(purchaseIssue["total_quantity"] or 0)

    # Compare with InwardInvoice totals
    for (ucode, batch, debit_note_number, partner_detail_id), totalQuantity in ucodeBatchCount.items():
        dest_tenant = pdiToTenantMap.get(str(partner_detail_id)) or pdiToTenantMap.get(partner_detail_id)
        if not dest_tenant:
            print(f"❌ Missing dest_tenant for partner_detail_id {partner_detail_id}")
            continue

        totalQuantityInInwardInvoice = getTotalQuantityInInwardInvoice(dest_tenant, ucode, batch, debit_note_number)


        diff = totalQuantityInInwardInvoice - totalQuantity

        if diff == 0:
            continue
        
        with csv_lock:
            append_to_csv(
                "strCreatedReturnQunatityDifferent.csv",
                {
                    "tenant": tenant,
                    "ucode": ucode,
                    "batch": batch,
                    "debit_note_number": debit_note_number,
                    "totalQuantityInPurchaseIssue": totalQuantity,
                    "totalQuantityInInwardInvoice": totalQuantityInInwardInvoice,
                    "diff": diff,
                    "dest_tenant": dest_tenant
                },
                output_dir=CURRENT_DIRECTORY
            )


def fetchPurchaseIssuesForTenant(tenant, pdis):
    if not pdis:
        return []

    conn = create_db_connection(tenant)
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        placeholders = ",".join(["%s"] * len(pdis))
        query = f"""
            SELECT pii.ucode, pii.batch, pi.debit_note_number, pi.partner_detail_id, SUM(pii.return_quantity) AS total_quantity
            FROM purchase_issue pi
            JOIN purchase_issue_item pii ON pii.purchase_issue_id = pi.id
            WHERE pi.pr_type <> 'REGULAR_EASYSOL'
              AND pi.debit_note_number IS NOT NULL
              AND pi.debit_note_number != ''
              AND pi.status NOT IN ('cancelled', 'DELETED')
              AND pi.partner_detail_id IN ({placeholders})
              AND pi.created_on >= '2025-08-30'
            GROUP BY pii.ucode, pii.batch, pi.debit_note_number, pi.partner_detail_id
        """
        cursor.execute(query, tuple(pdis))
        return cursor.fetchall()
    finally:
        conn.close()


def processTenant(tenant):
    """Fetch all purchase issues for a tenant and process in batches"""
    try:
        print(f"Processing tenant: {tenant}")
        pdis = list(pdiToTenantMap.keys())
        allPurchaseIssues = fetchPurchaseIssuesForTenant(tenant, pdis)
        if not allPurchaseIssues:
            return

        # Split into batches
        batches = [allPurchaseIssues[i:i + BATCH_SIZE] for i in range(0, len(allPurchaseIssues), BATCH_SIZE)]

        # Process batches concurrently
        with ThreadPoolExecutor(max_workers=10) as batch_executor:
            futures = [batch_executor.submit(processPurchaseIssueBatch, batch, tenant) for batch in batches]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"❌ Error in batch for tenant {tenant}: {e}")
    except Exception as e:
        print(f"❌ Error in processTenant for tenant {tenant}: {e}")


def processAllTenants(tenants, max_workers=10):
    """Process all tenants concurrently"""
    with ThreadPoolExecutor(max_workers=max_workers) as tenant_executor:
        futures = {tenant_executor.submit(processTenant, tenant): tenant for tenant in tenants}
        for future in as_completed(futures):
            tenant = futures[future]
            try:
                future.result()
                print(f"✅ Finished tenant: {tenant}")
            except Exception as e:
                print(f"❌ Error processing tenant {tenant}: {e}")


if __name__ == "__main__":
    tenants = getAllWarehouse() + getAllArsenal()
    processAllTenants(tenants, max_workers=10)