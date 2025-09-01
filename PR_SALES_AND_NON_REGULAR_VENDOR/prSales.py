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
csv_lock = Lock()  # Thread-safe CSV writing
BATCH_SIZE = 500  # Number of purchase issues per batch

def processPurchaseIssueBatch(batch_purchaseIssues, tenant):
    for purchaseIssue in batch_purchaseIssues:
        with csv_lock:
                append_to_csv(
                    "prSales.csv",
                    {
                        "tenant": tenant,
                        "purchase_issue_id": purchaseIssue["id"],
                        "pr_type": purchaseIssue["pr_type"],
                        "debit_note_number": purchaseIssue["debit_note_number"],
                        "partner_detail_id": purchaseIssue["partner_detail_id"],
                        "created_on": purchaseIssue["created_on"],
                        "status": purchaseIssue["status"],
                    },
                    output_dir=CURRENT_DIRECTORY
                )


def fetchPurchaseIssuesForTenant(tenant):

    conn = create_db_connection(tenant)
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        query = f"""
            SELECT pi.id, pi.pr_type, pi.debit_note_number, pi.partner_detail_id, pi.created_on , pi.status
            FROM purchase_issue pi
            WHERE pi.pr_type = 'PR_SALES'
              AND pi.debit_note_number IS NULL
              AND pi.debit_note_number = ''
              AND pi.status NOT IN ('cancelled', 'DELETED')
              AND pi.created_on >= '2025-07-01'
        """
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        conn.close()


def processTenant(tenant):
    """Fetch all purchase issues for a tenant and process in batches"""
    try:
        print(f"Processing tenant: {tenant}")
        allPurchaseIssues = fetchPurchaseIssuesForTenant(tenant)
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