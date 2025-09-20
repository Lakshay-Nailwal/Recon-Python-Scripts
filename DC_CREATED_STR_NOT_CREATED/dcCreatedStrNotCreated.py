import sys
import os
import pymysql
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pdi import pdiToTenantMap
from getDBConnection import create_db_connection
from csv_utils import append_to_csv
from getAllWarehouse import getAllWarehouse
from getAllArsenal import getAllArsenal

CURRENT_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CSV_FILES")

# Global lock for thread-safe CSV writes
csv_lock = Lock()

def fetchDistinctDebitNoteNumbersWithPdi(tenant, pdis):
    try:
        conn = create_db_connection(tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        placeholders = ','.join(['%s'] * len(pdis))
        query = f"""
            SELECT DISTINCT pi.debit_note_number, pi.partner_detail_id
            FROM purchase_issue pi
            JOIN purchase_issue_item pii ON pii.purchase_issue_id = pi.id
            WHERE pi.debit_note_number IS NOT NULL
              AND pi.invoice_date >= '2025-05-28'
              AND pi.pr_type <> 'REGULAR_EASYSOL'
              AND pi.partner_detail_id IN ({placeholders})
              AND pi.status NOT IN ('cancelled', 'DELETED')
        """
        cursor.execute(query, pdis)
        return cursor.fetchall()
        
    except Exception as e:
        print(f"Error fetching debit note numbers for tenant {tenant}: {e}")
        return []
    
def fetchDCForTenant(tenant, listOfDcs, batch_size=500):
    """
    Fetch distinct invoice_no for a tenant in batches to avoid large IN clauses.
    """
    try:
        if not listOfDcs:
            return []

        conn = create_db_connection(tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        results = []
        # Process in chunks
        for i in range(0, len(listOfDcs), batch_size):
            batch = listOfDcs[i:i+batch_size]
            placeholders = ','.join(['%s'] * len(batch))
            query = f"""
                SELECT DISTINCT ii.invoice_no
                FROM inward_invoice ii
                WHERE ii.invoice_no IN ({placeholders})
                  AND ii.status NOT IN ('CANCELLED', 'DELETED')
            """
            cursor.execute(query, batch)
            results.extend(cursor.fetchall())

        return results

    except Exception as e:
        print(f"Error fetching DC for tenant {tenant}: {e}")
        return []


def processTenant(tenant):
    print(f"Processing tenant: {tenant}")

    pdis = list(pdiToTenantMap.keys())
    purchaseIssueData = fetchDistinctDebitNoteNumbersWithPdi(tenant, pdis)

    pdiToDcMap = defaultdict(list)
    for pi in purchaseIssueData:
        if pi["debit_note_number"].startswith('PE'):
            continue
        pdiToDcMap[pi['partner_detail_id']].append(pi['debit_note_number'])

    for pdi, dc_list in pdiToDcMap.items():
        dest_tenant = pdiToTenantMap.get(str(pdi))
        destDCList = fetchDCForTenant(dest_tenant, dc_list)
        destDCNumbers = {row['invoice_no'] for row in destDCList}

        for dc in dc_list:
            if dc not in destDCNumbers:
                with csv_lock:  # ✅ thread-safe CSV write
                    append_to_csv(
                        "dcCreatedStrNotCreated.csv",
                        {"source_debit_note_number": dc, "dest_tenant": dest_tenant, "source_tenant": tenant}
                    ,None, CURRENT_DIRECTORY, False)
    return tenant

def fetchDCForAllTenants(tenants, max_workers=10):
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
    fetchDCForAllTenants(all_tenants, max_workers=10)
