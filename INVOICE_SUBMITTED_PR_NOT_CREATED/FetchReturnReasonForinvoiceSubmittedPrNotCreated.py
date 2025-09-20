import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymysql
from threading import Lock
import csv
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


def process_tenant(tenant, batchInvoiceDetails):
    """Run SQL query for a tenant and save results"""
    try:
        print(f"Processing tenant: {tenant} with {len(batchInvoiceDetails)} invoice details")
        for invoiceDetail in batchInvoiceDetails:
            print(invoiceDetail)
    except Exception as e:
        print(f"❌ Error running query for tenant {tenant}: {e}")

def processCsv(csv_file):
    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file)
        tenantToInvoiceDetailMap = defaultdict(list)
        for row in reader:
            tenantToInvoiceDetailMap[row["tenant"]].append(row)

        if(len(tenantToInvoiceDetailMap) == 0):
            return

        for tenant, invoiceDetails in tenantToInvoiceDetailMap.items():
            batches = [invoiceDetails[i:i+BATCH_SIZE] for i in range(0, len(invoiceDetails), BATCH_SIZE)]
            with ThreadPoolExecutor(max_workers=10) as batch_executor:
                futures = [batch_executor.submit(process_tenant, tenant, batch) for batch in batches]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        print(f"❌ Exception in tenant {tenant}: {e}")

def processAllTenants(tenants, max_workers=10):
    """Run query for all tenants concurrently"""
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_tenant, tenant, []): tenant for tenant in tenants}
        for future in as_completed(futures):
            tenant = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"❌ Exception in tenant {tenant}: {e}")


if __name__ == "__main__":
    processCsv("/Users/lakshay.nailwal/Desktop/ReconScripts/INVOICE_SUBMITTED_PR_NOT_CREATED/CSV_FILES/input.csv")
    # tenants = getAllWarehouse() + getAllArsenal()
    # processAllTenants(tenants, max_workers=10)
