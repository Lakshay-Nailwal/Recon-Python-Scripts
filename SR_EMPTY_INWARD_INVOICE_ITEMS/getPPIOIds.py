import csv
import pymysql
import os
import sys

# ====== UPDATE: MULTIPLE INPUT FILES ======
INPUT_FILES = [
    "/Users/lakshay.nailwal/Desktop/ReconScripts/SR_EMPTY_INWARD_INVOICE_ITEMS/CSV_FILES/srEmptyInwardInvoiceItems.csv",
    "/Users/lakshay.nailwal/Desktop/ReconScripts/SR_EMPTY_INWARD_INVOICE_ITEMS/CSV_FILES/srEmptyInwardInvoiceItems_v2.csv",
    "/Users/lakshay.nailwal/Desktop/ReconScripts/SR_EMPTY_INWARD_INVOICE_ITEMS/CSV_FILES/srEmptyInwardInvoiceItems_v3.csv",
    "/Users/lakshay.nailwal/Desktop/ReconScripts/SR_EMPTY_INWARD_INVOICE_ITEMS/CSV_FILES/srEmptyInwardInvoiceItems_v4.csv",
    "/Users/lakshay.nailwal/Desktop/ReconScripts/SR_EMPTY_INWARD_INVOICE_ITEMS/CSV_FILES/srEmptyInwardInvoiceItems_v5.csv",
    "/Users/lakshay.nailwal/Desktop/ReconScripts/SR_EMPTY_INWARD_INVOICE_ITEMS/CSV_FILES/srEmptyInwardInvoiceItems_v6.csv",
    "/Users/lakshay.nailwal/Desktop/ReconScripts/SR_EMPTY_INWARD_INVOICE_ITEMS/CSV_FILES/srEmptyInwardInvoiceItems_v7.csv",
    "/Users/lakshay.nailwal/Desktop/ReconScripts/SR_EMPTY_INWARD_INVOICE_ITEMS/CSV_FILES/srEmptyInwardInvoiceItems_v8.csv"
]

OUTPUT_DIR = "/Users/lakshay.nailwal/Desktop/ReconScripts/SR_EMPTY_INWARD_INVOICE_ITEMS/CSV_FILES/OUTPUT_V5"

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from getDBConnection import create_db_connection
from csv_utils import append_to_csv
from getAllWarehouse import getAllWarehouse
from getAllArsenal import getAllArsenal


def load_rows_grouped_by_tenant():
    tenant_map = {}

    # ====== LOOP OVER ALL INPUT FILES ======
    for file_path in INPUT_FILES:
        print(f"🔍 Reading: {file_path}")

        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                if row.get("invoice_status") not in ("live", "live-in-progress"):
                    continue

                tenant = row["tenant"]

                if tenant not in tenant_map:
                    tenant_map[tenant] = set()

                tenant_map[tenant].add(row["all_return_order_id"])

    return tenant_map


def fetch_items(tenant, order_id):
    sql = f"""
        SELECT id , ucode , batch , tray_id, status
        FROM {tenant}.pre_purchase_issue_order
        WHERE reference_id = %s
          AND reference_type = 'ALL_RETURN_ORDER'
          AND status = 'CREATED'
    """

    try:
        conn = create_db_connection(tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(sql, (order_id,))
        return cursor.fetchall()
    except Exception as e:
        print(f"Error fetching items for tenant {tenant}: {e}")
        return []
    finally:
        cursor.close()
        conn.close()


def write_csv_for_tenant(tenant, rows):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    out_path = os.path.join(OUTPUT_DIR, f"{tenant}.csv")

    with open(out_path, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "ucode", "batch", "tray_id", "status"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ Saved: {out_path}")


def process():
    tenant_map = load_rows_grouped_by_tenant()

    for tenant, order_ids in tenant_map.items():
        print(f"\n➡️ Processing tenant: {tenant}")

        merged = []
        for oid in order_ids:
            items = fetch_items(tenant, oid)
            merged.extend(items)

        write_csv_for_tenant(tenant, merged)


if __name__ == "__main__":
    process()
