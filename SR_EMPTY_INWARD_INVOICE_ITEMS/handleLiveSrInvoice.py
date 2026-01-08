import requests
import time
import json
import pymysql
import sys
import os
import csv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from getDBConnection import create_db_connection

INPUT_FILES = [
    "/Users/lakshay.nailwal/Desktop/ReconScripts/SR_EMPTY_INWARD_INVOICE_ITEMS/CSV_FILES/srEmptyInwardInvoiceItems_v8.csv",
]

OUTPUT_CSV = "/Users/lakshay.nailwal/Desktop/ReconScripts/SR_EMPTY_INWARD_INVOICE_ITEMS/CSV_FILES/final_updates_v15.csv"


def load_unique_rows():
    unique_set = set()
    unique_rows = []

    for file_path in INPUT_FILES:
        print(f"🔍 Reading file: {file_path}")

        with open(file_path, "r", newline='', encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                # Skip non-live invoices
                if row.get("invoice_status") != "live":
                    continue

                key = (row["invoice_id"], row["all_return_order_id"], row["tenant"])

                if key not in unique_set:
                    unique_set.add(key)
                    unique_rows.append(row)

    print(f"🟢 Total unique merged rows: {len(unique_rows)}")
    return unique_rows


def fetchRectificationInvoices(tenant, invoice_id):
    connection = create_db_connection(tenant)
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    cursor.execute(
        "SELECT * FROM inward_invoice WHERE id = %s AND purchase_type = 'RECTIFICATION'",
        (invoice_id,)
    )
    result = cursor.fetchone()

    cursor.close()
    connection.close()
    return result


def fetchWorkerJobs(tenant, invoice_id):
    connection = create_db_connection("mercury")
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SELECT * FROM worker_job WHERE tenant = %s AND name = %s AND type = 'APP_INVOICE_LIVE_V1' AND created_on >= '2025-01-01' AND status = 'COMPLETED'", (tenant, f"Tran#{invoice_id}"))
    result = cursor.fetchone()
    cursor.close()
    connection.close()
    return result if result else None

def process():
    rows = load_unique_rows()
    print(f"Total unique rows to process: {len(rows)}")

    with open(OUTPUT_CSV, "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)

        writer.writerow([
            "tenant",
            "invoice_id",
            "order_id",
            "update_invoice_query",
            "update_worker_job_query",
            "id"
        ])

        for count, row in enumerate(rows, start=1):
            tenant = row["tenant"]
            invoice_id = row["invoice_id"]
            order_id = row["all_return_order_id"]

            print(f"➡️ {count}/{len(rows)} Processing | tenant={tenant} | order={order_id} | invoice={invoice_id}")


            rectification_invoice = fetchRectificationInvoices(tenant, invoice_id)
            if rectification_invoice is not None:
                print(f"❌ Rectification invoice found for tenant {tenant} and invoice {invoice_id}")
                continue


            worker_job = fetchWorkerJobs(tenant, invoice_id)
            if worker_job is None:
                print(f"❌ No worker job found for tenant {tenant} and invoice {invoice_id}")
                continue

            update_invoice_query = (
                f"UPDATE {tenant}.inward_invoice "
                f"SET status = 'live-in-progress', updated_on = NOW() "
                f"WHERE id = {invoice_id} AND status = 'live';"
            )

            update_worker_job_query = (
                "UPDATE mercury.worker_job "
                "SET status = 'PENDING', updated_on = NOW() "
                f"WHERE id = {worker_job['id']} "
                "AND status = 'COMPLETED';"
            )

            writer.writerow([
                tenant,
                invoice_id,
                order_id,
                update_invoice_query,
                update_worker_job_query,
                worker_job['id']
            ])


if __name__ == "__main__":
    process()
