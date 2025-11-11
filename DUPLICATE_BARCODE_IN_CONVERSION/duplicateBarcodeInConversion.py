#!/usr/bin/env python3
import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymysql
from threading import Lock
import csv
from datetime import datetime
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from getDBConnection import create_db_connection
from csv_utils import append_to_csv

# -------------------- Config --------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CURRENT_DIRECTORY = os.path.join(BASE_DIR, "CSV_FILES")
CURRENT_DIRECTORY_FOR_BACKUP = os.path.join(BASE_DIR, "BackUp_CSV_FILES")
CURRENT_DIRECTORY_FOR_SQL = os.path.join(BASE_DIR, "SQL_FILES")

os.makedirs(CURRENT_DIRECTORY, exist_ok=True)
os.makedirs(CURRENT_DIRECTORY_FOR_BACKUP, exist_ok=True)
os.makedirs(CURRENT_DIRECTORY_FOR_SQL, exist_ok=True)

CSV_LOCK = Lock()
MAP_LOCK = Lock()
BATCH_SIZE = 500
MAX_WORKERS = 10

tenantToRackerItemIdMap = {}
# ------------------------------------------------

def fetchBarcodes(tenant, rackerTaskId, ucode, batch, binId):
    conn = create_db_connection(tenant)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    SQL_QUERY = """
        SELECT rti.id, rti.bar_code
        FROM racker_task_item rti
        WHERE rti.racker_task_id = %s
          AND rti.ucode = %s
          AND rti.batch_number = %s
          AND rti.bin_id = %s
    """
    try:
        cursor.execute(SQL_QUERY, (rackerTaskId, ucode, batch, binId))
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


def checkForDuplicateBarcodes(Barcodes, tenant):
    duplicateBarcodes = []
    if not Barcodes:
        return duplicateBarcodes

    conn = create_db_connection(tenant)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    try:
        for i in range(0, len(Barcodes), BATCH_SIZE):
            chunk = [b["bar_code"] for b in Barcodes[i:i + BATCH_SIZE]]
            if not chunk:
                continue
            placeholders = ",".join(["%s"] * len(chunk))
            SQL_QUERY = f"SELECT b.barcode AS barcode FROM barcode b WHERE b.barcode IN ({placeholders})"
            cursor.execute(SQL_QUERY, chunk)
            duplicateBarcodes.extend(cursor.fetchall())
    finally:
        cursor.close()
        conn.close()

    return duplicateBarcodes


def safe_append_to_csv(filename, rows, output_dir=CURRENT_DIRECTORY):
    if not rows:
        return
    file_path = os.path.join(output_dir, filename)
    fieldnames = rows[0].keys()
    with CSV_LOCK:
        file_exists = os.path.exists(file_path)
        with open(file_path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists or os.stat(file_path).st_size == 0:
                writer.writeheader()
            writer.writerows(rows)


def safe_append_to_sql(filename, lines):
    if not lines:
        return
    file_path = os.path.join(CURRENT_DIRECTORY_FOR_SQL, filename)
    with CSV_LOCK:
        with open(file_path, "a") as f:
            for line in lines:
                f.write(line.rstrip() + "\n")


def process_row(row):
    try:
        row_id = row.get("id")
        tenant = row.get("tenant")
        name = row.get("name", "")
        if not tenant or not name:
            print(f"⚠️ Skipping invalid row (missing tenant/name): {row}")
            return

        parts = name.split("|")
        if len(parts) < 4:
            print(f"⚠️ Invalid 'name' format for row id {row_id}: {name}")
            return
    
        rackerTaskId, ucode, batch, binId , full_bin , aggregated_racker_task_id = parts
        print(f"🔍 {tenant} | Task {rackerTaskId} | Ucode {ucode} | Batch {batch} | Bin {binId}")

        Barcodes = fetchBarcodes(tenant, rackerTaskId, ucode, batch, binId)
        if not Barcodes:
            print(f"⚠️ No racker_task_item rows for {tenant} | task {rackerTaskId}")
            return

        barcodeToIDMap = defaultdict(list)
        for b in Barcodes:
            barcodeToIDMap[b["bar_code"]].append(b["id"])

        duplicateBarcodes = checkForDuplicateBarcodes(Barcodes, tenant)
        print(f"✅ {tenant} | Task {rackerTaskId} | Found duplicates: {len(duplicateBarcodes)}")

        if not duplicateBarcodes:
            return

        enriched_rows, found_item_ids = [], []
        for db in duplicateBarcodes:
            barcode = db["barcode"]
            item_ids = barcodeToIDMap.get(barcode, [])
            joined_ids = ",".join(map(str, item_ids)) if item_ids else ""
            enriched_rows.append({
                "barcode": barcode,
                "racker_task_item_ids": joined_ids,
                "racker_task_id": rackerTaskId,
                "ucode": ucode,
                "batch": batch,
                "bin_id": binId
            })
            found_item_ids.extend(item_ids)

        safe_append_to_csv(f"{tenant}_duplicate_barcode_analysis.csv", enriched_rows)

        if found_item_ids:
            with MAP_LOCK:
                tenantToRackerItemIdMap.setdefault(tenant, []).extend(found_item_ids)

    except Exception as e:
        print(f"❌ Error processing row {row}: {e}")


def process_csv_parallel(filename, max_workers=MAX_WORKERS):
    with open(filename, newline="") as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)
    print(f"🚀 Processing {len(rows)} rows with {max_workers} workers")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_row, row) for row in rows]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"❌ Worker exception: {e}")


def backup_racker_task_items(tenant, chunk):
    conn = create_db_connection(tenant)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    placeholders = ",".join(["%s"] * len(chunk))
    SQL_QUERY = f"SELECT * FROM racker_task_item WHERE id IN ({placeholders})"
    cursor.execute(SQL_QUERY, chunk)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    if rows:
        safe_append_to_csv(f"{tenant}_racker_task_item_backup.csv", rows, output_dir=CURRENT_DIRECTORY_FOR_BACKUP)


if __name__ == "__main__":
    start_time = datetime.now()
    filename = "/Users/lakshay.nailwal/Desktop/ReconScripts/DUPLICATE_BARCODE_IN_CONVERSION/CSV_FILES/query_result.csv"

    process_csv_parallel(filename)

    print("\n🧾 Generating tenant-wise delete SQL files and backups...")
    for tenant, item_ids in tenantToRackerItemIdMap.items():
        unique_ids = list(set(item_ids))
        if not unique_ids:
            continue

        for i in range(0, len(unique_ids), 3000):
            chunk = unique_ids[i:i + 3000]
            query = f"DELETE FROM {tenant}.racker_task_item WHERE id IN ({','.join(map(str, chunk))});"
            safe_append_to_sql(f"{tenant}_delete_racker_task_item.sql", [query])

        for i in range(0, len(unique_ids), 100):
            chunk = unique_ids[i:i + 100]
            backup_racker_task_items(tenant, chunk)

        print(f"🗑️  {tenant} | Delete SQL + Backup generated for {len(unique_ids)} items")

    duration = (datetime.now() - start_time).total_seconds()
    print(f"\n✅ Done in {duration:.2f}s. Output folder: {CURRENT_DIRECTORY}")