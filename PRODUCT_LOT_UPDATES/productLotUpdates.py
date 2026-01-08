import sys
import os
import pymysql
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import csv

# --- Imports from your project ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# --- Constants ---
CURRENT_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CSV_FILES")
CSV_LOCK = Lock()
CSV_PRODUCT_LOT_BACKUP = "product_lot_updates_backup.csv"
CSV_PRODUCT_INVENTORY_BACKUP = "product_inventory_updates_backup.csv"
MAX_WORKERS = 10



def create_db_connection(db_name):
    try:
        return pymysql.connect(
            host="mercury-prod-replica.crbaj2am3zwb.ap-south-1.rds.amazonaws.com",
            user="dyno_lakshay_nailwal1_pe_hockc",
            password="tA03EwJf2AUTFNA3",
            port=3306,
            database=db_name
        )
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise
# --- SQL Query ---
SQL_QUERY = """
WITH ranked AS (
  SELECT 
      iii.id,
      iii.code,
      iii.batch,
      iii.mrp,
      ROW_NUMBER() OVER (
          PARTITION BY iii.code, iii.batch 
          ORDER BY ii.updated_on DESC
      ) AS rn
  FROM inward_invoice ii
  JOIN inward_invoice_item iii ON iii.invoice_id = ii.id
  WHERE ii.status = 'live'
    AND ii.comment != 'easysol migration'
    AND iii.total_tax = 0.00
    AND ii.created_on >= '2025-10-12'
    AND ii.purchase_type in ('PROCUREMENT' , 'JIT')
)
SELECT 
    id AS inward_invoice_item_id,
    code,
    batch,
    mrp
FROM ranked
WHERE rn = 1;
"""

def append_to_csv(filename, data, headers=None, output_dir=None, needLogs=True):
    """
    Append row(s) to CSV file.
    Supports both list of dicts and list of lists/tuples.
    Auto-picks headers if not provided.
    """
    if output_dir is None:
        output_dir = CURRENT_DIRECTORY

    os.makedirs(output_dir, exist_ok=True)
    full_path = os.path.join(output_dir, filename)
    file_exists = os.path.isfile(full_path)

    # Normalize data into list form
    if isinstance(data, dict):
        data = [data]
    elif isinstance(data, (list, tuple)) and data and isinstance(data[0], (str, int, float)):
        # single row like ["a", "b", "c"]
        data = [list(data)]

    try:
        with open(full_path, 'a', newline='', encoding='utf-8') as csvfile:
            if isinstance(data, list) and data and isinstance(data[0], dict):
                # Auto-pick headers from dict keys
                if headers is None:
                    headers = list(data[0].keys())

                sanitized_data = [
                    {k: v for k, v in row.items() if k.strip() != ''}
                    for row in data
                ]

                writer = csv.DictWriter(csvfile, fieldnames=headers)

                # If file does not exist, write headers first
                if not file_exists:
                    writer.writeheader()

                writer.writerows(sanitized_data)

            else:
                # Handle list of lists/tuples
                if headers is None and data:
                    headers = [f"col{i+1}" for i in range(len(data[0]))]

                writer = csv.writer(csvfile)

                # If file does not exist, write headers first
                if not file_exists and headers:
                    writer.writerow(headers)

                if data and isinstance(data[0], (list, tuple)):
                    writer.writerows(data)   # multiple rows
                else:
                    writer.writerow(data)    # single row

        if needLogs:
            print(f"✅ Data appended to: {full_path}")
            rows_added = len(data) if isinstance(data, list) else 1
            print(f"Rows appended: {rows_added}")

        return full_path

    except Exception as e:
        print(f"❌ Error appending to CSV file: {e}")
        raise

# --- Thread-safe CSV append ---
def safe_append_to_csv(filename, rows):
    with CSV_LOCK:
        append_to_csv(filename, rows, output_dir=CURRENT_DIRECTORY)

# --- DB Helpers ---
def get_product_lot(conn, code, batch):
    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute("SELECT pl.id, pl.mrp FROM product_lot pl JOIN product p ON pl.product_id = p.id WHERE p.code = %s AND pl.batch = %s", (code, batch))
        return cursor.fetchone()

def get_product_inventory(conn, code, batch):
    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute("SELECT id, mrp FROM product_inventory WHERE code = %s AND batch = %s", (code, batch))
        return cursor.fetchone()

# --- Main per-tenant function ---
def process_tenant(tenant: str):
    print(f"🔹 Starting processing for tenant: {tenant}")
    try:
        with create_db_connection(tenant) as conn:
            with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(SQL_QUERY)
                rows = cursor.fetchall()

            total = len(rows)
            print(f"✅ Fetched {total} rows for tenant {tenant}")

            print(rows)

            if not rows:
                print(f"⚠️ No rows found for tenant {tenant}. Skipping.")
                return

            def process_row(row):
                code, batch = row["code"], row["batch"]
                try:
                    product_lot = get_product_lot(conn, code, batch)
                    product_inventory = get_product_inventory(conn, code, batch)

                    if product_lot:
                        update_Query = f"UPDATE product_lot SET mrp = {row['mrp']} WHERE id = {product_lot['id']}"
                        safe_append_to_csv(CSV_PRODUCT_LOT_BACKUP, {"product_lot_id": product_lot['id'], "product_lot_mrp": product_lot['mrp'], "updated_mrp": row['mrp'], "update_query": update_Query , "invoice_item_id": row["inward_invoice_item_id"]})
                    if product_inventory:
                        update_Query = f"UPDATE product_inventory SET mrp = {row['mrp']} WHERE id = {product_inventory['id']}"
                        safe_append_to_csv(CSV_PRODUCT_INVENTORY_BACKUP, {"product_inventory_id": product_inventory['id'], "product_inventory_mrp": product_inventory['mrp'], "updated_mrp": row['mrp'], "update_query": update_Query , "invoice_item_id": row["inward_invoice_item_id"]})

                    print(f"✅ Processed code={code}, batch={batch}")
                except Exception as e:
                    print(f"❌ Error processing code={code}, batch={batch}: {e}")

            # Process all rows in parallel
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(process_row, row) for row in rows]
                for i, future in enumerate(as_completed(futures), start=1):
                    try:
                        future.result()
                        if i % 500 == 0:
                            print(f"➡️  Progress: {i}/{total} rows processed for tenant {tenant}")
                    except Exception as e:
                        print(f"❌ Thread error during processing: {e}")

        print(f"🎯 Completed processing {total} rows for tenant {tenant}")

    except Exception as e:
        print(f"❌ Error running query for tenant {tenant}: {e}")

# --- Entry point ---
if __name__ == "__main__":
    process_tenant("th214")
